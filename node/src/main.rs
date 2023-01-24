// Copyright(C) Facebook, Inc. and its affiliates.
use anvil::spawn;
use anyhow::{Context, Result};
use clap::{crate_name, crate_version, App, AppSettings, ArgMatches, SubCommand};
use config::Export as _;
use config::Import as _;
use config::{Committee, KeyPair, Parameters, WorkerId};
use consensus::Consensus;
use env_logger::Env;
use futures::channel::mpsc::Sender as Snd;
use parking_lot::Mutex;
use primary::{Certificate, Primary};
use std::net::SocketAddr;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use worker::{batch_maker::Transaction, Worker};
/// The default channel capacity.

pub const CHANNEL_CAPACITY: usize = 1_000;

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("A research implementation of Narwhal and Tusk.")
        .args_from_usage("-v... 'Sets the level of verbosity'")
        .subcommand(
            SubCommand::with_name("generate_keys")
                .about("Print a fresh key pair to file")
                .args_from_usage("--filename=<FILE> 'The file where to print the new key pair'"),
        )
        .subcommand(
            SubCommand::with_name("run")
                .about("Run a node")
                .args_from_usage("--keys=<FILE> 'The file containing the node keys'")
                .args_from_usage("--committee=<FILE> 'The file containing committee information'")
                .args_from_usage("--parameters=[FILE] 'The file containing the node parameters'")
                .args_from_usage("--store=<PATH> 'The path where to create the data store'")
                .subcommand(SubCommand::with_name("primary").about("Run a single primary"))
                .subcommand(
                    SubCommand::with_name("worker")
                        .about("Run a single worker")
                        .args_from_usage("--id=<INT> 'The worker id'"),
                )
                .setting(AppSettings::SubcommandRequiredElseHelp),
        )
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .get_matches();

    let log_level = match matches.occurrences_of("v") {
        0 => "error",
        1 => "warn",
        2 => "info",
        3 => "debug",
        _ => "trace",
    };
    let mut logger = env_logger::Builder::from_env(Env::default().default_filter_or(log_level));
    #[cfg(feature = "benchmark")]
    logger.format_timestamp_millis();
    logger.init();

    match matches.subcommand() {
        ("generate_keys", Some(sub_matches)) => KeyPair::new()
            .export(sub_matches.value_of("filename").unwrap())
            .context("Failed to generate key pair")?,
        ("run", Some(sub_matches)) => run(sub_matches).await?,
        _ => unreachable!(),
    }
    Ok(())
}

// Runs either a worker or a primary.
async fn run(matches: &ArgMatches<'_>) -> Result<()> {
    let key_file = matches.value_of("keys").unwrap();
    let committee_file = matches.value_of("committee").unwrap();
    let parameters_file = matches.value_of("parameters");
    let store_path = matches.value_of("store").unwrap();

    // Read the committee and node's keypair from file.
    let keypair = KeyPair::import(key_file).context("Failed to load the node's keypair")?;
    let committee =
        Committee::import(committee_file).context("Failed to load the committee information")?;

    // Load default parameters if none are specified.
    let parameters = match parameters_file {
        Some(filename) => {
            Parameters::import(filename).context("Failed to load the node's parameters")?
        }
        None => Parameters::default(),
    };

    // Make the data store.
    let store = Store::new(store_path).context("Failed to create a store")?;

    // Channels the sequence of certificates.
    let (tx_output, mut rx_output) = channel(CHANNEL_CAPACITY);

    // Channels Tx from Anvil to Workers Batchmakrer.
    let (tx_batch_maker, rx_batch_maker) = channel(CHANNEL_CAPACITY);

    let vec: Vec<Snd<Transaction>> = Vec::new();
    let tx_listeners = Mutex::new(vec);

    // Check whether to run a primary, a worker, or an entire authority.
    match matches.subcommand() {
        // Spawn the primary and consensus core.
        ("primary", _) => {
            let (tx_new_certificates, rx_new_certificates) = channel(CHANNEL_CAPACITY);
            let (tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);
            Primary::spawn(
                keypair,
                committee.clone(),
                parameters.clone(),
                store.clone(),
                /* tx_consensus */ tx_new_certificates,
                /* rx_consensus */ rx_feedback,
            );
            Consensus::spawn(
                committee.clone(),
                parameters.gc_depth,
                /* rx_primary */ rx_new_certificates,
                /* tx_primary */ tx_feedback,
                tx_output,
            );

            spawn_anvil(rx_output, store, tx_batch_maker.clone(), tx_listeners).await?;
        }

        // Spawn a single worker.
        ("worker", Some(sub_matches)) => {
            let id = sub_matches
                .value_of("id")
                .unwrap()
                .parse::<WorkerId>()
                .context("The worker id must be a positive integer")?;
            Worker::spawn(
                keypair.name,
                id,
                committee.clone(),
                parameters,
                store.clone(),
                tx_batch_maker.clone(),
                rx_batch_maker,
                tx_listeners,
            );

            rx_output.recv().await;
        }
        _ => unreachable!(),
    }

    // Analyze the consensus' output.
    //analyze(rx_output).await;

    // If this expression is reached, the program ends and all other tasks terminate.
    unreachable!();
}

async fn spawn_anvil(
    rx_output: Receiver<Certificate>,
    mempool_store: Store,
    tx_batch_maker: Sender<Transaction>,
    tx_listeners: Mutex<Vec<Snd<Transaction>>>,
) -> Result<()> {
    let addr = "0.0.0.0:25664".parse::<SocketAddr>().unwrap();
    let config = anvil::NodeConfig {
        host: Some(addr.ip()),
        port: addr.port(),
        enable_tracing: false,
        ..Default::default()
    };

    let (_, handle) = spawn(
        config,
        tx_batch_maker,
        rx_output,
        mempool_store,
        tx_listeners,
    )
    .await;
    handle
        .await
        .unwrap()
        .context("Anvil Handler panicked")
        .unwrap();
    Ok(())
}
