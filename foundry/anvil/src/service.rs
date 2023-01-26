//! background service

use crate::{
    eth::{fees::FeeHistoryService, pool::transactions::PoolTransaction},
    filter::Filters,
    mem::{storage::MinedBlockOutcome, Backend},
    NodeResult,
};
use futures::{future::try_join_all, stream::FuturesUnordered};
use futures::{FutureExt, Stream, StreamExt};
use log::{info, trace};
use primary::Certificate;
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use store::{Store, StoreError, StoreResult};
use tokio::{sync::mpsc::Receiver, time::Interval};
use worker::batch_maker::Batch;

/// The type that drives the blockchain's state
///
/// This service is basically an endless future that continuously polls the miner which returns
/// transactions for the next block, then those transactions are handed off to the
/// [backend](backend::mem::Backend) to construct a new block, if all transactions were successfully
/// included in a new block they get purged from the `Pool`.
pub struct NodeService {
    /// creates new blocks
    block_producer: BlockProducer,
    /// maintenance task for fee history related tasks
    fee_history: FeeHistoryService,
    /// Tracks all active filters
    filters: Filters,
    /// The interval at which to check for filters that need to be evicted
    filter_eviction_interval: Interval,
    /// Channel to Receive certificates from consensus
    rx_consensus: Receiver<Certificate>,
    /// Store for mempool batches
    store: Store,
}

impl NodeService {
    pub fn new(
        backend: Arc<Backend>,
        fee_history: FeeHistoryService,
        filters: Filters,
        rx_consensus: Receiver<Certificate>,
        store: Store,
    ) -> Self {
        let start = tokio::time::Instant::now() + filters.keep_alive();
        let filter_eviction_interval = tokio::time::interval_at(start, filters.keep_alive());
        Self {
            block_producer: BlockProducer::new(backend),
            fee_history,
            filter_eviction_interval,
            filters,
            rx_consensus,
            store,
        }
    }
    async fn tx_fetcher(mut missing: Vec<(Vec<u8>, Store)>, deliver: Batch) -> StoreResult<Batch> {
        let waiting: Vec<_> = missing.iter_mut().map(|(x, y)| y.notify_read(x.to_vec())).collect();

        info!(target: "node", "fn tx_fetcher: returning StoreResult; waiting: {:?}", waiting.len());
        try_join_all(waiting).await.map(|_| deliver).map_err(StoreError::from)
    }
}

impl Future for NodeService {
    type Output = NodeResult<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let pin = self.get_mut();
        let mut store_workers = FuturesUnordered::new();

        // this drives block production and feeds new sets of ready transactions to the block
        // producer
        loop {
            while let Poll::Ready(Some(outcome)) = pin.block_producer.poll_next_unpin(cx) {
                info!(target: "node", "mined block {}", outcome.block_number);
                // prune the transactions from the pool
                // DON'T NEED TO PRUNE TX FROM POOL AS NO POOL TO PRUNE, MAY USE THIS FOR FLUSHING CACHE
                //pin.pool.on_mined_block(outcome);
            }

            if let Poll::Ready(Some(cert)) = pin.rx_consensus.poll_recv(cx) {
                info!(target: "node", "received cert: round {}, id {}", cert.header.round, cert.header.id);
                let batch: Vec<Vec<u8>> = Vec::new();
                let wait_for = cert
                    .header
                    .payload
                    .into_iter()
                    .map(|(digest, _)| (digest.to_vec(), pin.store.clone()))
                    .collect();
                let fut = Self::tx_fetcher(wait_for, batch);
                store_workers.push(fut);
            } else {
                // no progress made
                break;
            }

            if let Poll::Ready(Some(batch_res)) = store_workers.poll_next_unpin(cx) {
                let batch = batch_res.unwrap();

                info!(target: "node", "Deserializing Batches from Store: # Batches {}", batch.clone().len());
                let pool_txs =
                    serde_json::from_slice::<Vec<PoolTransaction>>(batch.concat().as_slice())
                        .unwrap();
                let txs: Vec<Arc<PoolTransaction>> =
                    pool_txs.into_iter().map(|tx| Arc::new(tx)).collect::<Vec<_>>();

                // fetcher returned a set of transaction that we feed to the producer
                info!(target: "node", "Sending {} Tx to BlockProducer", txs.len());
                pin.block_producer.queued.push_back(txs);
            }
        }

        // poll the fee history task
        let _ = pin.fee_history.poll_unpin(cx);

        if pin.filter_eviction_interval.poll_tick(cx).is_ready() {
            let filters = pin.filters.clone();
            // evict filters that timed out
            tokio::task::spawn(async move { filters.evict().await });
        }

        Poll::Pending
    }
}

// The type of the future that mines a new block
type BlockMiningFuture =
    Pin<Box<dyn Future<Output = (MinedBlockOutcome, Arc<Backend>)> + Send + Sync>>;

/// A type that exclusively mines one block at a time
#[must_use = "BlockProducer does nothing unless polled"]
struct BlockProducer {
    /// Holds the backend if no block is being mined
    idle_backend: Option<Arc<Backend>>,
    /// Single active future that mines a new block
    block_mining: Option<BlockMiningFuture>,
    /// backlog of sets of transactions ready to be mined
    queued: VecDeque<Vec<Arc<PoolTransaction>>>,
}

// === impl BlockProducer ===

impl BlockProducer {
    fn new(backend: Arc<Backend>) -> Self {
        Self { idle_backend: Some(backend), block_mining: None, queued: Default::default() }
    }
}

impl Stream for BlockProducer {
    type Item = MinedBlockOutcome;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let pin = self.get_mut();

        if !pin.queued.is_empty() {
            if let Some(backend) = pin.idle_backend.take() {
                let transactions = pin.queued.pop_front().expect("not empty; qed");
                pin.block_mining = Some(Box::pin(async move {
                    info!(target: "miner", "creating new block");
                    let block = backend.mine_block(transactions).await;
                    info!(target: "miner", "created new block: {}", block.block_number);
                    (block, backend)
                }));
            }
        }

        if let Some(mut mining) = pin.block_mining.take() {
            if let Poll::Ready((outcome, backend)) = mining.poll_unpin(cx) {
                pin.idle_backend = Some(backend);
                return Poll::Ready(Some(outcome));
            } else {
                pin.block_mining = Some(mining)
            }
        }

        Poll::Pending
    }
}
