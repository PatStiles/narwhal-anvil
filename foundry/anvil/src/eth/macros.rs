/// A `info!` helper macro that emits to the target, the node logger listens for
macro_rules! node_info {
    ($($arg:tt)*) => {
         log::info!(target: "node::user", $($arg)*);
    };
}

pub(crate) use node_info;
