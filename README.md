subscriptions do not work they are hardcoded to return the 0 hash.
there is something else that returns the 0 hash
Tx are deserialized directly into Vec<PoolTransaction> this could cause issues may have to first deserialize 
into a batch then PoolTransaction
Could also make PoolTransaction a crate and import into worker
