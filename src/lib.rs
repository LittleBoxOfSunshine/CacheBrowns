pub mod hydration;
pub mod managed_cache;
pub mod source_of_record;
pub mod store;

/// Result with type erasure for propagating arbitrary errors.
pub type CacheBrownsResult<T> = Result<T, Box<dyn std::error::Error>>;

// pub trait Flushable {
//     type FlushResultIterator: Iterator<Item = CacheResult<Option<Key>>>;
// }
