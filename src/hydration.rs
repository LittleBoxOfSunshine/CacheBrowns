use crate::CacheBrownsResult;
use std::borrow::Borrow;

pub mod polling;
pub mod pull;

pub trait Hydrator {
    type Key;
    type Value: Clone;

    type FlushResultIterator: Iterator<Item = CacheBrownsResult<Self::Key>>;

    // TODO: Should we have a special version of Cow where the Borrow variant can take a lock handle?
    // This would allow us to return the borrowed data without thread safety issues if the hydrator
    // has internal locking, like it does in the case of Polling. Downsides are a custom type, more
    // complexity. Upside is that having to copy a value every time you use it is a bit yikes. Is it
    // better to just say yeah, it's expensive so you should use Arc if you care about this? Pub-sub
    // will also have this issue. The question is what balance to have, because either is adding
    // slight overhead for the non-threaded hydrators like Pull. With custom Cow, that overhead is
    // just passing a None around though so pretty light. Another downside is that letting external
    // users hold references means they're exposed to a more complicated borrow checker interaction.
    // Is it already the case in some other respect though? Is the ref based version and more
    // cumbersome than any existing instances?
    fn get<Q: Borrow<Self::Key>>(&mut self, key: &Q) -> Option<CacheLookupSuccess<Self::Value>>;

    fn flush(&mut self) -> Self::FlushResultIterator;

    fn stop_tracking(&mut self, key: &Self::Key) -> CacheBrownsResult<()>;
}

#[derive(Debug, Eq, PartialEq)]
pub enum CacheLookupSuccess<Value: Clone> {
    /// Value was not present in the underlying store and had to be fetched from source of record.
    Miss(Value),

    /// Value was found in underlying store, but had to be refreshed from source of record.
    Refresh(Value),

    /// Stale value was found in underlying store, but fresh value could not be fetched from source of record.
    Stale(Value),

    /// Valid value found in underlying store.
    Hit(Value),
}

impl<Value: Clone> CacheLookupSuccess<Value> {
    pub fn new(store_result: StoreResult, did_fetch_from_sor: bool, value: Value) -> Self {
        match store_result {
            StoreResult::Invalid => {
                if did_fetch_from_sor {
                    CacheLookupSuccess::Refresh(value)
                } else {
                    CacheLookupSuccess::Stale(value)
                }
            }
            StoreResult::NotFound => CacheLookupSuccess::Miss(value),
            StoreResult::Valid => CacheLookupSuccess::Hit(value),
        }
    }

    pub fn into_inner(self) -> Value {
        match self {
            CacheLookupSuccess::Miss(v) => v,
            CacheLookupSuccess::Refresh(v) => v,
            CacheLookupSuccess::Stale(v) => v,
            CacheLookupSuccess::Hit(v) => v,
        }
    }
}

/// Standardized semantics for the result of a store operation. Used with [`CacheLookupSuccess::new`]
/// to properly wrap data in the correct [`CacheLookupSuccess`] variant. These helpers are intended
/// to increase semantic clarity and reduce code duplication.
pub enum StoreResult {
    /// Valid value found in the store for the corresponding key.
    Valid,

    /// No value found in the store for the corresponding key.
    NotFound,

    /// Invalid value found in the store for the corresponding key.
    Invalid,
}
