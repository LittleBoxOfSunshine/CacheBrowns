use crate::CacheBrownsResult;

pub mod polling;
pub mod pull;

// TODO: It occurs that why this feels not useful it's just "a cache". Maybe rename, maybe a sign the other traits should inherent from it, maybe it should just be an alias?
// actually, get is slightly different, so it's flush / stop that feel like another trait is lurking
// could also just be that flush should be pulled out?
pub trait Hydrator {
    type Key;
    type Value;

    type FlushResultIterator: Iterator<Item = CacheBrownsResult<Option<Self::Key>>>;

    fn get(&mut self, key: &Self::Key) -> Option<CacheLookupSuccess<Self::Value>>;

    fn flush(&mut self) -> Self::FlushResultIterator;

    fn stop_tracking(&mut self, key: &Self::Key) -> CacheBrownsResult<()>;
}

#[derive(Debug, Eq, PartialEq)]
pub enum CacheLookupSuccess<Value> {
    /// Value was not present in the underlying store and had to be fetched from source of record.
    Miss(Value),

    /// Value was found in underlying store, but had to be refreshed from source of record.
    Refresh(Value),

    /// Stale value was found in underlying store, but fresh value could not be fetched from source of record.
    Stale(Value),

    /// Valid value found in underlying store.
    Hit(Value),
}

impl<Value> CacheLookupSuccess<Value> {
    pub fn new(store_result: StoreResult, hydrated: bool, value: Value) -> Self {
        match store_result {
            StoreResult::Invalid => {
                if hydrated {
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
