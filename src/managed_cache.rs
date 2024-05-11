use crate::hydration::{CacheLookupSuccess, Hydrator};
use crate::managed_cache::CacheLookupFailure::NotFound;
use crate::CacheBrownsResult;

pub enum CacheLookupFailure {
    /// No valid value could be fetched.
    NotFound,
    /// Data was fetched, but the result is invalid.
    NotValid,
}

pub enum InvalidCacheEntryBehavior {
    /// Invalid data is never returned, instead a failure is issued. Use when data can't be out of sync.
    ReturnNotValid,
    /// Returned invalid values anyway as a best-effort. Use when data can be delayed.
    ReturnStale,
}

pub struct ManagedCache<Key, Value, H>
where
    Value: Clone,
    H: Hydrator<Key = Key, Value = Value>,
{
    hydrator: H,
    when_invalid: InvalidCacheEntryBehavior,
}

impl<'a, Key, Value, H> ManagedCache<Key, Value, H>
where
    Value: From<CacheLookupSuccess<Value>> + Clone + 'a,
    H: Hydrator<Key = Key, Value = Value>,
{
    pub fn new(hydrator: H, when_invalid: InvalidCacheEntryBehavior) -> Self {
        Self {
            hydrator,
            when_invalid,
        }
    }

    pub fn get(&mut self, key: &Key) -> Result<CacheLookupSuccess<Value>, CacheLookupFailure> {
        match self.hydrator.get(key) {
            None => Err(NotFound),
            Some(lookup_result) => {
                if let CacheLookupSuccess::Stale(_) = &lookup_result {
                    if let InvalidCacheEntryBehavior::ReturnStale = &self.when_invalid {
                        return Ok(lookup_result);
                    }

                    Err(CacheLookupFailure::NotValid)
                } else {
                    Ok(lookup_result)
                }
            }
        }
    }

    /// Delete all records from the underlying store. Valid reasons to do this include:
    ///
    /// 1. Application logic includes events that mean all records are nonsensical. Example, a hypervisor with per VM data that has been reset and no longer has VMs.
    /// 2. Reduce utilization (though this calls your choice of replacement strategy into question)
    /// 3. Clear out non-recoverable corrupted data. This should only happen in advanced scenarios such as live patching while the underlying [`super::store::Store`] is non-volatile with schema breaking changes.
    ///
    /// In general, reasons 2 & 3 are regarded as code smells. They are supported as workarounds for
    /// issues in code that you do not control, or can not easily update in a service workload. An
    /// example would be your custom [`super::store::Store`] has a defect that needs to be patched, and in the
    /// meantime you deploy recovery actions that hook into this method.
    ///
    /// IF YOU ARE USING THIS TO TRY TO LOOPHOLE A CACHE INVALIDATION, YOU ARE GOING TO BREAK THINGS
    pub fn flush(&mut self) -> H::FlushResultIterator {
        self.hydrator.flush()
    }

    /// Indicates that a given key is no longer relevant and can be purged. For example, a client
    /// session ends or a VM is deallocated. Intended to prevent leaks when no replacement algorithm
    /// is used, or as an optimization to free the resource earlier than it otherwise would be.
    ///
    /// IF YOU ARE USING THIS TO TRY TO LOOPHOLE A CACHE INVALIDATION, YOU ARE GOING TO BREAK THINGS
    pub fn stop_tracking(&mut self, key: &Key) -> CacheBrownsResult<()> {
        self.hydrator.stop_tracking(key)
    }
}
