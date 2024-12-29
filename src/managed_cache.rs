use crate::hydration::{CacheLookupSuccess, Hydrator};
use crate::managed_cache::CacheLookupFailure::NotFound;
use crate::CacheBrownsResult;
use std::borrow::Borrow;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CacheLookupFailure {
    /// No valid value could be fetched.
    #[error("No valid value could be fetched")]
    NotFound,
    /// Data was fetched, but the result is invalid.
    #[error("Data was fetched, but the result is invalid")]
    NotValid,
}

#[derive(Clone)]
pub enum InvalidCacheEntryBehavior {
    /// Invalid data is never returned, instead a failure is issued. Use when data can't be out of sync.
    ReturnNotValid,
    /// Returned invalid values anyway as a best-effort. Use when data can be delayed.
    ReturnStale,
}

pub struct ManagedCache<H>
{
    hydrator: Arc<H>,
    when_invalid: InvalidCacheEntryBehavior,
}

impl<H> Clone for ManagedCache<H> {
    fn clone(&self) -> Self {
        Self {
            hydrator: self.hydrator.clone(),
            when_invalid: self.when_invalid.clone(),
        }
    }
}

impl<'a, Key, Value, H> ManagedCache<H>
where
    Value: From<CacheLookupSuccess<Value>> + Clone + 'a,
    H: Hydrator<Key = Key, Value = Value>,
{
    pub fn new(hydrator: H, when_invalid: InvalidCacheEntryBehavior) -> Self {
        let hydrator = Arc::new(hydrator);
        Self {
            hydrator,
            when_invalid,
        }
    }

    pub async fn get<Q: Borrow<Key> + Sync>(
        &mut self,
        key: &Q,
    ) -> Result<CacheLookupSuccess<Value>, CacheLookupFailure> {
        match self.hydrator.get(key).await {
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
    pub async fn flush(&self) -> H::FlushResultIterator {
        self.hydrator.flush().await
    }

    /// Indicates that a given key is no longer relevant and can be purged. For example, a client
    /// session ends or a VM is deallocated. Intended to prevent leaks when no replacement algorithm
    /// is used, or as an optimization to free the resource earlier than it otherwise would be.
    ///
    /// IF YOU ARE USING THIS TO TRY TO LOOPHOLE A CACHE INVALIDATION, YOU ARE GOING TO BREAK THINGS
    ///
    /// If you are using an unbounded cache (no Replacement strategy) you must consider how to handle
    /// errors, retries, and repeated errors. A failure to do so can result in a memory leak.
    pub async fn stop_tracking<Q: Borrow<Key> + Sync>(&self, key: &Q) -> CacheBrownsResult<()> {
        self.hydrator.stop_tracking(key).await
    }
}

// struct ManagedCacheFuture;
//
// impl Future for ManagedCacheFuture {
//     type Item = CacheLookupSuccess<Value>;
//     type Error = CacheLookupFailure;
//
//     fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
//         todo!()
//     }
// }

impl<'a, Key, Value, H> tower_service::Service<Key> for ManagedCache<H>
where
    Key: Clone + Send + Sync + 'static,
    Value: From<CacheLookupSuccess<Value>> + Clone + 'a,
    H: Hydrator<Key = Key, Value = Value> + Sync + 'static,
{
    type Response = CacheLookupSuccess<Value>;
    type Error = CacheLookupFailure;

    type Future = Pin<Box<dyn Future<Output=Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // TODO: There's likely a good opportunity to integrate this to pass through to the source of record, where this concept will typically exist.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Key) -> Self::Future {
        let mut cache = (*self).clone();

        Box::pin(async move {
            cache.get(&req).await
        })
    }
}
