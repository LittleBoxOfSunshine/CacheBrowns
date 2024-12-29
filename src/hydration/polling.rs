use crate::CacheBrownsResult;
use interruptible_polling::tokio::{PollingTaskBuilder, PollingTaskHandle, TaskChecker};
use itertools::Itertools;
use std::borrow::Borrow;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

use crate::source_of_record::SourceOfRecord;
use crate::store::Store;

use super::{CacheLookupSuccess, Hydrator, StoreResult};

/// [`PollingHydrator`] initially hydrates values the same way that [`super::pull::PullHydrator`]
/// does, by immediately fetching the missing data from the [`SourceOfRecord`], but it refreshes
/// data by polling the [`SourceOfRecord`] for updates rather than as needed when stale data is
/// encountered.
///
/// Note, this hydrator does not attempt to prevent parallel or redundant calls to the [`SourceOfRecord`].
/// If your data source can be overwhelmed, or you need to avoid wasting cycles this way, the
/// provided [`SourceOfRecord`] must internally enforce this logic. See [`crate::source_of_record::concurrent`]
/// for generic wrappers.
pub struct PollingHydrator<S, Sor>
where
    S: Store + Send + Sync + 'static,
    Sor: SourceOfRecord + Send + Sync + 'static,
{
    shared_inner_state: Arc<InnerState<S, Sor>>,
    // We can't know if the underlying store is volatile, clean exit property of PollingTask
    // increases probability of maintaining data integrity.
    _polling_thread: PollingTaskHandle,
}

struct InnerState<S, Sor>
where
    S: Store + Send + Sync,
    Sor: SourceOfRecord + Send + Sync + 'static,
{
    data_source: Sor,
    store: RwLock<S>,
}

impl<S, Sor> PollingHydrator<S, Sor>
where
    S: Store + Send + Sync,
    S::Key: Clone,
    S::Value: Clone,
    Sor: SourceOfRecord<Key = S::Key, Value = S::Value> + Send + Sync + 'static,
{
    pub fn new(data_source: Sor, store: S, polling_interval: Duration) -> Self {
        let shared_state = Arc::new(InnerState {
            data_source,
            store: RwLock::from(store),
        });

        // TODO: Consider integration with the try to wait feature. This should probably have an abstraction to make easier to consume via propagation. Some sort of token returned (could be as simple as a type alias for the future).
        Self {
            shared_inner_state: shared_state.clone(),
            _polling_thread: PollingTaskBuilder::new(polling_interval)
                .task_with_checker(move |checker| {
                    let shared_state_clone = shared_state.clone();
                    async move {
                        Self::poll(&shared_state_clone, checker).await;
                    }
                })
        }
    }

    async fn poll(shared_inner_state: &Arc<InnerState<S, Sor>>, checker: TaskChecker) {
        let keys: Vec<S::Key> = shared_inner_state
            .store
            .read()
            .await
            .keys()
            .await
            .collect_vec();

        for key in keys {
            if !checker.is_running() {
                break;
            }

            // Fetch separately to release the lock
            let peeked_value = shared_inner_state
                .store
                .read()
                .await
                .peek(&key)
                .await;

            // If value was deleted since pulling keys, don't issue a superfluous retrieve.
            if let Some(value) = peeked_value {
                let canonical_value = shared_inner_state
                    .data_source
                    .retrieve_with_hint(&key, &value);

                if let Some(v) = canonical_value.as_ref() {
                    let mut store_handle = shared_inner_state.store.write().await;

                    // TODO: During telemetry pass, consider making this not silent
                    // Respect delete if delete occurred during retrieval
                    let _ = store_handle.update(key, v.clone());
                }
            }
        }
    }
}

impl<Key, Value, S, Sor> Hydrator for PollingHydrator<S, Sor>
where
    Key: Clone,
    Value: Clone + Send + Sync,
    S: Store<Key = Key, Value = Value> + Send + Sync,
    Sor: SourceOfRecord<Key = Key, Value = Value> + Send + Sync + 'static,
{
    type Key = Key;
    type Value = Value;
    type FlushResultIterator = S::FlushResultIterator;

    async fn get<Q: Borrow<Self::Key> + Sync>(
        &mut self,
        key: &Q,
    ) -> Option<CacheLookupSuccess<Value>> {
        let read_lock = self.shared_inner_state.store.read().await;
        let value = read_lock.get(key).await;

        match value {
            None => {
                drop(read_lock);
                match self.shared_inner_state
                    .data_source
                    .retrieve(key)
                    .map(|value: Self::Value| async {
                        // TODO: During telemetry pass, consider making this not silent
                        let _ = self
                            .shared_inner_state
                            .store
                            .write()
                            .await
                            .put(key.borrow().clone(), value.clone());
                        CacheLookupSuccess::new(StoreResult::NotFound, true, value)
                    })
                {
                    Some(fut) => Some(fut.await),
                    None => None,
                }
            }
            Some(value) => Some(CacheLookupSuccess::new(
                if self
                    .shared_inner_state
                    .data_source
                    .is_valid(key.borrow(), &value)
                {
                    StoreResult::Valid
                } else {
                    StoreResult::Invalid
                },
                false,
                value,
            )),
        }
    }

    async fn flush(&mut self) -> Self::FlushResultIterator {
        self.shared_inner_state.store.write().await.flush().await
    }

    async fn stop_tracking<Q: Borrow<Self::Key> + Sync>(&mut self, key: &Q) -> CacheBrownsResult<()> {
        match self.shared_inner_state.store.write().await.delete(key).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::hydration::polling::PollingHydrator;
    use crate::hydration::{CacheLookupSuccess, Hydrator};
    use crate::source_of_record::test_helpers::{MockSor, MockSorWrapper};
    use crate::store::test_helpers::{MockStore, MockStoreWrapper};
    use std::time::Duration;

    const CONTINUOUS: Duration = Duration::from_secs(0);
    const NEVER: Duration = Duration::MAX;

    fn base_fakes() -> (MockStore, MockSor) {
        (MockStore::new(), MockSor::new())
    }

    #[tokio::test]
    async fn not_found_on_demand_succeeds() {
        let (mut store, mut data_source) = base_fakes();

        store.expect_keys().return_const(vec![].into_iter());
        store.expect_peek().return_const(None);
        store.expect_get().return_const(None);
        store.expect_put().once().returning(|_, _| Ok(()));
        data_source.expect_retrieve().return_const(Some(42));

        let mut polling = PollingHydrator::new(
            MockSorWrapper::new(data_source),
            MockStoreWrapper::new(store),
            CONTINUOUS,
        );
        assert!(polling.get(&32).await.is_some());
    }

    #[tokio::test]
    async fn not_found_on_demand_fails() {
        let (mut store, mut data_source) = base_fakes();

        store.expect_keys().return_const(vec![].into_iter());
        store.expect_peek().return_const(None);
        store.expect_get().return_const(None);
        data_source.expect_retrieve().return_const(None);

        let mut polling = PollingHydrator::new(
            MockSorWrapper::new(data_source),
            MockStoreWrapper::new(store),
            CONTINUOUS,
        );
        assert!(polling.get(&32).await.is_none());
    }

    #[tokio::test]
    async fn found_no_fetch_fresh_or_stale() {
        let (mut store, mut data_source) = base_fakes();

        store.expect_keys().return_const(vec![&32].into_iter());
        store.expect_peek().return_const(Some(42));
        store.expect_get().return_const(Some(42));
        data_source
            .expect_is_valid()
            .once()
            .withf(|k, v| k == &32 && *v == 42)
            .return_const(true);
        data_source
            .expect_is_valid()
            .withf(|k, v| k == &32 && *v == 42)
            .return_const(false);

        let mut polling = PollingHydrator::new(
            MockSorWrapper::new(data_source),
            MockStoreWrapper::new(store),
            NEVER,
        );
        assert!(polling
            .get(&32)
            .await
            .is_some_and(|v| v == CacheLookupSuccess::Hit(42)));
        assert!(polling
            .get(&32)
            .await
            .is_some_and(|v| v == CacheLookupSuccess::Stale(42)));
    }

    #[tokio::test]
    async fn poll_during_on_demand() {}

    #[tokio::test]
    async fn on_demand_during_poll() {}

    #[tokio::test]
    async fn deleted_before_poll_remains_deleted() {}

    #[tokio::test]
    async fn delete_during_poll_remains_deleted() {}

    #[tokio::test]
    async fn stop_tracking_during_read_and_write() {}
}
