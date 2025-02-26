use crate::{
    hydration::{CacheLookupSuccess, Hydrator, StoreResult},
    source_of_record::SourceOfRecord,
    store::Store,
    CacheBrownsResult,
};
use std::borrow::Borrow;
use tokio::sync::RwLock;

/// [`PullHydrator`] hydrates the cache by immediately calling out to the [`SourceOfRecord`]
/// any time data is missing or stale to attempt to provide current data.
///
/// Note, this hydrator does not attempt to prevent parallel or redundant calls to the [`SourceOfRecord`].
/// If your data source can be overwhelmed, or you need to avoid wasting cycles this way, the
/// provided [`SourceOfRecord`] must internally enforce this logic. See [`crate::source_of_record::concurrent`]
/// for generic wrappers.
pub struct PullHydrator<S, Sor> {
    store: RwLock<S>,
    data_source: Sor,
}

impl<K, V, S, Sor> Hydrator for PullHydrator<S, Sor>
where
    K: Clone + Send + Sync,
    V: Clone + Send + Sync,
    S: Store<Key = K, Value = V> + Send + Sync,
    Sor: SourceOfRecord<Key = K, Value = V> + Send + Sync,
{
    type Key = K;
    type Value = V;
    type FlushResultIterator = S::FlushResultIterator;

    async fn get<Q: Borrow<Self::Key> + Sync>(
        &self,
        key: &Q,
    ) -> Option<CacheLookupSuccess<Self::Value>> {
        let value = self.store.read().await.get(key).await;
        match value {
            Some(value) => self.try_use_cached_value(key.borrow(), value).await,
            None => match self.data_source.retrieve(key).await {
                Some(value) => {
                    // TODO: During telemetry pass, consider making this not silent
                    let _ = self
                        .store
                        .write()
                        .await
                        .put(key.borrow().clone(), value.clone())
                        .await;
                    Some(CacheLookupSuccess::new(StoreResult::NotFound, true, value))
                }
                None => None,
            },
        }
    }

    async fn flush(&self) -> Self::FlushResultIterator {
        self.store.write().await.flush().await
    }

    async fn stop_tracking<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) -> CacheBrownsResult<()> {
        match self.store.write().await.delete(key).await {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

impl<Key, Value, S, Sor> PullHydrator<S, Sor>
where
    Key: Clone + Send + Sync,
    Value: Clone + Send + Sync,
    S: Store<Key = Key, Value = Value>,
    Sor: SourceOfRecord<Key = Key, Value = Value> + Send + Sync,
{
    pub fn new(store: S, data_source: Sor) -> Self {
        let store = RwLock::new(store);
        Self { store, data_source }
    }

    async fn try_use_cached_value(
        &self,
        key: &Key,
        value: Value,
    ) -> Option<CacheLookupSuccess<Value>> {
        if self.data_source.is_valid(key, &value).await {
            Some(CacheLookupSuccess::new(StoreResult::Valid, false, value))
        } else {
            match self.data_source.retrieve_with_hint(key, &value).await {
                Some(new_value) => {
                    // TODO: During telemetry pass, consider making this not silent
                    let _ = self
                        .store
                        .write()
                        .await
                        .put((*key).clone(), new_value.clone())
                        .await;
                    Some(CacheLookupSuccess::new(
                        StoreResult::Invalid,
                        true,
                        new_value,
                    ))
                }
                None => Some(CacheLookupSuccess::new(StoreResult::Invalid, false, value)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        hydration::{pull::PullHydrator, CacheLookupSuccess, Hydrator},
        source_of_record::test_helpers::{MockSor, MockSorWrapper},
        store::test_helpers::{MockStore, MockStoreWrapper},
    };
    use std::vec;

    fn base_fakes() -> (MockStore, MockSor) {
        (MockStore::new(), MockSor::new())
    }

    #[tokio::test]
    async fn not_found_retrieves_valid_and_hydrates() {
        let (mut store, mut data_source) = base_fakes();
        store.expect_get().return_const(None);
        store.expect_get().return_const(Some(42));
        // Validate hydration occurred.
        store
            .expect_put()
            .once()
            .withf(|key, value| key == &42 && *value == 42)
            .returning(|_, _| Ok(()));

        data_source.expect_retrieve().return_once(|_key| Some(42));

        let store = MockStoreWrapper::new(store);
        let data_source = MockSorWrapper::new(data_source);

        assert_eq!(
            Some(CacheLookupSuccess::Miss(42)),
            PullHydrator::new(store, data_source).get(&42).await
        );
    }

    #[tokio::test]
    async fn not_found_retrieves_invalid() {
        let (mut store, mut data_source) = base_fakes();
        store.expect_get().return_once(|_key| None);

        data_source.expect_retrieve().return_once(|_key| None);

        let store = MockStoreWrapper::new(store);
        let data_source = MockSorWrapper::new(data_source);

        assert_eq!(None, PullHydrator::new(store, data_source).get(&42).await);
    }

    #[tokio::test]
    async fn found_returns_with_proper_metadata() {
        let (mut store, mut data_source) = base_fakes();
        store.expect_get().times(3).return_const(Some(42));

        data_source.expect_is_valid().once().return_const(true);

        // Simulate value going stale before second read with a failed retrieve.
        data_source.expect_is_valid().return_const(false);
        data_source
            .expect_retrieve_with_hint()
            .once()
            .return_const(None);

        // Stale value, but successfully refreshed this time
        data_source.expect_is_valid().return_const(false);
        data_source
            .expect_retrieve_with_hint()
            .once()
            .return_const(Some(55));
        store.expect_put().once().returning(|_, _| Ok(()));

        let store = MockStoreWrapper::new(store);
        let data_source = MockSorWrapper::new(data_source);
        let hydrator = PullHydrator::new(store, data_source);

        assert_eq!(Some(CacheLookupSuccess::Hit(42)), hydrator.get(&42).await);

        assert_eq!(Some(CacheLookupSuccess::Stale(42)), hydrator.get(&42).await);

        assert_eq!(
            Some(CacheLookupSuccess::Refresh(55)),
            hydrator.get(&42).await
        );
    }

    #[tokio::test]
    async fn flush_propagates() {
        let (mut store, data_source) = base_fakes();
        store.expect_flush().once().returning(|| vec![].into_iter());

        let store = MockStoreWrapper::new(store);
        let data_source = MockSorWrapper::new(data_source);
        let hydrator = PullHydrator::new(store, data_source);
        assert_eq!(0, hydrator.flush().await.len());
    }

    #[tokio::test]
    async fn stop_tracking_propagates() {
        let key = 42;
        let key_clone = key;

        let (mut store, data_source) = base_fakes();
        store
            .expect_delete()
            .withf(move |key_in: &i32| *key_in == key_clone)
            .returning(|key| Ok(Some(*key)));

        let store = MockStoreWrapper::new(store);
        let data_source = MockSorWrapper::new(data_source);
        let hydrator = PullHydrator::new(store, data_source);

        hydrator.stop_tracking(&42).await.unwrap()
    }
}
