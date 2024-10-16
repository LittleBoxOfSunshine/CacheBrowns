use crate::hydration::{CacheLookupSuccess, Hydrator, StoreResult};
use crate::source_of_record::SourceOfRecord;
use crate::store::Store;
use crate::CacheBrownsResult;
use std::borrow::Borrow;

/// [`PullHydrator`] hydrates the cache by immediately calling out to the [`SourceOfRecord`]
/// any time data is missing or stale to attempt to provide current data.
///
/// Note, this hydrator does not attempt to prevent parallel or redundant calls to the [`SourceOfRecord`].
/// If your data source can be overwhelmed, or you need to avoid wasting cycles this way, the
/// provided [`SourceOfRecord`] must internally enforce this logic. See [`crate::source_of_record::concurrent`]
/// for generic wrappers.
pub struct PullHydrator<S, Sor> {
    store: S,
    data_source: Sor,
}

impl<K, V, S, Sor> Hydrator for PullHydrator<S, Sor>
where
    K: Clone,
    V: Clone,
    S: Store<Key = K, Value = V>,
    Sor: SourceOfRecord<Key = K, Value = V>,
{
    type Key = K;
    type Value = V;
    type FlushResultIterator = S::FlushResultIterator;

    fn get<Q: Borrow<Self::Key>>(&mut self, key: &Q) -> Option<CacheLookupSuccess<Self::Value>> {
        match self.store.get(key) {
            Some(value) => {
                let value = value.into_owned();
                self.try_use_cached_value(key.borrow(), value)
            }
            None => match self.data_source.retrieve(key) {
                Some(value) => {
                    // TODO: During telemetry pass, consider making this not silent
                    let _ = self.store.put(key.borrow().clone(), value.clone());
                    Some(CacheLookupSuccess::new(StoreResult::NotFound, true, value))
                }
                None => None,
            },
        }
    }

    fn flush(&mut self) -> Self::FlushResultIterator {
        self.store.flush()
    }

    fn stop_tracking(&mut self, key: &Self::Key) -> CacheBrownsResult<()> {
        match self.store.delete(key) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

impl<Key, Value, S, Sor> PullHydrator<S, Sor>
where
    Key: Clone,
    Value: Clone,
    S: Store<Key = Key, Value = Value>,
    Sor: SourceOfRecord<Key = Key, Value = Value>,
{
    pub fn new(store: S, data_source: Sor) -> Self {
        Self { store, data_source }
    }

    fn try_use_cached_value(
        &mut self,
        key: &Key,
        value: Value,
    ) -> Option<CacheLookupSuccess<Value>> {
        if self.data_source.is_valid(key, &value) {
            Some(CacheLookupSuccess::new(StoreResult::Valid, false, value))
        } else {
            match self.data_source.retrieve_with_hint(key, &value) {
                Some(new_value) => {
                    // TODO: During telemetry pass, consider making this not silent
                    let _ = self.store.put((*key).clone(), new_value.clone());
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
    use crate::hydration::pull::PullHydrator;
    use crate::hydration::{CacheLookupSuccess, Hydrator};
    use crate::source_of_record::test_helpers::{MockSor, MockSorWrapper};
    use crate::store::test_helpers::{MockStore, MockStoreWrapper};
    use std::borrow::Cow;
    use std::vec;

    fn base_fakes() -> (MockStore, MockSor) {
        (MockStore::new(), MockSor::new())
    }

    #[test]
    fn not_found_retrieves_valid_and_hydrates() {
        let (mut store, mut data_source) = base_fakes();
        store.expect_get().return_const(None);
        store.expect_get().return_const(Some(Cow::Owned(42)));
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
            PullHydrator::new(store, data_source).get(&42)
        );
    }

    #[test]
    fn not_found_retrieves_invalid() {
        let (mut store, mut data_source) = base_fakes();
        store.expect_get().return_once(|_key| None);

        data_source.expect_retrieve().return_once(|_key| None);

        let store = MockStoreWrapper::new(store);
        let data_source = MockSorWrapper::new(data_source);

        assert_eq!(None, PullHydrator::new(store, data_source).get(&42));
    }

    #[test]
    fn found_returns_with_proper_metadata() {
        let (mut store, mut data_source) = base_fakes();
        store
            .expect_get()
            .times(3)
            .return_const(Some(Cow::Owned(42)));

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
        let mut hydrator = PullHydrator::new(store, data_source);

        assert_eq!(Some(CacheLookupSuccess::Hit(42)), hydrator.get(&42));

        assert_eq!(Some(CacheLookupSuccess::Stale(42)), hydrator.get(&42));

        assert_eq!(Some(CacheLookupSuccess::Refresh(55)), hydrator.get(&42));
    }

    #[test]
    fn flush_propagates() {
        let (mut store, data_source) = base_fakes();
        store.expect_flush().once().returning(|| vec![].into_iter());

        let store = MockStoreWrapper::new(store);
        let data_source = MockSorWrapper::new(data_source);
        let mut hydrator = PullHydrator::new(store, data_source);
        assert_eq!(0, hydrator.flush().len());
    }

    #[test]
    fn stop_tracking_propagates() {
        let key = 42;
        let key_clone = key;

        let (mut store, data_source) = base_fakes();
        store
            .expect_delete()
            .withf(move |key_in: &i32| *key_in == key_clone)
            .returning(|key| Ok(Some(*key)));

        let store = MockStoreWrapper::new(store);
        let data_source = MockSorWrapper::new(data_source);
        let mut hydrator = PullHydrator::new(store, data_source);

        hydrator.stop_tracking(&42).unwrap()
    }
}
