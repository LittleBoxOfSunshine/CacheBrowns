use crate::CacheBrownsResult;
use interruptible_polling::SelfUpdatingPollingTask;
use itertools::Itertools;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::source_of_record::SourceOfRecord;
use crate::store::Store;

use super::{CacheLookupSuccess, Hydrator, StoreResult};

/// [`PollingHydrationStrategy`] initially hydrates values the same way that [`super::pull::PullCacheHydrator`]
/// does, by immediately fetching the missing data from the [`SourceOfRecord`], but it refreshes
/// data by polling the [`SourceOfRecord`] for updates rather than as needed when stale data is
/// encountered.
///
///
///
/// Note, this hydrator does not attempt to prevent parallel or redundant calls to the [`SourceOfRecord`].
/// If your data source can be overwhelmed, or you need to avoid wasting cycles this way, the
/// provided [`SourceOfRecord`] must internally enforce this logic. See [`crate::source_of_record::concurrent`]
/// for generic wrappers.
pub struct PollingHydrationStrategy<S, Sor>
where
    S: Store + Send + Sync + 'static,
    Sor: SourceOfRecord + Send + Sync + 'static,
{
    shared_inner_state: Arc<InnerState<S, Sor>>,
    // We can't know if the underlying store is volatile, clean exit property of PollingTask
    // increases probability of maintaining data integrity.
    _polling_thread: SelfUpdatingPollingTask,
}

struct InnerState<S, Sor>
where
    S: Store + Send + Sync,
    Sor: SourceOfRecord + Send + Sync + 'static,
{
    data_source: Sor,
    store: RwLock<S>,
}

impl<S, Sor> PollingHydrationStrategy<S, Sor>
where
    S: Store + Send + Sync,
    S::Key: Clone,
    S::Value: Clone,
    Sor: SourceOfRecord<Key = S::Key, Value = S::Value> + Send + Sync + 'static,
{
    pub fn new(data_source: Sor, store: S, polling_interval: Duration) -> Self {
        Self::new_self_updating(data_source, store, polling_interval, |_| ())
    }

    pub fn new_self_updating<F>(
        data_source: Sor,
        store: S,
        polling_interval: Duration,
        interval_updater: F,
    ) -> Self
    where
        F: Fn(&Duration) + Send + 'static,
    {
        let shared_state = Arc::new(InnerState {
            data_source,
            store: RwLock::from(store),
        });

        Self {
            shared_inner_state: shared_state.clone(),
            _polling_thread: SelfUpdatingPollingTask::new_with_checker(
                polling_interval,
                move |interval, checker| {
                    Self::poll(&shared_state, checker);
                    interval_updater(interval);
                },
            ),
        }
    }

    fn poll(shared_inner_state: &Arc<InnerState<S, Sor>>, checker: &dyn Fn() -> bool) {
        let keys: Vec<S::Key> = shared_inner_state
            .store
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect_vec();

        for key in keys {
            if !checker() {
                break;
            }

            // Fetch separately to release the lock
            let peeked_value = shared_inner_state
                .store
                .read()
                .unwrap()
                .peek(&key)
                .map(|v| v.into_owned());

            // If value was deleted since pulling keys, don't issue a superfluous retrieve.
            if let Some(value) = peeked_value {
                let canonical_value = shared_inner_state
                    .data_source
                    .retrieve_with_hint(&key, &value);

                if let Some(v) = canonical_value.as_ref() {
                    let mut store_handle = shared_inner_state.store.write().unwrap();

                    // Respect delete if delete occurred during retrieval
                    if store_handle.contains(&key) {
                        store_handle.put(key, v.clone());
                    }
                }
            }
        }
    }

    // fn poll(shared_inner_state: &Arc<InnerState<S, Sor>>, checker: &dyn Fn() -> bool) {
    //     let keys_with_hints: Vec<S::Key> = shared_inner_state
    //         .store
    //         .read()
    //         .unwrap()
    //         .keys()
    //         .cloned()
    //         .collect_vec();
    //     let keys_with_hints = keys_with_hints.into_iter()
    //         .filter_map(|k| {
    //             shared_inner_state
    //                 .store
    //                 .read()
    //                 .unwrap()
    //                 .peek(&k)
    //                 .map_or(None, |v| Some((k, v)))
    //         });
    //
    //     for batch in shared_inner_state
    //         .data_source
    //         .batch_retrieve_with_hint(keys_with_hints)
    //     {
    //         if !checker() {
    //             return;
    //         }
    //
    //         for (k, v) in batch
    //             // We're already working with copies, so it's fine to just drop any returned hint inputs
    //             .filter_map(|(k, v)| {
    //                 match v {
    //                     Some(v) => Some((k, v)),
    //                     None => None
    //                 }
    //             })
    //         {
    //             if !checker() {
    //                 return;
    //             }
    //
    //             let mut store_handle = shared_inner_state.store.write().unwrap();
    //
    //             // Respect any delete that occurred while retrieving
    //             if store_handle.contains(&k) {
    //                 store_handle.put(k, v);
    //             }
    //         }
    //     }
    // }
}

impl<Key, Value, S, Sor> Hydrator for PollingHydrationStrategy<S, Sor>
where
    Key: Clone,
    Value: Clone,
    S: Store<Key = Key, Value = Value> + Send + Sync,
    Sor: SourceOfRecord<Key = Key, Value = Value> + Send + Sync + 'static,
{
    type Key = Key;
    type Value = Value;
    type FlushResultIterator = S::FlushResultIterator;

    fn get(&mut self, key: &Key) -> Option<CacheLookupSuccess<Value>> {
        let read_lock = self.shared_inner_state.store.read().unwrap();
        let value = read_lock.get(key);

        match value {
            None => {
                drop(read_lock);
                self.shared_inner_state
                    .data_source
                    .retrieve(key)
                    .map(|value| {
                        self.shared_inner_state
                            .store
                            .write()
                            .unwrap()
                            .put(key.clone(), value.clone());
                        CacheLookupSuccess::new(StoreResult::NotFound, true, value.to_owned())
                    })
            }
            Some(value) => Some(CacheLookupSuccess::new(
                if self.shared_inner_state.data_source.is_valid(key, &value) {
                    StoreResult::Valid
                } else {
                    StoreResult::Invalid
                },
                false,
                value.into_owned(),
            )),
        }
    }

    fn flush(&mut self) -> Self::FlushResultIterator {
        self.shared_inner_state.store.write().unwrap().flush()
    }

    fn stop_tracking(&mut self, key: &Key) -> CacheBrownsResult<()> {
        match self.shared_inner_state.store.write().unwrap().delete(key) {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    // use std::hash::Hash;
    // use std::sync::atomic::{AtomicU64, Ordering};
    // use std::{collections::HashMap, sync::Mutex, thread};
    //
    // use crate::store::memory::MemoryStore;
    //
    // use super::*;
    // use uuid::Uuid;
    //
    // struct BasicSource<K, V> {
    //     inner: HashMap<K, V>,
    // }
    //
    // impl<K, V> BasicSource<K, V>
    // where
    //     K: Eq + PartialEq + Hash,
    //     V: Eq,
    // {
    //     fn new() -> Self {
    //         Self {
    //             inner: HashMap::new(),
    //         }
    //     }
    //
    //     fn insert(&mut self, key: K, value: V) {
    //         self.inner.insert(key, value);
    //     }
    // }
    //
    // impl<K, V> SourceOfRecord for BasicSource<K, V>
    // where
    //     K: Eq + PartialEq + Hash,
    //     V: Clone + Eq,
    // {
    //     type Key = K;
    //     type Value = V;
    //
    //     fn retrieve(&self, key: &K) -> Option<V> {
    //         self.inner.get(key).cloned()
    //     }
    //
    //     fn retrieve_with_hint(&self, key: &K, _current: &V) -> Option<V> {
    //         self.inner.get(key).cloned()
    //     }
    //
    //     fn is_valid(&self, key: &K, value: &V) -> bool {
    //         self.retrieve(key) == Some(value.clone())
    //     }
    // }
    //
    // struct ForgetfulSource {
    //     inner: HashMap<Uuid, Mutex<Option<u64>>>,
    // }
    //
    // impl ForgetfulSource {
    //     fn new() -> Self {
    //         Self {
    //             inner: HashMap::new(),
    //         }
    //     }
    //
    //     fn insert(&mut self, key: Uuid, value: u64) {
    //         self.inner.insert(key, Mutex::new(Some(value)));
    //     }
    // }
    //
    // impl SourceOfRecord for ForgetfulSource {
    //     type Key = Uuid;
    //     type Value = u64;
    //
    //     fn retrieve(&self, key: &Uuid) -> Option<u64> {
    //         // Immediately remove the value once we retrieve it once.
    //         // We need a Mutex for this because this takes &self.
    //         self.inner.get(key).and_then(|x| {
    //             let mut v = x.lock().unwrap();
    //             let previous = *v;
    //             *v = None;
    //             previous
    //         })
    //     }
    //
    //     fn retrieve_with_hint(&self, key: &Uuid, _value: &u64) -> Option<u64> {
    //         self.retrieve(key)
    //     }
    //
    //     fn is_valid(&self, key: &Uuid, value: &u64) -> bool {
    //         self.inner
    //             .get(key)
    //             .map_or(false, |v| *v.lock().unwrap() == Some(*value))
    //     }
    // }
    //
    // struct IncrementingSource {
    //     value: AtomicU64,
    // }
    //
    // impl IncrementingSource {
    //     fn new() -> Self {
    //         Self {
    //             value: AtomicU64::new(0),
    //         }
    //     }
    // }
    //
    // impl SourceOfRecord for IncrementingSource {
    //     type Key = String;
    //     type Value = u64;
    //
    //     fn retrieve(&self, _key: &String) -> Option<u64> {
    //         Some(self.value.fetch_add(1, Ordering::Relaxed))
    //     }
    //
    //     fn retrieve_with_hint(&self, key: &String, _v: &u64) -> Option<u64> {
    //         self.retrieve(key)
    //     }
    //
    //     fn is_valid(&self, _key: &String, _value: &u64) -> bool {
    //         false
    //     }
    // }
    //
    // #[test]
    // fn get_not_found_immediate_pull() {}
    //
    // #[test]
    // fn get_only_updates_after_poll_operation() {}
    //
    // #[test]
    // fn derp() {}
    //
    // #[test]
    // fn flush_propagates() {}
    //
    // #[test]
    // fn stop_tracking_propagates() {}
    //
    // #[test]
    // fn poll_hydration_valid_key() {
    //     let mut source = BasicSource::new();
    //     let key_1 = Uuid::new_v4();
    //     let key_2 = Uuid::new_v4();
    //     source.insert(key_1, String::from("AAAA"));
    //     source.insert(key_2, String::from("AAAAAAAA"));
    //
    //     let mut cache =
    //         PollingHydrationStrategy::new(source, MemoryStore::new(), Duration::from_secs(1));
    //
    //     assert_eq!(
    //         cache.get(&key_1),
    //         Some(CacheLookupSuccess::Miss(String::from("AAAA")))
    //     );
    //
    //     assert_eq!(
    //         cache.get(&key_1),
    //         Some(CacheLookupSuccess::Hit(String::from("AAAA")))
    //     );
    // }
    //
    // #[test]
    // fn poll_hydration_no_key() {
    //     let mut source = BasicSource::new();
    //     let key_1 = Uuid::new_v4();
    //     let key_2 = Uuid::new_v4();
    //     let key_3 = Uuid::new_v4();
    //     source.insert(key_1, String::from("AAAA"));
    //     source.insert(key_2, String::from("AAAAAAAA"));
    //
    //     let mut cache =
    //         PollingHydrationStrategy::new(source, MemoryStore::new(), Duration::from_secs(1));
    //
    //     assert_eq!(cache.get(&key_3), None);
    //     assert_eq!(cache.get(&key_3), None);
    // }
    //
    // #[test]
    // fn poll_hydration_lost_key() {
    //     let mut source = ForgetfulSource::new();
    //     let key_1 = Uuid::new_v4();
    //     let key_2 = Uuid::new_v4();
    //     source.insert(key_1, 0);
    //     source.insert(key_2, 1);
    //
    //     let mut cache =
    //         PollingHydrationStrategy::new(source, MemoryStore::new(), Duration::from_secs(1));
    //
    //     assert_eq!(cache.get(&key_1), Some(CacheLookupSuccess::Miss(0)));
    //
    //     assert_eq!(cache.get(&key_1), Some(CacheLookupSuccess::Stale(0)));
    // }
    //
    // #[test]
    // fn poll_hydration_update_after_poll() {
    //     let source = IncrementingSource::new();
    //     let mut cache =
    //         PollingHydrationStrategy::new(source, MemoryStore::new(), Duration::from_secs(2));
    //     let key = String::from("asdf");
    //
    //     // First lookup is a miss
    //     assert_eq!(cache.get(&key), Some(CacheLookupSuccess::Miss(0)),);
    //
    //     // Align ourselves to be neatly between polls
    //     thread::sleep(Duration::from_secs(1));
    //
    //     // Value now updated to 1
    //     assert_eq!(cache.get(&key), Some(CacheLookupSuccess::Stale(1)));
    //
    //     assert_eq!(cache.get(&key), Some(CacheLookupSuccess::Stale(1)),);
    //
    //     thread::sleep(Duration::from_secs(2));
    //
    //     // Value now updated to 2
    //     assert_eq!(cache.get(&key), Some(CacheLookupSuccess::Stale(2)),);
    // }
    //
    // #[test]
    // fn poll_hydration_stop_tracking_miss() {
    //     let mut source = BasicSource::new();
    //     let key_1 = Uuid::new_v4();
    //     let key_2 = Uuid::new_v4();
    //     source.insert(key_1, String::from("AAAA"));
    //     source.insert(key_2, String::from("AAAAAAAA"));
    //
    //     let mut cache =
    //         PollingHydrationStrategy::new(source, MemoryStore::new(), Duration::from_secs(1));
    //
    //     assert_eq!(
    //         cache.get(&key_1),
    //         Some(CacheLookupSuccess::Miss(String::from("AAAA")))
    //     );
    //
    //     assert_eq!(
    //         cache.get(&key_1),
    //         Some(CacheLookupSuccess::Hit(String::from("AAAA")))
    //     );
    //
    //     cache.stop_tracking(&key_1).unwrap();
    //
    //     assert_eq!(
    //         cache.get(&key_1),
    //         Some(CacheLookupSuccess::Miss(String::from("AAAA")))
    //     );
    // }
}
