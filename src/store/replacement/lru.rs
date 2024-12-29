// This implementation borrows heavily from https://crates.io/crates/lru

use crate::{store::Store, CacheBrownsResult};
use itertools::Itertools;
use std::{
    borrow::Borrow,
    cell::RefCell,
    collections::HashMap,
    hash::{Hash, Hasher},
    mem::MaybeUninit,
    num::NonZeroUsize,
    ptr,
    ptr::NonNull,
    vec,
};

#[derive(Eq, Debug)]
struct KeyRef<Key>
where
    Key: Eq,
{
    pub key: *const Key,
}

impl<Key, B> From<&B> for KeyRef<Key>
where
    Key: Eq,
    B: Borrow<Key>,
{
    fn from(key: &B) -> Self {
        KeyRef {
            key: ptr::from_ref(key.borrow()),
        }
    }
}

impl<Key: Hash> Hash for KeyRef<Key>
where
    Key: Eq,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        unsafe { (*self.key).hash(state) }
    }
}

impl<Key: PartialEq> PartialEq for KeyRef<Key>
where
    Key: Eq,
{
    fn eq(&self, other: &KeyRef<Key>) -> bool {
        unsafe { (*self.key).eq(&*other.key) }
    }
}

struct Entry<Key> {
    pub key: MaybeUninit<Key>,
    pub previous: *mut Entry<Key>,
    pub next: *mut Entry<Key>,
}

impl<Key> Entry<Key> {
    fn new(key: Key) -> Self {
        Self {
            key: MaybeUninit::new(key),
            previous: ptr::null_mut(),
            next: ptr::null_mut(),
        }
    }

    fn new_edge() -> Self {
        Self {
            key: MaybeUninit::uninit(),
            previous: ptr::null_mut(),
            next: ptr::null_mut(),
        }
    }
}

/// Implements the Least Recently Used replacement strategy on top of an arbitrary data store.
///
/// ## Volatility
///
/// While it supports non-volatile data, the usage order metadata held in memory is volatile.
/// For example, if you restart the process the same data will load, but the order depends on
/// the order the underlying store iterates keys over. This tradeoff was chosen because committing
/// usage tracking in a non-volatile store means that all read operations are now writes and
/// potentially expensive calls. Given that the point of a cache is to optimize reads and storing
/// the order perfectly over long periods of time isn't generally needed, this is the better default
/// implementation.
///
/// ## Thread Safety
///
///  poses a challenge to the [`Store`] trait. [`Store::get`] is immutable, this is because
/// for all basic stores there are no side effects and thus
pub struct LruReplacement<Key, Value, S>
where
    Key: Eq + Hash + Send + Sync,
    S: Store<Key = Key, Value = Value>,
{
    store: S,
    index: RefCell<HashMap<KeyRef<Key>, NonNull<Entry<Key>>>>,
    max_capacity: NonZeroUsize,
    head: *mut Entry<Key>,
    tail: *mut Entry<Key>,
}

unsafe impl<Key, Value, S> Send for LruReplacement<Key, Value, S>
where
    Key: Eq + Hash + Clone + Send + Sync,
    Value: Clone + Send + Sync,
    S: Store<Key = Key, Value = Value>,
{
}

unsafe impl<Key, Value, S> Sync for LruReplacement<Key, Value, S>
where
    Key: Eq + Hash + Clone + Send + Sync,
    Value: Clone + Send + Sync,
    S: Store<Key = Key, Value = Value>,
{
}

impl<Key, Value, S> LruReplacement<Key, Value, S>
where
    Key: Eq + Hash + Clone + Send + Sync,
    Value: Clone + Send + Sync,
    S: Store<Key = Key, Value = Value>,
{
    pub async fn new(max_capacity: NonZeroUsize, store: S) -> CacheBrownsResult<Self> {
        let mut lru = Self {
            store,
            index: RefCell::new(HashMap::new()),
            max_capacity,
            head: Box::into_raw(Box::new(Entry::new_edge())),
            tail: Box::into_raw(Box::new(Entry::new_edge())),
        };

        unsafe {
            (*lru.head).next = lru.tail;
            (*lru.tail).previous = lru.head;
        }

        // If the data store is non-volatile, there might already be data present. Iterate through
        // the values to build out an arbitrary usage order.
        for key in lru.store.keys().await {
            lru.add_to_usage_order(key.clone());
        }

        lru.remove_excess_entries().await?;

        Ok(lru)
    }

    fn mark_as_most_recent<Q: Borrow<<LruReplacement<Key, Value, S> as Store>::Key>>(
        &self,
        key: &Q,
    ) {
        // Private function, it is never called on keys that don't exist.
        let entry = self
            .index
            .borrow()
            .get(&KeyRef::from(key))
            .unwrap()
            .as_ptr();
        self.detach(entry);
        self.attach_head(entry);
    }

    fn add_to_usage_order(&self, key: S::Key) {
        unsafe {
            let entry = Box::into_raw(Box::new(Entry::new(key.clone())));
            self.attach_head(entry);
            let key_ref = KeyRef {
                key: (*entry).key.as_ptr(),
            };
            self.index
                .borrow_mut()
                .insert(key_ref, NonNull::new_unchecked(entry));
        }
    }

    fn remove<Q: Borrow<<LruReplacement<Key, Value, S> as Store>::Key>>(&self, key: &Q) -> bool {
        if let Some(entry) = self.index.borrow_mut().remove(&KeyRef::from(key)) {
            let entry = entry.as_ptr();
            self.detach(entry);
            unsafe {
                let _ = Box::from_raw(entry);
            }

            return true;
        }

        false
    }

    fn detach(&self, entry: *mut Entry<Key>) {
        unsafe {
            (*(*entry).previous).next = (*entry).next;
            (*(*entry).next).previous = (*entry).previous;
        }
    }

    fn attach_head(&self, entry: *mut Entry<Key>) {
        unsafe {
            (*entry).next = (*self.head).next;
            (*entry).previous = self.head;
            (*self.head).next = entry;
            (*(*entry).next).previous = entry;
        }
    }

    async fn remove_excess_entries(&mut self) -> CacheBrownsResult<()> {
        while self.index.borrow().len() > self.max_capacity.get() {
            self.try_delete_last().await?;
        }

        Ok(())
    }

    async fn try_delete_last(&mut self) -> CacheBrownsResult<()> {
        unsafe {
            // SAFETY: max_capacity is non-zero, so if this executes tail.previous != head
            let _ = self
                .store
                .delete((*(*self.tail).previous).key.assume_init_ref())
                .await?;

            self.remove_last_entry_from_index();
        }

        Ok(())
    }

    unsafe fn remove_last_entry_from_index(&self) {
        let last_key = KeyRef {
            key: unsafe { &(*(*(*self.tail).previous).key.as_ptr()) },
        };

        let last_entry = self.index.borrow_mut().remove(&last_key).unwrap();
        let last_entry_ptr: *mut Entry<Key> = last_entry.as_ptr();

        (*last_entry_ptr).key.assume_init_drop();

        self.detach(last_entry_ptr);
        let _ = Box::from_raw(last_entry_ptr);
    }
}

impl<Key, Value, S> Drop for LruReplacement<Key, Value, S>
where
    Key: Eq + Hash + Send + Sync,
    S: Store<Key = Key, Value = Value>,
{
    fn drop(&mut self) {
        self.index
            .borrow_mut()
            .drain()
            .for_each(|(_, entry)| unsafe {
                let mut entry = *Box::from_raw(entry.as_ptr());
                // SAFETY: In order for this to be in the map, it has a value.
                entry.key.assume_init_drop();
            });

        unsafe {
            let _ = Box::from_raw(self.head);
            let _ = Box::from_raw(self.tail);
        }
    }
}

impl<Key, Value, S> Store for LruReplacement<Key, Value, S>
where
    Key: Eq + Hash + Clone + Send + Sync,
    Value: Clone + Send + Sync,
    S: Store<Key = Key, Value = Value>,
{
    type Key = Key;
    type Value = Value;

    // Avoid underlying [`keys`] implementation to guarantee an all memory operation.
    type KeyIterator = vec::IntoIter<Key>;

    // Because flush can fail, we can't rely on our own iterator as a potential optimization.
    type FlushResultIterator = S::FlushResultIterator;

    async fn get<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) -> Option<Self::Value> {
        let value = self.store.get(key).await;

        if value.is_some() {
            self.mark_as_most_recent(key);
        }

        value
    }

    async fn poke<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) {
        self.store.poke(key).await;
    }

    async fn peek<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) -> Option<Self::Value> {
        self.store.peek(key).await
    }

    async fn update(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()> {
        self.store.update(key, value).await
    }

    async fn put(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()> {
        if self.index.borrow().len() >= self.max_capacity.get()
            // If the key is already present, this is an update and no eviction should occur
            && !self.contains(&key).await
        {
            // We failed to evict, so to honor space limit we can't insert
            self.try_delete_last().await?;
        }

        // Update
        if self.contains(&key).await {
            self.mark_as_most_recent(&key);
        }
        // Insert
        else {
            self.add_to_usage_order(key.clone());
        }

        self.store.put(key, value).await
    }

    async fn delete<Q: Borrow<Self::Key> + Sync>(
        &mut self,
        key: &Q,
    ) -> CacheBrownsResult<Option<Self::Key>> {
        if self.remove(key) {
            return self.store.delete(key).await;
        }

        Ok(None)
    }

    async fn flush(&mut self) -> Self::FlushResultIterator {
        self.index.borrow_mut().clear();
        self.store.flush().await
    }

    async fn keys(&self) -> Self::KeyIterator {
        // We can optimize by guaranteeing a memory lookup checking the metadata instead of the
        // underlying store, which may or may not be in memory
        unsafe {
            self.index
                .borrow()
                .keys()
                .map(|k| (*k.key).clone())
                .collect_vec()
                .into_iter()
        }
    }

    async fn contains<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) -> bool {
        // We can optimize by guaranteeing a memory lookup checking the metadata instead of the
        // underlying store, which may or may not be in memory
        self.index.borrow().contains_key(&KeyRef::from(key))
    }

    async fn take<Q: Borrow<Self::Key> + Sync>(
        &mut self,
        _key: &Q,
    ) -> CacheBrownsResult<Option<(Self::Key, Self::Value)>> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        store::{
            memory::MemoryStore,
            replacement::lru::{KeyRef, LruReplacement},
            Store,
        },
        CacheBrownsResult,
    };
    use itertools::{assert_equal, Itertools};
    use std::{
        borrow::Borrow, collections::BTreeSet, fmt::Debug, hash::Hash, io::ErrorKind,
        num::NonZeroUsize, ptr, vec,
    };

    async fn lru(max_capacity: usize) -> LruReplacement<u32, u32, MemoryStore<u32, u32>> {
        let store = MemoryStore::new();
        LruReplacement::new(NonZeroUsize::new(max_capacity).unwrap(), store)
            .await
            .unwrap()
    }

    fn assert_key_is_most_recent<
        K: Eq + Hash + Debug + Send + Sync,
        V,
        S: Store<Key = K, Value = V>,
    >(
        store: &LruReplacement<K, V, S>,
        key: K,
    ) {
        unsafe {
            assert_eq!(key, (*(*store.head).next).key.assume_init_read());
        }
    }

    #[tokio::test]
    async fn empty_lru_can_drop() {
        let _ = lru(1).await;
    }

    #[tokio::test]
    async fn get_on_missing_key_no_side_effects() {
        let store = lru(1).await;

        assert_eq!(None, store.get(&0).await);
        assert_eq!(0, store.index.borrow().len());
    }

    #[tokio::test]
    async fn capacity_exceeded_oldest_key_removed() {
        let mut store = lru(1).await;
        store.put(0, 1).await.unwrap();
        store.put(1, 1).await.unwrap();

        assert_eq!(None, store.get(&0).await);
        assert_eq!(1, store.index.borrow().len());
        assert_key_is_most_recent(&store, 1);
    }

    #[tokio::test]
    async fn peek_no_side_effects() {
        let mut store = lru(2).await;
        store.put(0, 1).await.unwrap();
        store.put(1, 1).await.unwrap();

        assert_key_is_most_recent(&store, 1);
        store.peek(&0).await;
        assert_key_is_most_recent(&store, 1);
    }

    #[tokio::test]
    async fn get_marks_as_latest() {
        let mut store = lru(2).await;
        store.put(0, 1).await.unwrap();
        store.put(1, 1).await.unwrap();

        assert_key_is_most_recent(&store, 1);
        store.get(&0).await;
        assert_key_is_most_recent(&store, 0);
    }

    #[tokio::test]
    async fn put_existing_value_updates_and_marked_latest() {
        let mut store = lru(2).await;
        store.put(0, 1).await.unwrap();
        store.put(1, 1).await.unwrap();

        // Confirm latest as invariant
        assert_key_is_most_recent(&store, 1);

        store.put(0, 42).await.unwrap();

        // Confirm original value is now latest
        assert_key_is_most_recent(&store, 0);

        // Peak used to avoid side effects
        assert_eq!(42u32, store.peek(&0).await.unwrap())
    }

    #[tokio::test]
    async fn put_would_evict_but_is_update() {
        let mut store = lru(2).await;
        store.put(0, 1).await.unwrap();
        store.put(1, 1).await.unwrap();

        store.put(0, 2).await.unwrap();

        assert!(store.store.contains(&0).await);
        assert!(store.store.contains(&1).await);
        assert!(store.index.borrow().contains_key(&KeyRef::from(&0)));
        assert!(store.index.borrow().contains_key(&KeyRef::from(&1)));
        assert_eq!(2, store.store.keys().await.len());
        assert_eq!(2, store.index.borrow().len());
        assert_key_is_most_recent(&store, 0);
    }

    #[tokio::test]
    async fn usage_order_always_shifts_one_right() {
        let mut store = lru(5).await;
        store.put(0, 1).await.unwrap();
        store.put(1, 1).await.unwrap();
        store.put(2, 1).await.unwrap();
        store.put(3, 1).await.unwrap();
        store.put(4, 1).await.unwrap();

        validate_usage_order(vec![4, 3, 2, 1, 0], &store);

        store.get(&2).await;
        validate_usage_order(vec![2, 4, 3, 1, 0], &store);

        store.get(&1).await;
        validate_usage_order(vec![1, 2, 4, 3, 0], &store);

        store.get(&2).await;
        validate_usage_order(vec![2, 1, 4, 3, 0], &store);

        store.get(&0).await;
        validate_usage_order(vec![0, 2, 1, 4, 3], &store);
    }

    fn validate_usage_order(
        vec: Vec<u32>,
        store: &LruReplacement<u32, u32, MemoryStore<u32, u32>>,
    ) {
        unsafe {
            let mut ptr = (*store.head).next;

            for key in &vec {
                assert_eq!(*key, *(*ptr).key.assume_init_ref());
                ptr = (*ptr).next;
            }
        }
    }

    #[tokio::test]
    async fn delete_removes_from_index_and_propagates() {
        let mut store = lru(1).await;
        store.put(0, 1).await.unwrap();
        assert!(store.store.contains(&0).await);

        assert_eq!(None, store.delete(&1).await.unwrap());
        assert_eq!(Some(0), store.delete(&0).await.unwrap());

        assert_eq!(None, store.delete(&0).await.unwrap());
    }

    #[tokio::test]
    async fn flush_no_index_and_propagates() {
        let mut store = lru(3).await;
        store.put(0, 1).await.unwrap();
        store.put(1, 1).await.unwrap();
        store.put(2, 1).await.unwrap();

        let deleted_keys: BTreeSet<u32> = store.flush().await.map(|x| x.unwrap()).collect();
        assert_equal(BTreeSet::from_iter(vec![0, 1, 2]), deleted_keys);
        assert_eq!(0, store.index.borrow().len());
        assert_eq!(0, store.store.keys().await.len());
    }

    #[tokio::test]
    async fn store_has_initial_data_index_build() {
        let mut store = MemoryStore::new();
        store.put(0, 1).await.unwrap();
        store.put(1, 1).await.unwrap();
        store.put(2, 1).await.unwrap();

        let store = LruReplacement::new(NonZeroUsize::new(3).unwrap(), store)
            .await
            .unwrap();

        // Order isn't guaranteed, so just check values
        assert!(store.contains(&0).await);
        assert!(store.contains(&1).await);
        assert!(store.contains(&2).await);
        assert_eq!(3, store.index.borrow().len());
    }

    #[tokio::test]
    async fn store_has_too_much_initial_data_records_purged_valid_index() {
        let mut store = MemoryStore::new();
        store.put(1, 1).await.unwrap();
        store.put(2, 1).await.unwrap();
        store.put(4, 1).await.unwrap();

        let store = LruReplacement::new(NonZeroUsize::new(2).unwrap(), store)
            .await
            .unwrap();

        // Order isn't guaranteed, so just check that exactly one was deleted.
        let index_sum: i32 = store.index.borrow().keys().map(|k| unsafe { *k.key }).sum();
        let store_sum: i32 = store.store.keys().await.sum();
        assert!(index_sum == 3 || index_sum == 6 || index_sum == 5);

        // We picked powers of 2, so checking sum rather than all values is sufficient for equality.
        assert_eq!(store_sum, index_sum);
        assert_eq!(2, store.index.borrow().len());
        assert_eq!(2, store.store.keys().await.len());
    }

    #[tokio::test]
    async fn store_has_too_much_initial_data_purge_fails() {
        assert!(LruReplacement::new(
            NonZeroUsize::new(1).unwrap(),
            FailingMemoryStore::new_with_n_items(3)
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn should_fail() {
        let mut store = lru(10).await;
        store.put(1, 1).await.unwrap();
        store.put(2, 1).await.unwrap();
        store.put(3, 1).await.unwrap();
        let store = std::sync::Arc::new(tokio::sync::Mutex::new(store));

        let store_copy = store.clone();

        tokio::task::spawn(async move {
            store_copy.lock().await.get(&2).await;
            store_copy.lock().await.get(&3).await;
        });

        for _i in 1..100000 {
            store.lock().await.get(&1).await;
            store.lock().await.get(&2).await;
        }
    }

    #[tokio::test]
    async fn contains_and_keys_match_underlying_store() {
        let mut store = lru(5).await;
        let underlying_store = unsafe { &(*ptr::from_ref(&store.store)) };

        store.put(0, 0).await.unwrap();
        assert_keys_and_contains_match(underlying_store, &store).await;

        store.put(1, 1).await.unwrap();
        assert_keys_and_contains_match(underlying_store, &store).await;

        store.put(1, 2).await.unwrap();
        assert_keys_and_contains_match(underlying_store, &store).await;

        store.put(2, 2).await.unwrap();
        assert_keys_and_contains_match(underlying_store, &store).await;

        store.put(3, 2).await.unwrap();
        assert_keys_and_contains_match(underlying_store, &store).await;

        store.put(4, 2).await.unwrap();
        assert_keys_and_contains_match(underlying_store, &store).await;

        store.put(4, 2).await.unwrap();
        assert_keys_and_contains_match(underlying_store, &store).await;

        store.put(4, 2).await.unwrap();
        assert_keys_and_contains_match(underlying_store, &store).await;

        store.put(5, 2).await.unwrap();
        assert_keys_and_contains_match(underlying_store, &store).await;

        store.put(6, 2).await.unwrap();
        assert_keys_and_contains_match(underlying_store, &store).await;

        store.put(5, 2).await.unwrap();
        assert_keys_and_contains_match(underlying_store, &store).await;

        store.delete(&4).await.unwrap();
        assert_keys_and_contains_match(underlying_store, &store).await;

        assert!(store.delete(&0).await.is_ok_and(|x| x.is_none()));
        assert_keys_and_contains_match(underlying_store, &store).await;
    }

    async fn assert_keys_and_contains_match<S, S2>(store: &S, store2: &S2)
    where
        S: Store<Key = u32>,
        S2: Store<Key = u32>,
    {
        for i in 0..7 {
            assert_eq!(store.contains(&i).await, store2.contains(&i).await);
        }

        assert_equal::<BTreeSet<u32>, BTreeSet<u32>>(
            BTreeSet::from_iter(store.keys().await),
            BTreeSet::from_iter(store2.keys().await),
        );
    }

    #[tokio::test]
    async fn put_exits_when_underlying_store_fails() {
        let mut store =
            LruReplacement::new(NonZeroUsize::new(1).unwrap(), FailingMemoryStore::new())
                .await
                .unwrap();
        assert!(store.put(1, 1).await.is_ok());
        assert!(store.put(2, 1).await.is_err());

        assert!(store.contains(&1).await);
        assert!(!store.contains(&2).await);
    }

    struct FailingMemoryStore {
        vec: Vec<u32>,
    }

    impl FailingMemoryStore {
        fn new() -> Self {
            Self { vec: vec![] }
        }

        fn new_with_n_items(n: u32) -> Self {
            let mut vec = Vec::with_capacity(n as usize);

            for i in 0..n {
                vec.push(i);
            }

            Self { vec }
        }
    }

    impl Store for FailingMemoryStore {
        type Key = u32;
        type Value = u32;
        type KeyIterator = vec::IntoIter<u32>;
        type FlushResultIterator = vec::IntoIter<CacheBrownsResult<u32>>;

        async fn get<Q: Borrow<Self::Key> + Sync>(&self, _key: &Q) -> Option<Self::Value> {
            unimplemented!()
        }

        async fn poke<Q: Borrow<Self::Key> + Sync>(&self, _key: &Q) {
            unimplemented!()
        }

        async fn peek<Q: Borrow<Self::Key> + Sync>(&self, _key: &Q) -> Option<Self::Value> {
            unimplemented!()
        }

        async fn put(&mut self, _key: Self::Key, _value: Self::Value) -> CacheBrownsResult<()> {
            Ok(())
        }

        async fn update(&mut self, _key: Self::Key, _value: Self::Value) -> CacheBrownsResult<()> {
            unimplemented!()
        }

        async fn delete<Q: Borrow<Self::Key> + Sync>(
            &mut self,
            _key: &Q,
        ) -> CacheBrownsResult<Option<Self::Key>> {
            Err(Box::new(std::io::Error::new(ErrorKind::Other, "stub")))
        }

        async fn take<Q: Borrow<Self::Key> + Sync>(
            &mut self,
            _key: &Q,
        ) -> CacheBrownsResult<Option<(Self::Key, Self::Value)>> {
            unimplemented!()
        }

        async fn flush(&mut self) -> Self::FlushResultIterator {
            unimplemented!()
        }

        async fn keys(&self) -> Self::KeyIterator {
            self.vec.iter().cloned().collect_vec().into_iter()
        }

        async fn contains<Q: Borrow<Self::Key> + Sync>(&self, _key: &Q) -> bool {
            unimplemented!()
        }
    }
}
