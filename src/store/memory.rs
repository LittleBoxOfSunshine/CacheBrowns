use crate::store::Store;
use crate::CacheBrownsResult;
use itertools::Itertools;
use std::borrow::{Borrow, Cow};
use std::collections::{hash_map, HashMap};
use std::vec;

/// A [`MemoryStore`] is a wrapper around [`HashMap`] that satisfies [`Store`].
#[derive(Default)]
pub struct MemoryStore<Key, Value> {
    data: HashMap<Key, Value>,
}

impl<Key, Value> MemoryStore<Key, Value> {
    pub fn new() -> Self {
        MemoryStore {
            data: HashMap::new(),
        }
    }
}

impl<Key, Value> From<HashMap<Key, Value>> for MemoryStore<Key, Value> {
    fn from(data: HashMap<Key, Value>) -> Self {
        Self { data }
    }
}

impl<Key: Eq + std::hash::Hash + Send + Sync, Value: Clone + Send + Sync> Store
    for MemoryStore<Key, Value>
{
    type Key = Key;
    type Value = Value;
    type KeyRefIterator<'k> = hash_map::Keys<'k, Key, Value> where Key: 'k, Value: 'k;
    type FlushResultIterator = vec::IntoIter<CacheBrownsResult<Key>>;

    async fn get<Q: Borrow<Key> + Sync>(&self, key: &Q) -> Option<Cow<Self::Value>> {
        self.peek(key).await
    }

    async fn poke<Q: Borrow<Self::Key> + Sync>(&self, _key: &Q) {}

    async fn peek<Q: Borrow<Key> + Sync>(&self, key: &Q) -> Option<Cow<Value>> {
        self.data.get(key.borrow()).map(|v| Cow::Borrowed(v))
    }

    async fn put(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()> {
        self.data.insert(key, value);
        Ok(())
    }

    async fn update(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()> {
        self.data.entry(key).and_modify(|v| *v = value);
        Ok(())
    }

    async fn delete<Q: Borrow<Self::Key> + Sync>(&mut self, key: &Q) -> CacheBrownsResult<Option<Key>> {
        Ok(self.data.remove_entry(key.borrow()).map(|(k, _v)| k))
    }

    async fn take<Q: Borrow<Self::Key> + Sync>(
        &mut self,
        key: &Q,
    ) -> CacheBrownsResult<Option<(Self::Key, Self::Value)>> {
        Ok(self.data.remove_entry(key.borrow()))
    }

    async fn flush(&mut self) -> Self::FlushResultIterator {
        self.data
            .drain()
            .map(|(k, _v)| Ok(k))
            .collect_vec()
            .into_iter()
    }

    async fn keys(&self) -> Self::KeyRefIterator<'_> {
        self.data.keys()
    }

    async fn contains<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) -> bool {
        self.data.contains_key(key.borrow())
    }
}

#[cfg(test)]
mod tests {
    use crate::store::memory::MemoryStore;
    use crate::store::Store;
    use std::collections::HashMap;

    #[tokio::test]
    async fn flush() {
        let mut store = MemoryStore::new();
        store.put(&1, 1).await.unwrap();
        store.put(&2, 1).await.unwrap();
        store.put(&3, 1).await.unwrap();

        assert_eq!(3, store.keys().await.len());

        store.flush();

        assert_eq!(0, store.keys().await.len());
    }

    #[tokio::test]
    async fn from_hashmap() {
        let mut map = HashMap::new();
        map.insert(1, 1);

        let store = MemoryStore::from(map);

        assert_eq!(1, *store.get(&1).await.unwrap());
        assert_eq!(1, store.keys().await.len())
    }
}
