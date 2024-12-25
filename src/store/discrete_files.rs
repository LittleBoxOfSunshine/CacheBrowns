use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::map;
use std::borrow::{Borrow, Cow};
use std::collections::hash_map::Drain;
use std::collections::{hash_map, HashMap};
use std::fmt::Debug;
use std::fs::File;
use std::hash::Hash;
use std::io::{BufReader, BufWriter, Error, ErrorKind};
use std::iter::Map;
use std::path::{Path, PathBuf};
use std::{fs, vec};
use uuid::Uuid;

// TODO: Support the notion of versioning?

use crate::store::Store;
use crate::CacheBrownsResult;

/// Stores each element in a unique file, serialized. A single directory is used to represent the
/// store. No other files are permitted to be co-located in the directory. Violating this requirement
/// at startup, or at any other point during its lifetime may lead to undefined behavior.
///
/// An index of key to path mappings is held in memory. This index is rebuilt from existing data on
/// startup by iterating over the files in the directory. If a record fails to deserialize, it is
/// silently dropped. Since this is a cache, the rehydration is best-effort. The paths of all
/// failures are returned.
///
/// Before using, *strongly consider* using [`DiscreteFileStoreVolatile`] instead. Do you really
/// *need* this cache to rehydrate without hitting the source of record? Is the cost high when weighed
/// against the frequency of cold starts? Using the Non-Volatile variant prevents using restarts to
/// clear corruption. Additionally, the application must consider N vs N+1 schema issues when
/// downgrading or upgrading the executable.
pub struct DiscreteFileStoreNonVolatile<Key, Value, Serde>
where
    Key: Clone + Eq + Hash + Serialize + for<'a> Deserialize<'a> + Debug,
    Value: Serialize + for<'a> Deserialize<'a>,
    Serde: DiscreteFileSerializerDeserializer<Value>,
{
    cache_directory: PathBuf,
    index: HashMap<Key, PathBuf>,
    phantom_serde: std::marker::PhantomData<Serde>,
    phantom_value: std::marker::PhantomData<Value>,
}

impl<Key, Value, Serde> DiscreteFileStoreNonVolatile<Key, Record<Key, Value>, Serde>
where
    Key: Clone + Eq + Hash + Serialize + for<'a> Deserialize<'a> + Debug,
    Value: Clone + Serialize + for<'a> Deserialize<'a>,
    Serde: DiscreteFileSerializerDeserializer<Record<Key, Value>> + Send,
{
    pub fn new<P: Into<PathBuf>>(
        cache_directory: P,
    ) -> Result<(Self, impl Iterator<Item = PathBuf>), Error> {
        let mut store = Self {
            cache_directory: cache_directory.into(),
            index: HashMap::new(),
            phantom_serde: Default::default(),
            phantom_value: Default::default(),
        };
        let failures = Self::rehydrate_index(&mut store)?;

        Ok((store, failures))
    }

    fn rehydrate_index(
        store: &mut DiscreteFileStoreNonVolatile<Key, Record<Key, Value>, Serde>,
    ) -> Result<impl Iterator<Item = PathBuf>, Error> {
        fs::create_dir_all(store.cache_directory.as_path())?;
        let mut failures = Vec::new();

        if let Ok(paths) = fs::read_dir(store.cache_directory.as_path()) {
            for dir_entry in paths.flatten() {
                let path = dir_entry.path();

                if let Some(record) = get::<Serde, Record<Key, Value>>(&path) {
                    if store.index.insert(record.key.clone(), path).is_some() {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            format!("Duplicate key {:?}", record.key),
                        ));
                    }
                } else {
                    failures.push(path);
                }
            }
        }

        Ok(failures.into_iter())
    }
}

impl<Key, Value, Serde> Store for DiscreteFileStoreNonVolatile<Key, Record<Key, Value>, Serde>
where
    Key: Clone + Eq + Hash + Serialize + for<'a> Deserialize<'a> + Debug + Send + Sync,
    Value: Clone + Serialize + for<'a> Deserialize<'a> + Send + Sync,
    Serde: DiscreteFileSerializerDeserializer<Record<Key, Value>> + Send + Sync,
{
    type Key = Key;
    type Value = Value;
    type KeyIterator = vec::IntoIter<Key>;
    type FlushResultIterator = vec::IntoIter<CacheBrownsResult<Key>>;

    async fn get<Q: Borrow<Key> + Sync>(&self, key: &Q) -> Option<Self::Value> {
        self.peek(key).await
    }

    async fn poke<Q: Borrow<Self::Key> + Sync>(&self, _key: &Q) {}

    async fn peek<Q: Borrow<Key> + Sync>(&self, key: &Q) -> Option<Value> {
        if let Some(path) = self.index.get(key.borrow()) {
            return Some(get::<Serde, Record<Key, Value>>(path)?.value);
        }

        None
    }

    async fn put(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()> {
        let path = get_or_create_index_entry(&self.cache_directory, &mut self.index, key.clone());
        put::<Serde, Record<Key, Value>>(path, Record { key, value })
    }

    async fn update(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()> {
        if self.contains(&key).await {
            return self.put(key, value).await;
        }

        Ok(())
    }

    async fn delete<Q: Borrow<Self::Key> + Sync>(
        &mut self,
        key: &Q,
    ) -> CacheBrownsResult<Option<Key>> {
        delete::<Key>(&mut self.index, key.borrow())
    }

    async fn take<Q: Borrow<Self::Key> + Sync>(
        &mut self,
        key: &Q,
    ) -> CacheBrownsResult<Option<(Self::Key, Self::Value)>> {
        Ok(
            take::<Serde, Key, Record<Key, Value>>(&mut self.index, key.borrow())?
                .map(|(k, v)| (k, v.value)),
        )
    }

    async fn flush(&mut self) -> Self::FlushResultIterator {
        flush::<Key>(self.index.drain())
    }

    //noinspection DuplicatedCode
    async fn keys(&self) -> Self::KeyIterator {
        self.index.keys().cloned().collect_vec().into_iter()
    }

    async fn contains<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) -> bool {
        self.index.contains_key(key.borrow())
    }
}

/// Stores each element in a unique file, serialized. A single directory is used to represent the
/// store. No other files are permitted to be co-located in the directory. Violating this requirement
/// post-startup may lead to undefined behavior. The volatile variant attempts to clear the target
/// directory when it is created, which is a fallible operation.
///
/// [`DiscreteFileStoreVolatile`] will also purge files when it is dropped. The purge during
/// construction is done handle orphaned data from a previous unexpected exit.
///
/// An index of key to path mappings is held in memory.
///
/// This variant is generally preferred over [`DiscreteFileStoreNonVolatile`] as it is easier to
/// work with and safer during executable lifecycle events, upgrades, and downgrades. By definition,
/// it's safe to purge a cache (it's not the source of truth).
pub struct DiscreteFileStoreVolatile<Key, Value, Serde>
where
    Key: Clone + Eq + Hash + Serialize + for<'a> Deserialize<'a> + Debug,
    Value: Serialize + for<'a> Deserialize<'a>,
    Serde: DiscreteFileSerializerDeserializer<Value>,
{
    cache_directory: PathBuf,
    index: HashMap<Key, PathBuf>,
    phantom_serde: std::marker::PhantomData<Serde>,
    phantom_value: std::marker::PhantomData<Value>,
}

impl<Key, Value, Serde> DiscreteFileStoreVolatile<Key, Value, Serde>
where
    Key: Clone + Eq + Hash + Serialize + for<'a> Deserialize<'a> + Debug,
    Value: Serialize + for<'a> Deserialize<'a>,
    Serde: DiscreteFileSerializerDeserializer<Value>,
{
    /// When created, the [`DiscreteFileStoreVolatile`] will check the target directory to ensure it
    /// exists and is empty. If the target exists and has other files, it will attempt to purge them.
    /// If the files cannot be deleted, construction fails to protect the validity of the cache.
    /// If the target directory does not exist, it will be automatically created.
    pub fn new<P: Into<PathBuf>>(cache_directory: P) -> Result<Self, Error> {
        let cache_directory = cache_directory.into();
        fs::remove_dir_all(cache_directory.as_path())?;
        fs::create_dir_all(cache_directory.as_path())?;

        Ok(Self {
            cache_directory,
            index: HashMap::new(),
            phantom_serde: Default::default(),
            phantom_value: Default::default(),
        })
    }
}

impl<Key, Value, Serde> Drop for DiscreteFileStoreVolatile<Key, Value, Serde>
where
    Key: Clone + Eq + Hash + Serialize + for<'a> Deserialize<'a> + Debug,
    Value: Serialize + for<'a> Deserialize<'a>,
    Serde: DiscreteFileSerializerDeserializer<Value>,
{
    fn drop(&mut self) {
        // Best-effort. Any errors can be address by the attempt made on next construction.
        let _ = fs::remove_dir_all(self.cache_directory.as_path());
    }
}

impl<Key, Value, Serde> Store for DiscreteFileStoreVolatile<Key, Value, Serde>
where
    Key: Clone + Eq + Hash + Serialize + for<'a> Deserialize<'a> + Debug + Send + Sync,
    Value: Clone + Serialize + for<'a> Deserialize<'a> + Send + Sync,
    Serde: DiscreteFileSerializerDeserializer<Value> + Send + Sync,
{
    type Key = Key;
    type Value = Value;
    type KeyIterator = vec::IntoIter<Key>;
    type FlushResultIterator = vec::IntoIter<CacheBrownsResult<Key>>;

    async fn get<Q: Borrow<Key> + Sync>(&self, key: &Q) -> Option<Self::Value> {
        self.peek(key).await
    }

    async fn poke<Q: Borrow<Self::Key> + Sync>(&self, _key: &Q) {}

    async fn peek<Q: Borrow<Key> + Sync>(&self, key: &Q) -> Option<Value> {
        if let Some(path) = self.index.get(key.borrow()) {
            return get::<Serde, Value>(path);
        }

        None
    }

    async fn put(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()> {
        let path = get_or_create_index_entry(&self.cache_directory, &mut self.index, key);
        put::<Serde, Value>(path, value)
    }

    async fn update(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()> {
        if self.contains(&key).await {
            return self.put(key, value).await;
        }

        Ok(())
    }

    async fn delete<Q: Borrow<Self::Key> + Sync>(
        &mut self,
        key: &Q,
    ) -> CacheBrownsResult<Option<Key>> {
        delete::<Key>(&mut self.index, key.borrow())
    }

    async fn take<Q: Borrow<Self::Key> + Sync>(
        &mut self,
        key: &Q,
    ) -> CacheBrownsResult<Option<(Self::Key, Self::Value)>> {
        // let record =
        take::<Serde, Key, Value>(&mut self.index, key.borrow())
        //?;

        // match record {
        //     None => Ok(None),
        //     Some(record) => Ok(Some((record.0, Cow::Owned(record.1))))
        // }
    }

    async fn flush(&mut self) -> Self::FlushResultIterator {
        flush::<Key>(self.index.drain())
    }

    async fn keys(&self) -> Self::KeyIterator {
        self.index.keys().cloned().collect_vec().into_iter()
    }

    async fn contains<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) -> bool {
        self.index.contains_key(key.borrow())
    }
}

// Shared implementation of get
fn get<Serde, Value>(path: &PathBuf) -> Option<Value>
where
    Value: Clone + Serialize + for<'a> Deserialize<'a>,
    Serde: DiscreteFileSerializerDeserializer<Value> + Send,
{
    if let Ok(file) = File::open(path) {
        return Serde::deserialize(BufReader::new(file));
    }

    None
}

// Shared implementation of put
fn put<Serde, Value>(path: PathBuf, value: Value) -> CacheBrownsResult<()>
where
    Value: Serialize + for<'a> Deserialize<'a>,
    Serde: DiscreteFileSerializerDeserializer<Value>,
{
    match File::create(path) {
        Ok(file) => Serde::serialize(BufWriter::new(file), &value),
        Err(e) => Err(Box::new(e)),
    }
}

// Shared implementation of delete
fn delete<Key>(index: &mut HashMap<Key, PathBuf>, key: &Key) -> CacheBrownsResult<Option<Key>>
where
    Key: Eq + Hash,
{
    if let Some((key, path)) = index.remove_entry(key) {
        fs::remove_file(path)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + 'static>)?;
        return Ok(Some(key));
    }

    Ok(None)
}

fn take<Serde, Key, Value>(
    index: &mut HashMap<Key, PathBuf>,
    key: &Key,
) -> CacheBrownsResult<Option<(Key, Value)>>
where
    Key: Eq + Hash,
    Value: Clone + Serialize + for<'a> Deserialize<'a>,
    Serde: DiscreteFileSerializerDeserializer<Value> + Send,
{
    // Checking index an extra time is way faster than going to disk.
    if let Some(path) = index.get(key) {
        if let Some(value) = get::<Serde, Value>(path) {
            if let Some(key) = delete(index, key)? {
                // SAFETY: Above, we already checked confirmed key is present in index
                index.remove(&key).unwrap();
                return Ok(Some((key, value)));
            }
        }
    }

    Ok(None)
}

// Shared implementation of flush
fn flush<Key>(records: Drain<'_, Key, PathBuf>) -> vec::IntoIter<CacheBrownsResult<Key>> {
    records
        .into_iter()
        .map(|(k, path)| {
            fs::remove_file(path)
                .map(|_| k)
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + 'static>)
        })
        .collect_vec()
        .into_iter()
}

// Shared implementation, sub-step of put operations.
fn get_or_create_index_entry<Key>(
    cache_directory: &Path,
    index: &mut HashMap<Key, PathBuf>,
    key: Key,
) -> PathBuf
where
    Key: Eq + Hash,
{
    index
        .entry(key)
        .or_insert_with(|| -> PathBuf {
            let mut path = PathBuf::new();
            path.push(cache_directory);
            path.push(Uuid::new_v4().hyphenated().to_string());
            path
        })
        .clone()
}

// Wraps user data for serialization purposes
#[derive(Serialize, Deserialize, Clone)]
struct Record<Key, Value> {
    pub key: Key,
    pub value: Value,
}

/// A universal Serialize / Deserialize contract. While Serde provides traits for serializing and
/// deserializing data, there is no generic notion of how the actual Serializers and Deserializers
/// should be used. This allows any implementation to be plugged in.
///
/// Buffered reads are used because if you are writing to disk either:
///
/// 1. You are working with large data.
/// 2. You are just persisting, but everything is small and performance is fine anyway. By
///    nature of having selected discrete files you expect a fair amount of I/O sys-calls.
pub trait DiscreteFileSerializerDeserializer<Value>
where
    for<'a> Value: Serialize + Deserialize<'a>,
{
    fn serialize(buffered_writer: BufWriter<File>, value: &Value) -> CacheBrownsResult<()>;

    fn deserialize(buffered_reader: BufReader<File>) -> Option<Value>;
}

// TODO: dyn Error needs to propagate through maybe? Or at least a logging call back?
// This process will determine the outcome of the old todo off is this too laborious since the
// trait functions are infallible or yes/no

/// Implements serde
pub struct JsonDiscreteFileSerializerDeserializer<Value>
where
    for<'a> Value: Serialize + Deserialize<'a>,
{
    phantom: std::marker::PhantomData<Value>,
}

impl<Value> DiscreteFileSerializerDeserializer<Value>
    for JsonDiscreteFileSerializerDeserializer<Value>
where
    for<'a> Value: Serialize + Deserialize<'a>,
{
    fn serialize(buffered_writer: BufWriter<File>, value: &Value) -> CacheBrownsResult<()> {
        Ok(serde_json::to_writer(buffered_writer, &value)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + 'static>)?)
    }

    fn deserialize(buffered_reader: BufReader<File>) -> Option<Value> {
        serde_json::from_reader(buffered_reader).ok()
    }
}

pub struct BincodeDiscreteFileSerializerDeserializer<Value>
where
    for<'a> Value: Serialize + Deserialize<'a>,
{
    phantom: std::marker::PhantomData<Value>,
}

impl<Value> DiscreteFileSerializerDeserializer<Value>
    for BincodeDiscreteFileSerializerDeserializer<Value>
where
    for<'a> Value: Serialize + Deserialize<'a>,
{
    fn serialize(buffered_writer: BufWriter<File>, value: &Value) -> CacheBrownsResult<()> {
        Ok(bincode::serialize_into(buffered_writer, &value)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + 'static>)?)
    }

    fn deserialize(buffered_reader: BufReader<File>) -> Option<Value> {
        serde_json::from_reader(buffered_reader).ok()
    }
}

pub type DiscreteFileStoreNonVolatileBincode<Key, Value> =
    DiscreteFileStoreNonVolatile<Key, Value, BincodeDiscreteFileSerializerDeserializer<Value>>;
pub type DiscreteFileStoreVolatileBincode<Key, Value> =
    DiscreteFileStoreVolatile<Key, Value, BincodeDiscreteFileSerializerDeserializer<Value>>;
pub type DiscreteFileStoreNonVolatileJson<Key, Value> =
    DiscreteFileStoreNonVolatile<Key, Value, JsonDiscreteFileSerializerDeserializer<Value>>;
pub type DiscreteFileStoreVolatileJson<Key, Value> =
    DiscreteFileStoreVolatile<Key, Value, JsonDiscreteFileSerializerDeserializer<Value>>;

#[cfg(test)]
mod tests {
    use crate::store::discrete_files::{
        DiscreteFileStoreNonVolatile, DiscreteFileStoreNonVolatileJson,
        DiscreteFileStoreVolatileJson, JsonDiscreteFileSerializerDeserializer, Record,
    };
    use crate::store::Store;
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeSet;
    use std::fs;
    use std::fs::File;
    use std::future::Future;
    use std::io::{Error, ErrorKind, Write};
    use std::path::PathBuf;
    use tempdir::TempDir;

    const VALID_DATA1_KEY: u32 = 42;
    const VALID_DATA2_KEY: u32 = 43;
    const VALID_DATA1: &str = "{\"key\": 42, \"value\": 42}";
    const VALID_DATA2: &str = "{\"key\": 43, \"value\": 42}";

    async fn validate_against_volatile<
        F: Fn(DiscreteFileStoreVolatileJson<u32, u32>, TempDir) -> Fut,
        Fut: Future<Output = ()>,
    >(
        f: F,
    ) {
        let (store, dir) = empty_volatile_store();
        f(store, dir).await;
    }

    async fn validate_against_non_volatile<
        F: Fn(DiscreteFileStoreNonVolatileJson<u32, Record<u32, u32>>, TempDir) -> Fut,
        Fut: Future<Output = ()>,
    >(
        f: F,
    ) {
        let (store, dir) = empty_non_volatile_store();
        f(store, dir).await;
    }

    macro_rules! flush_clears_dir {
        ($store: ident, $dir: ident) => {
            $store.put(42, 42).await.unwrap();
            assert!($store.contains(&42).await);

            $store.flush().await;
            assert_eq!(0, fs::read_dir($dir).unwrap().count())
        };
    }

    #[tokio::test]
    async fn flush_clears_dir() {
        validate_against_volatile(|mut store, dir| async move {
            flush_clears_dir!(store, dir);
        })
        .await;
        validate_against_non_volatile(|mut store, dir| async move {
            flush_clears_dir!(store, dir);
        })
        .await;
    }

    macro_rules! contains {
        ($store: ident) => {
            assert!(!$store.contains(&42).await);

            $store.put(42, 42).await.unwrap();
            assert!($store.contains(&42).await);

            $store.put(45, 42).await.unwrap();
            assert!($store.contains(&42).await);

            $store.put(42, 0).await.unwrap();
            assert!($store.contains(&42).await);

            $store.delete(&42).await.unwrap();
            assert!(!$store.contains(&42).await);
        };
    }

    #[tokio::test]
    async fn contains() {
        validate_against_volatile(|mut store, dir| async move {
            let _dir = dir;
            contains!(store);
        })
        .await;
        validate_against_non_volatile(|mut store, dir| async move {
            let _dir = dir;
            contains!(store);
        })
        .await;
    }

    macro_rules! get_or_peek {
        ($func: ident, $store: ident) => {
            assert_eq!(None, $store.get(&42).await);

            $store.put(42, 42).await.unwrap();
            assert_eq!(42, $store.$func(&42).await.unwrap());
            assert_eq!(None, $store.$func(&45).await);

            $store.put(45, 42).await.unwrap();
            assert_eq!(42, $store.$func(&42).await.unwrap());
            assert_eq!(42, $store.$func(&45).await.unwrap());

            $store.put(42, 55).await.unwrap();
            assert_eq!(55, $store.$func(&42).await.unwrap());
            assert_eq!(42, $store.$func(&45).await.unwrap());
        };
    }

    #[tokio::test]
    async fn get() {
        validate_against_volatile(|mut store, dir| async move {
            let _dir = dir;
            get_or_peek!(get, store);
        })
        .await;
        validate_against_non_volatile(|mut store, dir| async move {
            let _dir = dir;
            get_or_peek!(get, store);
        })
        .await;
    }

    #[tokio::test]
    async fn peek() {
        validate_against_volatile(|mut store, dir| async move {
            let _dir = dir;
            get_or_peek!(peek, store);
        })
        .await;
        validate_against_non_volatile(|mut store, dir| async move {
            let _dir = dir;
            get_or_peek!(peek, store);
        })
        .await;
    }

    macro_rules! keys {
        ($store: ident) => {
            assert_eq!(0, $store.keys().await.count());

            $store.put(42, 42).await.unwrap();
            assert_eq!(1, $store.keys().await.count());

            $store.put(45, 45).await.unwrap();
            assert_eq!(2, $store.keys().await.count());

            $store.put(45, 50).await.unwrap();
            let keys: BTreeSet<u32> = $store.keys().await.collect();
            assert!(keys.contains(&42));
            assert!(keys.contains(&45));
            assert_eq!(2, keys.len());
        };
    }

    #[tokio::test]
    async fn keys() {
        validate_against_volatile(|mut store, dir| async move {
            let _dir = dir;
            keys!(store);
        })
        .await;
        validate_against_non_volatile(|mut store, dir| async move {
            let _dir = dir;
            keys!(store);
        })
        .await;
    }

    macro_rules! delete {
        ($store: ident) => {
            $store.put(42, 42).await.unwrap();
            $store.put(45, 42).await.unwrap();
            assert!($store.contains(&42).await);
            assert!($store.contains(&45).await);

            $store.delete(&42).await.unwrap();
            assert!(!$store.contains(&42).await);
            assert!($store.contains(&45).await);
            assert!($store.delete(&42).await.unwrap().is_none());

            $store.delete(&45).await.unwrap();
            assert!(!$store.contains(&42).await);
            assert!(!$store.contains(&45).await);
        };
    }

    #[tokio::test]
    async fn delete() {
        validate_against_volatile(|mut store, dir| async move {
            let _dir = dir;
            delete!(store);
        })
        .await;
        validate_against_non_volatile(|mut store, dir| async move {
            let _dir = dir;
            delete!(store);
        })
        .await;
    }

    #[tokio::test]
    async fn volatile_store_clears_pre_existing_data() {
        let (store, _dir) = create_volatile_scenario().unwrap();
        assert_eq!(0, store.keys().await.count());
    }

    #[tokio::test]
    async fn volatile_store_clears_clears_data_on_clean_exit() {
        let (mut store, dir) = create_volatile_scenario().unwrap();
        store.put(45, 45).await.unwrap();

        assert_eq!(1, store.keys().await.count());
        assert_eq!(1, std::fs::read_dir(dir.path()).unwrap().count());
        assert!(std::fs::metadata(dir.path()).unwrap().is_dir());

        drop(store);

        assert!(std::fs::metadata(dir.path()).is_err_and(|e| e.kind() == ErrorKind::NotFound));
    }

    #[tokio::test]
    async fn non_volatile_rehydrates() {
        let (store, _dir) = create_non_volatile_scenario(true).unwrap();
        assert!(store.0.contains(&VALID_DATA1_KEY).await);
        assert_eq!(
            VALID_DATA1_KEY,
            store.0.get(&VALID_DATA1_KEY).await.unwrap()
        );
        assert!(store.0.contains(&VALID_DATA2_KEY).await);
        assert_eq!(42, store.0.get(&VALID_DATA2_KEY).await.unwrap());
    }

    #[tokio::test]
    async fn non_volatile_rehydrate_corrupt_data_dropped() {
        let (store, _dir) = create_non_volatile_scenario(false).unwrap();
        assert!(store.0.contains(&VALID_DATA1_KEY).await);
        assert_eq!(
            VALID_DATA1_KEY,
            store.0.get(&VALID_DATA1_KEY).await.unwrap()
        );
        assert!(!store.0.contains(&VALID_DATA2_KEY).await);
    }

    #[allow(clippy::type_complexity)]
    fn non_volatile_from_path(
        dir: &TempDir,
    ) -> Result<
        (
            DiscreteFileStoreNonVolatile<
                u32,
                Record<u32, u32>,
                JsonDiscreteFileSerializerDeserializer<Record<u32, u32>>,
            >,
            impl Iterator<Item = PathBuf> + Sized + '_,
        ),
        Error,
    > {
        DiscreteFileStoreNonVolatileJson::new(dir.path())
    }

    #[tokio::test]
    async fn non_volatile_rehydrate_duplicate_key_err() {
        let dir = create_with_valid_file();
        create_valid_data_1_in_dir(&dir, "z");
        assert!(non_volatile_from_path(&dir).is_err());
    }

    #[tokio::test]
    async fn non_volatile_rehydrate_from_empty() {
        let dir = TempDir::new("test").unwrap();
        let store = non_volatile_from_path(&dir).unwrap().0;
        assert_eq!(0, store.keys().await.count());
    }

    #[tokio::test]
    async fn serialize_deserialize_struct_happy_path() {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        struct Bar {
            foo: i32,
        }
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        struct Foo {
            bar: usize,
            cow: (Option<bool>, Bar),
        }

        let foo = Foo {
            bar: 123456,
            cow: (Some(false), Bar { foo: -42 }),
        };

        let dir = TempDir::new("test").unwrap();
        let mut store = DiscreteFileStoreVolatileJson::new(dir.path()).unwrap();
        store.put(32, foo.clone()).await.unwrap();

        assert_eq!(foo, store.get(&32).await.unwrap())
    }

    fn empty_volatile_store() -> (DiscreteFileStoreVolatileJson<u32, u32>, TempDir) {
        let dir = TempDir::new("test").unwrap();
        (DiscreteFileStoreVolatileJson::new(dir.path()).unwrap(), dir)
    }

    fn empty_non_volatile_store() -> (
        DiscreteFileStoreNonVolatileJson<u32, Record<u32, u32>>,
        TempDir,
    ) {
        let dir = TempDir::new("test").unwrap();
        let store = DiscreteFileStoreNonVolatileJson::new(dir.path()).unwrap().0;
        (store, dir)
    }

    fn create_volatile_scenario(
    ) -> Result<(DiscreteFileStoreVolatileJson<u32, u32>, TempDir), Error> {
        create_files(DiscreteFileStoreVolatileJson::new, true)
    }

    #[allow(clippy::type_complexity)]
    fn create_non_volatile_scenario(
        with_only_valid_data: bool,
    ) -> Result<
        (
            (
                DiscreteFileStoreNonVolatile<
                    u32,
                    Record<u32, u32>,
                    JsonDiscreteFileSerializerDeserializer<Record<u32, u32>>,
                >,
                impl Iterator<Item = PathBuf>,
            ),
            TempDir,
        ),
        Error,
    > {
        create_files(DiscreteFileStoreNonVolatileJson::new, with_only_valid_data)
    }

    fn create_valid_data_1_in_dir(dir: &TempDir, name: &str) {
        let mut file1 = File::create(dir.path().join(name)).unwrap();
        file1.write_all(VALID_DATA1.as_bytes()).unwrap();
    }

    fn create_with_valid_file() -> TempDir {
        let dir = TempDir::new("test").unwrap();
        create_valid_data_1_in_dir(&dir, "a");
        dir
    }

    fn create_files<F: Fn(PathBuf) -> Result<S, Error>, S>(
        factory: F,
        with_only_valid_data: bool,
    ) -> Result<(S, TempDir), Error> {
        let dir = create_with_valid_file();
        let mut file2 = File::create(dir.path().join("b")).unwrap();

        if with_only_valid_data {
            file2.write_all(VALID_DATA2.as_bytes()).unwrap();
        } else {
            file2.write_all("ajsd;f".as_bytes()).unwrap();
        }

        Ok((factory(dir.path().to_path_buf())?, dir))
    }
}
