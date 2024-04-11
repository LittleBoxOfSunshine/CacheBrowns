use std::borrow::{Borrow, Cow};
use std::collections::hash_map::Drain;
use std::collections::{hash_map, HashMap};
use std::fs::File;
use std::hash::Hash;
use std::io::{BufReader, BufWriter, Error};
use std::path::{Path, PathBuf};
use std::{fs, vec};

use itertools::Itertools;
use serde::{Deserialize, Serialize};
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
    Key: Clone + Eq + Hash + Serialize + for<'a> Deserialize<'a>,
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
    Key: Clone + Eq + Hash + Serialize + for<'a> Deserialize<'a>,
    Value: Clone + Serialize + for<'a> Deserialize<'a>,
    Serde: DiscreteFileSerializerDeserializer<Record<Key, Value>>,
{
    pub fn new(cache_directory: PathBuf) -> Result<(Self, impl Iterator<Item = PathBuf>), Error> {
        let mut store = Self {
            cache_directory,
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
                    store.index.insert(record.key, path);
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
    Key: Clone + Eq + Hash + Serialize + for<'a> Deserialize<'a>,
    Value: Clone + Serialize + for<'a> Deserialize<'a>,
    Serde: DiscreteFileSerializerDeserializer<Record<Key, Value>>,
{
    type Key = Key;
    type Value = Value;
    type KeyRefIterator<'k> = hash_map::Keys<'k, Key, PathBuf> where Key: 'k, Value: 'k, Serde: 'k;
    type FlushResultIterator = vec::IntoIter<CacheBrownsResult<Option<Key>>>;

    fn get<Q: Borrow<Key>>(&self, key: &Q) -> Option<Cow<Value>> {
        self.peek(key)
    }

    fn peek<Q: Borrow<Key>>(&self, key: &Q) -> Option<Cow<Value>> {
        if let Some(path) = self.index.get(key.borrow()) {
            return Some(Cow::Owned(get::<Serde, Record<Key, Value>>(path)?.value));
        }

        None
    }

    fn put(&mut self, key: Key, value: Value) {
        let path = get_or_create_index_entry(&self.cache_directory, &mut self.index, key.clone());
        put::<Serde, Record<Key, Value>>(path, Record { key, value })
    }

    fn delete<Q: Borrow<Self::Key>>(&mut self, key: &Q) -> CacheBrownsResult<Option<Key>> {
        delete::<Key>(&mut self.index, key.borrow())
    }

    fn flush(&mut self) -> Self::FlushResultIterator {
        flush::<Key>(self.index.drain())
    }

    //noinspection DuplicatedCode
    fn keys(&self) -> Self::KeyRefIterator<'_> {
        self.index.keys()
    }

    fn contains<Q: Borrow<Self::Key>>(&self, key: &Q) -> bool {
        self.index.contains_key(key.borrow())
    }
}

/// Stores each element in a unique file, serialized. A single directory is used to represent the
/// store. No other files are permitted to be co-located in the directory. Violating this requirement
/// post-startup may lead to undefined behavior. The volatile variant attempts to clear the target
/// directory when it is created, which is a fallible operation.
///
/// [`DiscreteFileStoreVolatile`] will purge files when it is dropped. The purge during construction
/// is done handle orphaned data from a previous unexpected exit.
///
/// An index of key to path mappings is held in memory.
///
/// This variant is generally preferred over [`DiscreteFileStoreNonVolatile`] as it is easier to
/// work with and safer during executable lifecycle events, upgrades, and downgrades. By definition,
/// it's safe to purge a cache (it's not the source of truth).
pub struct DiscreteFileStoreVolatile<Key, Value, Serde>
where
    Key: Clone + Eq + Hash + Serialize + for<'a> Deserialize<'a>,
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
    Key: Clone + Eq + Hash + Serialize + for<'a> Deserialize<'a>,
    Value: Serialize + for<'a> Deserialize<'a>,
    Serde: DiscreteFileSerializerDeserializer<Value>,
{
    /// When created, the [`DiscreteFileStoreVolatile`] will check the target directory to ensure it
    /// exists and is empty. If the target exists and has other files, it will attempt to purge them.
    /// If the files cannot be deleted, construction fails to protect the validity of the cache.
    /// If the target directory does not exist, it will be automatically created.
    pub fn new(cache_directory: PathBuf) -> Result<Self, Error> {
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

impl<Key, Value, Serde> Store for DiscreteFileStoreVolatile<Key, Value, Serde>
where
    Key: Clone + Eq + Hash + Serialize + for<'a> Deserialize<'a>,
    Value: Clone + Serialize + for<'a> Deserialize<'a>,
    Serde: DiscreteFileSerializerDeserializer<Value>,
{
    type Key = Key;
    type Value = Value;
    type KeyRefIterator<'k> = hash_map::Keys<'k, Key, PathBuf> where Key: 'k, Value: 'k, Serde: 'k;
    type FlushResultIterator = vec::IntoIter<CacheBrownsResult<Option<Key>>>;

    fn get<Q: Borrow<Key>>(&self, key: &Q) -> Option<Cow<Value>> {
        self.peek(key)
    }

    fn peek<Q: Borrow<Key>>(&self, key: &Q) -> Option<Cow<Value>> {
        if let Some(path) = self.index.get(key.borrow()) {
            return get::<Serde, Value>(path).map(|v| Cow::Owned(v));
        }

        None
    }

    fn put(&mut self, key: Key, value: Value) {
        let path = get_or_create_index_entry(&self.cache_directory, &mut self.index, key);
        put::<Serde, Value>(path, value)
    }

    fn delete<Q: Borrow<Self::Key>>(&mut self, key: &Q) -> CacheBrownsResult<Option<Key>> {
        delete::<Key>(&mut self.index, key.borrow())
    }

    fn flush(&mut self) -> Self::FlushResultIterator {
        flush::<Key>(self.index.drain())
    }

    fn keys(&self) -> Self::KeyRefIterator<'_> {
        self.index.keys()
    }

    fn contains<Q: Borrow<Self::Key>>(&self, key: &Q) -> bool {
        self.index.contains_key(key.borrow())
    }
}

// Shared implementation of get
fn get<Serde, Value>(path: &PathBuf) -> Option<Value>
where
    Value: Clone + Serialize + for<'a> Deserialize<'a>,
    Serde: DiscreteFileSerializerDeserializer<Value>,
{
    if let Ok(file) = File::open(path) {
        return Serde::deserialize(BufReader::new(file));
    }

    None
}

// Shared implementation of put
fn put<Serde, Value>(path: PathBuf, value: Value)
where
    Value: Serialize + for<'a> Deserialize<'a>,
    Serde: DiscreteFileSerializerDeserializer<Value>,
{
    if let Ok(file) = File::create(path) {
        Serde::serialize(BufWriter::new(file), &value)
    }
}

// Shared implementation of delete
fn delete<Key>(index: &mut HashMap<Key, PathBuf>, key: &Key) -> CacheBrownsResult<Option<Key>>
where
    Key: Eq,
    Key: Hash,
{
    if let Some((key, path)) = index.remove_entry(key) {
        fs::remove_file(path).map_err(Box::new)?;
        return Ok(Some(key));
    }

    Ok(None)
}

// Shared implementation of flush
fn flush<Key>(records: Drain<'_, Key, PathBuf>) -> vec::IntoIter<CacheBrownsResult<Option<Key>>> {
    records
        .into_iter()
        .map(|(k, path)| fs::remove_file(path).map(|_| Some(k)).map_err(|e| e.into()))
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
    Key: Eq,
    Key: Hash,
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
    fn serialize(buffered_writer: BufWriter<File>, value: &Value);

    fn deserialize(buffered_reader: BufReader<File>) -> Option<Value>;
}

// TODO: dyn Error needs to propagate through maybe? Or at least a logging call back?
// This process will deterimine the outcome of the old todo off is this too laborious since the
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
    fn serialize(buffered_writer: BufWriter<File>, value: &Value) {
        let _ = serde_json::to_writer(buffered_writer, &value);
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
    fn serialize(buffered_writer: BufWriter<File>, value: &Value) {
        let _ = bincode::serialize_into(buffered_writer, &value);
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
