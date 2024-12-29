pub mod concurrent;
pub mod http;
pub mod pub_sub;

use std::{borrow::Borrow, future::Future};

/// The [`SourceOfRecord`] represents the real location of the data and the semantics of when it is
/// considered valid or invalid. It is the "source of truth" for the cache to fetch accurate state
/// from.
///
/// It is the key trait for decoupling the application logic from the general purpose strategy
/// implementations. Implementations can be composed to reuse and customize common implementations.

#[trait_variant::make(Send + Sync)]
//#[cfg_attr(test, mockall::automock(type Key=u32; type Value=u32;))]
pub trait SourceOfRecord {
    type Key: Clone + Send + Sync;
    type Value: Clone + Send + Sync;

    /// Fetch the data from the source of record. This may or may not be a remote call.
    async fn retrieve<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) -> Option<Self::Value>;

    /// Accepts current value if one exists in case it can be used for optimized load
    /// ex. If building an HTTP cache, and you receive a 304 response, replay the current value
    async fn retrieve_with_hint<Q: Borrow<Self::Key> + Sync, V: Borrow<Self::Value> + Sync>(
        &self,
        key: &Q,
        _current_value: &V,
    ) -> Option<Self::Value> {
        // Provide pass-through implementation for implementations with no current-value based optimization.
        async { self.retrieve(key).await }
    }

    /// Batched version of [`SourceOfRecord::retrieve`]. Retrieves records in as few batches as
    /// possible. Some sources can fetch many records at once, informing the source up front that
    /// multiple records are required allows for potential optimization.
    // async fn batch_retrieve<'a, I>(&'a self, keys: I) -> impl Iterator<Item = impl Future<Output = impl Iterator<Item = (Self::Key, Option<Self::Value>)>> + 'a>
    async fn batch_retrieve<I>(
        &self,
        keys: I,
    ) -> impl Iterator<Item = impl Future<Output = impl Iterator<Item = (Self::Key, Option<Self::Value>)>>>
    where
        I: Iterator<Item = Self::Key> + Send + Sync,
        Self: Sized,
    {
        let self_ref = self;
        async move {
            keys.map(move |k| async move {
                let value = self_ref.retrieve(&k).await;
                let vec = vec![(k, value)];
                vec.into_iter()
            })
        }
    }

    /// Batched version of [`SourceOfRecord::retrieve_with_hint`]. Retrieves records in as few batches as
    /// possible. Some sources can fetch many records at once, informing the source up front that
    /// multiple records are required allows for potential optimization.
    async fn batch_retrieve_with_hint<I>(
        &self,
        keys_with_hints: I,
    ) -> impl Iterator<Item = impl Future<Output = impl Iterator<Item = (Self::Key, Option<Self::Value>)>>>
    where
        I: Iterator<Item = (Self::Key, Self::Value)> + Send + Sync,
        Self: Sized,
    {
        let self_ref = self;
        async move {
            keys_with_hints.map(move |(k, v)| async move {
                let value = self_ref.retrieve_with_hint(&k, &v).await;
                let vec = vec![(k, value)];
                vec.into_iter()
            })
        }
    }

    /// Determines data is valid and fresh. This is a function of the [`SourceOfRecord`] because
    /// only the creator of the data knows how to evaluate it. Other aspects of the cache only need
    /// the question answered, they don't need to know how it's answered.
    ///
    /// For more complex cases like HTTP, this relationship is more clear. HTTP cached data could
    /// be valid off an ETAG or a timestamp. This concept is obviously a concern of the HTTP section
    /// of the code, and therefor of the [`SourceOfRecord`] rather than being a concern of other
    /// traits.
    async fn is_valid(&self, key: &Self::Key, value: &Self::Value) -> bool;
}

#[cfg(test)]
pub mod test_helpers {
    use super::*;
    use mockall::automock;

    pub struct Sor {}

    #[automock]
    impl Sor {
        pub fn retrieve(&self, _key: &i32) -> Option<i32> {
            unimplemented!()
        }

        pub fn retrieve_with_hint(&self, _key: &i32, _current_value: &i32) -> Option<i32> {
            unimplemented!()
        }

        pub fn is_valid(&self, _key: &i32, _value: &i32) -> bool {
            unimplemented!()
        }
    }

    pub struct MockSorWrapper {
        inner: MockSor,
    }

    impl MockSorWrapper {
        pub fn new(inner: MockSor) -> Self {
            Self { inner }
        }
    }

    impl SourceOfRecord for MockSorWrapper {
        type Key = i32;
        type Value = i32;

        async fn retrieve<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) -> Option<Self::Value> {
            self.inner.retrieve(key.borrow())
        }

        async fn retrieve_with_hint<Q: Borrow<Self::Key> + Sync, V: Borrow<Self::Value> + Sync>(
            &self,
            key: &Q,
            value: &V,
        ) -> Option<Self::Value> {
            self.inner.retrieve_with_hint(key.borrow(), value.borrow())
        }

        async fn is_valid(&self, key: &Self::Key, value: &Self::Value) -> bool {
            self.inner.is_valid(key, value)
        }
    }
}
