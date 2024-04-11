pub mod concurrent;
pub mod http;
pub mod pub_sub;

use std::borrow::Borrow;

/// The [`SourceOfRecord`] represents the real location of the data and the semantics of when it is
/// considered valid or invalid. It is the "source of truth" for the cache to fetch accurate state
/// from.
///
/// It is the key trait for decoupling the application logic from the general purpose strategy
/// implementations. Implementations can be composed to reuse and customize common implementations.
pub trait SourceOfRecord {
    type Key;
    type Value;

    /// Fetch the data from the source of record. This may or may not be a remote call.
    fn retrieve<Q: Borrow<Self::Key>>(&self, key: &Q) -> Option<Self::Value>;

    /// Accepts current value if one exists in case it can be used for optimized load
    /// ex. If building an HTTP cache, and you receive a 304 response, replay the current value
    fn retrieve_with_hint<Q: Borrow<Self::Key>, V: Borrow<Self::Value>>(
        &self,
        key: &Q,
        _current_value: &V,
    ) -> Option<Self::Value> {
        // Provide pass-through implementation for implementations with no current-value based optimization.
        self.retrieve(key)
    }

    // TODO: Consider how to introduce Hint trait or use AsRef for supporting lightweight hints

    // /// Batched version of [`SourceOfRecord::retrieve`]. Retrieves records in as few batches as
    // /// possible. Some sources can fetch many records at once, informing the source up front that
    // /// multiple records are required allows for potential optimization.
    // fn batch_retrieve<I>(&self, keys: I) -> impl Iterator<Item = (Self::Key, Option<Self::Value>)>
    // where
    //     I: Iterator<Item = Self::Key>,
    //     Self: Sized,
    // {
    //     keys.map(|k| {
    //         let v =  self.retrieve(&k);
    //         (k, v)
    //     }).into_iter()
    // }
    //
    // /// Batched version of [`SourceOfRecord::retrieve_with_hint`]. Retrieves records in as few batches as
    // /// possible. Some sources can fetch many records at once, informing the source up front that
    // /// multiple records are required allows for potential optimization.
    // fn batch_retrieve_with_hint<I>(
    //     &self,
    //     keys_with_hints: I,
    // ) -> impl Iterator<Item = impl Iterator<Item = (Self::Key, Option<Self::Value>)>>
    // where
    //     I: Iterator<Item = (Self::Key, Self::Value)>,
    //     Self: Sized,
    // {
    //     keys_with_hints.map(|(k, v)| {
    //         let v = self.retrieve_with_hint(&k, &v);
    //         vec![(k, v)].into_iter()
    //     })
    //         .into_iter()
    // }

    /// Determines data is valid and fresh. This is a function of the [`SourceOfRecord`] because
    /// only the creator of the data knows how to evaluate it. Other aspects of the cache only need
    /// the question answered, they don't need to know how it's answered.
    ///
    /// For more complex cases like HTTP, this relationship is more clear. HTTP cached data could
    /// be valid off an ETAG or a timestamp. This concept is obviously a concern of the HTTP section
    /// of the code, and therefor of the [`SourceOfRecord`] rather than being a concern of other
    /// traits.
    fn is_valid(&self, key: &Self::Key, value: &Self::Value) -> bool;
}

/// Implements an HTTP [`SourceOfRecord`] that respects cache control headers.
pub struct HttpDataSourceOfRecord {}

// TODO: Implement the HTTP standard, decide how best to abstract this out independent of a particular client library
// impl HttpDataSourceOfRecord {}
//
// impl<Key, Value> SourceOfRecord<Key, Value> for HttpDataSourceOfRecord {
//     fn retrieve(&self, _key: &Key) -> Option<Value> {
//         todo!()
//     }
//
//     fn retrieve_with_hint(&self, _key: &Key, _current_value: &Value) -> Option<Value> {
//         todo!()
//     }
//
//     fn is_valid(&self, _key: &Key, _value: &Value) -> bool {
//         todo!()
//     }
// }
