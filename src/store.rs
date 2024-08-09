use crate::CacheBrownsResult;
use std::borrow::{Borrow, Cow};

pub mod discrete_files;
pub mod memory;
pub mod replacement;
pub mod tiered;

// TODO: Demonstrate integration with fast external store like: https://crates.io/crates/scc
// TODO: Add link for blog post

/// A [`Store`] is the base layer of a [`crate::managed_cache::ManagedCache`] that handles the
/// storage of data. The actual representation of the underlying data is arbitrary, and higher
/// layers must consider that when working with the underlying data.
///
/// It should *never* be possible for a store to enter a non-recoverable, corrupted state as a
/// result of its internal logic. This doesn't mean its operations are infallible, it just means
/// errors should only be caused by hardware faults, external software, or changes in higher layers
/// such as live patching with schema breaking changes. If a store has a rehydration scenario, it
/// should either:
///
/// 1. Fault when it is constructed, with an optional mechanism provided to force a purge to recover.
/// 2. Perform a best-effort rehydration, where invalid records are dropped and reported to the caller.
///
/// Considerations for instance owners include:
///
/// 1. No thread safety is guaranteed. This can harm performance, so it is optional for implementors.
/// 2. [`Store`] objects can be composed / layered and may have usage tracking (e.g. Replacement algorithms).
/// 3. Failures can occur. You must ensure that consistency is maintained throughout the stack.
/// 4. The store may be volatile or best-effort persistent. If you are maintaining associated state (index, count, etc.) on top of the store you must check for pre-existing data at construction time.
///
/// Implicitly, there exists the notion of 3 kinds of stores:
///
/// 1. `Pure Store` - Isn't managing or consider other stores
/// 2. `Replacement` - Composes another store to provide data management. Why [`Store::peek`] is needed.
/// 3. `Composite Store` - Combines multiple pure stores and/or replacements. Why [`Store::poke`] is needed.
///
/// There isn't a great way to encode these differences into the type system. The [`Store::peek`] and
/// [`Store::poke`] functions aren't needed for pure stores. A pure store can always impl [`Store::peek`] as
/// [`Store::get`] and [`Store::poke`] as a no-op. We want maximum flexibility in composition, so these can't be
/// pulled out into a distinct trait. See this blog post for a detailed explanation.
pub trait Store {
    type Key;
    type Value: Clone;

    type KeyRefIterator<'k>: Iterator<Item = &'k Self::Key>
    where
        Self::Key: 'k,
        Self: 'k;

    // TODO: Why was it ok not to have keys returned with failures? Presumably was an optimization and/or avoid Clone on Key
    // TODO: Should it be a multimap? This is useful for Tiered store but redundant on individual stores
    type FlushResultIterator: Iterator<Item = CacheBrownsResult<Self::Key>>;

    /// Get a copy of the value associated with the key, if it exists. This function must induce any
    /// side effects that are appropriate for the semantics of the implementation. For example, if
    /// the store is an LRU, calls to [`Store::get`] must induce the side effect of updating the usage
    /// order.
    fn get<Q: Borrow<Self::Key>>(&self, key: &Q) -> Option<Cow<Self::Value>>;

    /// Complement of [`Store::get`]. A [`Store::poke`] will cause the side effects of a read to occur without
    /// actually reading the data. Fetching unneeded data is wasteful in general, but in some cases
    /// the arbitrary underlying store may be expensive to fetch data from (e.g. if it requires
    /// deserialization). In these cases, the compiler also can't necessarily optimize this out for
    /// you as it may be unclear of other side effects exist. Thus, the implementor needs to
    /// communicate this themselves. Also makes the intent of the caller clear.
    fn poke<Q: Borrow<Self::Key>>(&self, _key: &Q);

    /// A platform read of a value. When replacement strategies are used (e.g. LRU) reads have side
    /// effects that update internal tracking. If hydration requires inspecting the current state,
    /// these reads will skew tracking. Peek allows you to inspect state without side effects, it
    /// signals to any layer that a platform read has occurred that should be ignored for usage
    /// tracking purposes.
    fn peek<Q: Borrow<Self::Key>>(&self, key: &Q) -> Option<Cow<Self::Value>>;

    /// Insert or update the key-value pair in the [`Store`]. This is infallible because the caller
    /// can't do anything useful based on the underlying error coming from an unknown origin and
    /// failures can be suppressed without impacting correctness. If [`Store::put`] fails, the system
    /// remains in its prior state with stale or missing data. By definition, for the function to
    /// have been called this state was valid and possible to continue from. The value can still be
    /// retrieved, it's just happening with worse performance since the cache operation failed.
    ///
    /// There isn't value in propagating the error up the call stack as [`Store::put`] calls happen as an
    /// implementation detail of other operations.
    ///
    /// Instead, if you wish to track failures either:
    /// 1. Happen as an implementation detail of the [`Store`] itself
    /// 2. The [`Store`] should accept a callback that is invoked when failures occur
    /// 3. Use composition to wrap a component with a fallible put concept where the error is recorded then suppressed to satisfy the [`Store`] trait.
    fn put(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()>;

    /// Update the key-value pair in the [`Store`], if they key is present. This serves 2 purposes:
    /// 1. For correctness in composite caches. A [`crate::hydration::Hydrator`] views a composite cache as a monolith,
    ///    so when it refreshes data the composite needs this context to ensure it updates the data
    ///    only where it is already present, not shifting data between internal stores.
    /// 2. Allows the store to optimize if the internal implementation of contains can provide a hint for the write operation.
    ///
    /// If the key is no longer present when an [`Store::update`] is called, it will no-op and return [`Ok`]
    fn update(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()>;

    // TODO: peeked_update as update without side effects for polling hydrator?

    /// Remove a record from the store. We cannot guarantee that a store is infallible, nor can the
    /// store know whether other layers care about failures or how they would respond. Most of the
    /// time it's best to just suppress or propagate the error when inner logic encounters it.
    ///
    /// Returns the key of the deleted value if it was successfully removed. Returns [`None`] for not
    /// found.
    ///
    /// Retries may not be desirable, the cache needs to be responsive and predictable. If you are
    /// building on top of a layer, do not introduce retries. It is the responsibility of the
    /// underlying layer to ensure all efforts have been made. Doing otherwise would run afoul of
    /// common patterns for error handling, particularly handling failures in cloud architecture.
    /// We still want the same goal, to avoid exponential increases in retry count, be we can't know
    /// if a retry makes sense or what it's impact is when traveling down the stack of layers.
    ///
    /// Considering correctness, the cache doesn't offer the ability to purge individual records in
    /// an invalidation context, only to stop tracking. This means that even if a record should have
    /// been purged, it still being present won't harm correctness. The same logic that generated
    /// the invalidation attempt will fire again. In this sense, there is a "retry" on the next read
    /// (another reason to carefully consider if you really should be retrying in your [`Store`]).
    ///
    /// The fallibility is communicated for two reasons. First, in case an atypical implementation
    /// needs this information. Second, for [`crate::managed_cache::ManagedCache::stop_tracking`], we can't rely on future
    /// reads to ensure that correctness is maintained. While the data won't be invalid, it will be
    /// unclear to the user it's still present and can become a memory leak.
    ///
    /// Does not return `Option<Value>`, because getting the value may be expensive. If you want the
    /// deleted value, use [`Store::take`].
    fn delete<Q: Borrow<Self::Key>>(&mut self, key: &Q) -> CacheBrownsResult<Option<Self::Key>>;

    /// See [`Store::delete`]. Take is [`Store::delete`] that also returns the value. [`Store::get`] can be expensive, but
    /// when we do need the value even if it may be expensive, a [`Store::take`] implementation may be more
    /// optimized than a [`Store::get`] followed by [`Store::delete`].
    /// TODO: Was there a in-crate scenario for this or merely just flexibility offered to users? Maybe multi-tier with promote/demote?
    fn take<Q: Borrow<Self::Key>>(
        &mut self,
        key: &Q,
    ) -> CacheBrownsResult<Option<(Self::Key, Self::Value)>>;

    // TODO: Add integration test covering the delete fails leads to no correctness issues to prove the claim in doc comment above.

    /// Remove all values from the cache. This may partially fail. The store *must* attempt to
    /// delete all elements, it may not early exit after encountering a failure.
    fn flush(&mut self) -> Self::FlushResultIterator;

    /// An iterator of arbitrary order over the keys held in the [`Store`], by reference.
    fn keys(&self) -> Self::KeyRefIterator<'_>;

    /// Checks if the key is in the store. This is a momentary check that may be invalidated; not
    /// thread safe. It is the responsibility of the owning layer to maintain concurrency safety.
    fn contains<Q: Borrow<Self::Key>>(&self, key: &Q) -> bool;
}

#[cfg(test)]
pub mod test_helpers {
    use super::*;
    use mockall::automock;
    use std::vec;

    pub struct Store {}

    #[automock]
    impl Store {
        pub fn get<'a>(&self, _key: &i32) -> Option<Cow<'a, i32>> {
            unimplemented!()
        }

        pub fn peek<'a>(&self, _key: &i32) -> Option<Cow<'a, i32>> {
            unimplemented!()
        }

        pub fn poke(&self, _key: &i32) {
            unimplemented!()
        }

        pub fn put(&mut self, _key: i32, _value: i32) -> CacheBrownsResult<()> {
            unimplemented!()
        }

        pub fn update(&mut self, _key: i32, _value: i32) -> CacheBrownsResult<()> {
            unimplemented!()
        }

        pub fn delete(&mut self, _key: &i32) -> CacheBrownsResult<Option<i32>> {
            unimplemented!()
        }

        pub fn take(&mut self, _key: &i32) -> CacheBrownsResult<Option<(i32, i32)>> {
            unimplemented!()
        }

        pub fn flush(&mut self) -> vec::IntoIter<CacheBrownsResult<i32>> {
            unimplemented!()
        }

        pub fn keys(&self) -> vec::IntoIter<&'static i32> {
            unimplemented!()
        }

        pub fn contains(&self, _key: &i32) -> bool {
            unimplemented!()
        }
    }

    pub struct MockStoreWrapper {
        inner: MockStore,
    }

    impl MockStoreWrapper {
        pub fn new(inner: MockStore) -> Self {
            Self { inner }
        }
    }

    impl super::Store for MockStoreWrapper {
        type Key = i32;
        type Value = i32;

        type KeyRefIterator<'k> = vec::IntoIter<&'k i32>
            where
                <MockStoreWrapper as super::Store>::Key: 'k,
                Self: 'k;

        type FlushResultIterator = vec::IntoIter<CacheBrownsResult<Self::Key>>;

        fn get<Q: Borrow<Self::Key>>(&self, key: &Q) -> Option<Cow<Self::Value>> {
            self.inner.get(key.borrow())
        }

        fn poke<Q: Borrow<Self::Key>>(&self, key: &Q) {
            self.inner.poke(key.borrow());
        }

        fn peek<Q: Borrow<Self::Key>>(&self, key: &Q) -> Option<Cow<Self::Value>> {
            self.inner.peek(key.borrow())
        }

        fn put(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()> {
            self.inner.put(key, value)
        }

        fn update(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()> {
            self.inner.update(key, value)
        }

        fn delete<Q: Borrow<Self::Key>>(
            &mut self,
            key: &Q,
        ) -> CacheBrownsResult<Option<Self::Key>> {
            self.inner.delete(key.borrow())
        }

        fn take<Q: Borrow<Self::Key>>(
            &mut self,
            key: &Q,
        ) -> CacheBrownsResult<Option<(Self::Key, Self::Value)>> {
            self.inner.take(key.borrow())
        }

        fn flush(&mut self) -> Self::FlushResultIterator {
            self.inner.flush()
        }

        fn keys(&self) -> Self::KeyRefIterator<'_> {
            self.inner.keys()
        }

        fn contains<Q: Borrow<Self::Key>>(&self, key: &Q) -> bool {
            self.inner.contains(key.borrow())
        }
    }
}
