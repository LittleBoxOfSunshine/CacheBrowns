use crate::CacheBrownsResult;
use std::borrow::Borrow;

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
/// [`Store::get`] and [`Store::poke`] as a no-op. We want maximum flexibility in composition, so these cant be
/// pulled out into a distinct trait. See this blog post for a detailed explanation.
///
/// ## Replacement and Thread Safety
///
/// Replacement layers pose a trade-off for the design. The distinction between a Replacement and
/// a Pure Store is that [`get`] potentially has side effects, meaning interior mutability.
///
/// ### All functions mutable
/// If we make [`get`] mutable, we preclude hyrdators from making optimizations on internal locking. We
/// can't avoid that by continuing to bubble up thread safety responsibility either because it leaks
/// aspects of the behaviors we are trying to abstract away in the first place and because it
/// precludes yet more optimization.
///
/// ### A `Replacement` trait
/// Making distinct traits doesn't work well, because there's no place in code where we'd ever want
/// to restrict users from injecting Replacement strategies. If we require it everywhere, it's just
/// alternative semantics. An identity Replacement struct would have to be used to wrap Basic Stores
/// so that hyrdators, all expecting Replacements, can accept them.
///
/// For this to pay off, [`Hydrator`] must have a Replacement and Pure Store variant of its
/// implementation. In this way, it can use optimized behavior where available and take
/// responsibility for thread safety instead when it can't.
///
/// This is certainly an option. The pros are that it allows you to always pick the best
/// implementation based on the type you're actually given. The cons are adding more implementations
/// and additional user complexity. They need to be aware of the different variants and there's
/// additional boilerplate for them to contend with.
///
/// ### Interior Mutability
/// With interior mutability, the [`get`] call itself can continue to be read only, with the write
/// aspects handled internally. This is very attractive, because we're not leaking the side effects
/// into the trait. Additionally, this can be done quite performantly, especially if we are willing
/// to accept relaxed ordering for very closely timed events. By leveraging queues internally, at
/// most the queue operation has an interior critical section. The [`Lru`] implementation leverages
/// this approach.
///
/// In the prototype build we've elected for this option. It's the cleanest, most flexible, and the
/// least code. These are all important qualities for rapid iteration and time to market. Given this
/// project is experimental, we believe real world usage is the best way to figure out what's best.
/// Additionally, if it turns out the dedicated Replacement trait approach, or another, ends up
/// being the case having a substantial amount of real world usage would justify the additional
/// investment required.
///
/// In the end, the overhead here is extremely small, especially in the async context. This isn't
/// going to make or break the implementation in either direction.
#[trait_variant::make(Send)]
pub trait Store {
    type Key: Clone + Send + Sync;
    type Value: Clone + Send + Sync;

    type KeyIterator: Iterator<Item = Self::Key>;

    // TODO: Why was it ok not to have keys returned with failures? Presumably was an optimization and/or avoid Clone on Key
    // TODO: Should it be a multimap? This is useful for Tiered store but redundant on individual stores
    type FlushResultIterator: Iterator<Item = CacheBrownsResult<Self::Key>> + Send;

    /// Get a copy of the value associated with the key, if it exists. This function must induce any
    /// side effects that are appropriate for the semantics of the implementation. For example, if
    /// the store is an LRU, calls to [`Store::get`] must induce the side effect of updating the usage
    /// order.
    async fn get<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) -> Option<Self::Value>;
    // TODO: Consider result<option>

    /// Complement of [`Store::get`]. A [`Store::poke`] will cause the side effects of a read to occur without
    /// actually reading the data. Fetching unneeded data is wasteful in general, but in some cases
    /// the arbitrary underlying store may be expensive to fetch data from (e.g. if it requires
    /// deserialization). In these cases, the compiler also can't necessarily optimize this out for
    /// you as it may be unclear of other side effects exist. Thus, the implementor needs to
    /// communicate this themselves. Also makes the intent of the caller clear.
    async fn poke<Q: Borrow<Self::Key> + Sync>(&self, _key: &Q);

    /// A platform read of a value. When replacement strategies are used (e.g. LRU) reads have side
    /// effects that update internal tracking. If hydration requires inspecting the current state,
    /// these reads will skew tracking. Peek allows you to inspect state without side effects, it
    /// signals to any layer that a platform read has occurred that should be ignored for usage
    /// tracking purposes.
    async fn peek<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) -> Option<Self::Value>;

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
    async fn put(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()>;

    /// Update the key-value pair in the [`Store`], if they key is present. This serves 2 purposes:
    /// 1. For correctness in composite caches. A [`crate::hydration::Hydrator`] views a composite cache as a monolith,
    ///    so when it refreshes data the composite needs this context to ensure it updates the data
    ///    only where it is already present, not shifting data between internal stores.
    /// 2. Allows the store to optimize if the internal implementation of contains can provide a hint for the write operation.
    ///
    /// If the key is no longer present when an [`Store::update`] is called, it will no-op and return [`Ok`]
    async fn update(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()>;

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
    async fn delete<Q: Borrow<Self::Key> + Sync>(
        &mut self,
        key: &Q,
    ) -> CacheBrownsResult<Option<Self::Key>>;

    /// See [`Store::delete`]. Take is [`Store::delete`] that also returns the value. [`Store::get`] can be expensive, but
    /// when we do need the value even if it may be expensive, a [`Store::take`] implementation may be more
    /// optimized than a [`Store::get`] followed by [`Store::delete`].
    /// TODO: Was there a in-crate scenario for this or merely just flexibility offered to users? Maybe multi-tier with promote/demote?
    async fn take<Q: Borrow<Self::Key> + Sync>(
        &mut self,
        key: &Q,
    ) -> CacheBrownsResult<Option<(Self::Key, Self::Value)>>;

    // TODO: Add integration test covering the delete fails leads to no correctness issues to prove the claim in doc comment above.

    /// Remove all values from the cache. This may partially fail. The store *must* attempt to
    /// delete all elements, it may not early exit after encountering a failure.
    async fn flush(&mut self) -> Self::FlushResultIterator;

    /// An iterator of arbitrary order over the keys held in the [`Store`], by reference.
    async fn keys(&self) -> Self::KeyIterator;

    /// Checks if the key is in the store. This is a momentary check that may be invalidated; not
    /// thread safe. It is the responsibility of the owning layer to maintain concurrency safety.
    async fn contains<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) -> bool;
}

#[cfg(test)]
pub mod test_helpers {
    use super::*;
    use mockall::automock;

    use itertools::Itertools;
    use std::vec;

    pub struct Store {}

    #[automock]
    impl Store {
        pub async fn get(&self, _key: &i32) -> Option<i32> {
            unimplemented!()
        }

        pub fn peek(&self, _key: &i32) -> Option<i32> {
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

        type KeyIterator = vec::IntoIter<i32>;

        type FlushResultIterator = vec::IntoIter<CacheBrownsResult<Self::Key>>;

        async fn get<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) -> Option<Self::Value> {
            self.inner.get(key.borrow()).await
        }

        async fn poke<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) {
            self.inner.poke(key.borrow());
        }

        async fn peek<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) -> Option<Self::Value> {
            self.inner.peek(key.borrow())
        }

        async fn put(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()> {
            self.inner.put(key, value)
        }

        async fn update(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()> {
            self.inner.update(key, value)
        }

        async fn delete<Q: Borrow<Self::Key> + Sync>(
            &mut self,
            key: &Q,
        ) -> CacheBrownsResult<Option<Self::Key>> {
            self.inner.delete(key.borrow())
        }

        async fn take<Q: Borrow<Self::Key> + Sync>(
            &mut self,
            key: &Q,
        ) -> CacheBrownsResult<Option<(Self::Key, Self::Value)>> {
            self.inner.take(key.borrow())
        }

        async fn flush(&mut self) -> Self::FlushResultIterator {
            self.inner.flush()
        }

        async fn keys(&self) -> Self::KeyIterator {
            self.inner.keys().cloned().collect_vec().into_iter()
        }

        async fn contains<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) -> bool {
            self.inner.contains(key.borrow())
        }
    }
}
