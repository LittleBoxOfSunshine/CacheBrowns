use crate::store::CowIterExt;
use crate::store::Store;
use crate::{CacheBrownsResult, CowIterator, NestedCowIterator};
use std::borrow::{Borrow, Cow};
use itertools::Itertools;
use tokio::sync::RwLock;
/*
Tiered works with ripple propagation
So, if you want an LRU 2 tier (for scenario where you really just
want memory cache that can persist between boots) you would do:
Lru<Tiered<Mem,Disk

Whereas if you want a true "tiered cache" like in a CPU, where the
memory cache is smaller than the disk cache you would do:
Tiered<Lru<Mem>,Disk
 */

/// Determines if and how side effects should propagate through the [`Store`] tiers.
#[derive(Clone)]
pub enum RippleMode {
    /// Side effects propagate all the way to the bottom by invoking [`Store::poke`] repeatedly on
    /// all layers that weren't required to satisfy the request for data. This is used when you want
    /// the tiers to consider eviction based on the usage pattern of the whole tiered cache rather
    /// than
    ///
    /// For example, if we have a two tier cache of `Lru<Mem>; Lru<Disk>` and a [`Store::get`] call
    /// was satisfied by the Mem store, but we still want the usage order to be impacted in both
    /// tiers. If you have different tiers of different sizes, you may want the eviction policies
    /// + state to match.
    Unified,
    /// If a higher tier services the request, no side effects will propagate to the unused layers.
    /// This is used when you want each tier to consider data eviction independently based on its
    /// specific usage pattern rather than the usage pattern of the tiered store as a whole.
    ShortCircuit,
}

/// Implements a NINE Multi-level Cache (Neither Inclusive Nor Exclusive). See
/// [Wikipedia - Cache Inclusion Policy](https://en.wikipedia.org/wiki/Cache_inclusion_policy)
/// for a general introduction to this topic.
///
/// While the classification is accurate, the examples are not 1:1 as they discuss CPU caches. With
/// the [`TieredStore`] any store can be added as a tier and `Pure Store` implementations do not
/// have the notion of promote or demote operations.
///
/// Operations are applied to the lowest tier first, then propagate up the stack so long as each
/// tier continues to succeed. This is because [`Store`] operations are fallible, so we must guard
/// against potential rollbacks.
///
/// Imagine we have tiers `T1` and `T2`, both with eviction policies. If a record is updated in
/// `T1` but the update fails in `T2` we have a stale value. This isn't a problem in CPU caches
/// because writes are infallible, but we could have a situation where the eviction policy of `T1`
/// leads the value to be evicted. Now, the next read of the value will be exposed to the prior
/// state still held in `T2` effectively "rolling back" the data.
pub struct TieredStore<Tier, Next> {
    inner: RwLock<Tier>,
    next: RwLock<Next>,
    ripple_mode: RippleMode,
}

impl<K, V, Tier, Next> TieredStore<Tier, Next>
where
    K: Clone,
    V: Clone,
    Tier: Store<Key = K, Value = V>,
    Next: Store<Key = K, Value = V>,
{
    async fn inner_put(
        next: &mut Next,
        inner: &mut Tier,
        key: K,
        value: V,
    ) -> CacheBrownsResult<()> {
        next.put(key.clone(), value.clone()).await?;
        inner.put(key, value).await
    }
}

impl<Tier> TieredStore<(), Tier>
where
    Tier: Store,
{
    pub fn new(ripple_mode: RippleMode, tier: Tier) -> Self {
        Self {
            inner: RwLock::new(()),
            next: RwLock::new(tier),
            ripple_mode,
        }
    }

    pub fn tier<Next>(self, next: Next) -> TieredStore<(), TieredStore<Tier, Next>>
    where
        Next: Store,
    {
        TieredStore {
            inner: self.inner,
            next: RwLock::new(TieredStore {
                inner: self.next,
                next: RwLock::new(next),
                ripple_mode: self.ripple_mode.clone(),
            }),
            ripple_mode: self.ripple_mode,
        }
    }
}

impl<K, Tier, Next> Store for TieredStore<Tier, Next>
where
    K: Clone + Send + Sync,
    Tier: Store<Key = K, Value = Next::Value> + Sync,
    Next: Store<Key = K> + Sync,
{
    type Key = Tier::Key;
    type Value = Tier::Value;
    type KeyRefIterator<'k>
        = std::vec::IntoIter<Cow<'k, Self::Key>>
        // = NestedCowIterator!('k, <Tier as Store>::Key, Next::KeyRefIterator<'k>)
    where
        Self::Key: 'k,
        Self: 'k;
    type FlushResultIterator = std::iter::Chain<
        <Next as Store>::FlushResultIterator,
        <Tier as Store>::FlushResultIterator,
    >;

    async fn get<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) -> Option<Self::Value> {
        let lock = self.inner.read().await;
        let current_tier_value = lock.get(key).await;

        // Walk down tiers until value is found.
        match current_tier_value {
            Some(value) => {
                // Tiers closer to top of stack are faster, so we want to exit early, but we need to
                // apply the appropriate ripple mode effects first.
                match self.ripple_mode {
                    RippleMode::Unified => {
                        self.next.read().await.poke(key).await;
                        Some(value)
                    }
                    RippleMode::ShortCircuit => Some(value),
                }
            }
            None => {
                match self.inner.read().await.get(key).await {
                    None => None,
                    Some(value) => {
                        // Best effort, there is no correctness issue we're just attempting to use
                        // the higher cache tier (i.e. self) for future reads.
                        let _ = self
                            .inner
                            .write()
                            .await
                            .put(key.borrow().clone(), value.clone());
                        Some(value)
                    }
                }
            }
        }
    }

    async fn poke<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) {
        self.next.read().await.poke(key).await;
        self.inner.read().await.poke(key).await;
    }

    async fn peek<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) -> Option<Self::Value> {
        // We want to exit on the cheapest value, as it will match all the way down.
        // No side effects exist to consider, so no propagation is needed.
        self.inner
            .read()
            .await
            .peek(key)
            .await
            .or(self.next.read().await.peek(key).await)
    }

    async fn put(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()> {
        Self::inner_put(
            &mut *self.next.write().await,
            &mut *self.inner.write().await,
            key,
            value,
        )
        .await
    }

    async fn update(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()> {
        self.next
            .write()
            .await
            .update(key.clone(), value.clone())
            .await?;
        self.inner.write().await.update(key, value).await
    }

    async fn delete<Q: Borrow<Self::Key> + Sync>(
        &mut self,
        key: &Q,
    ) -> CacheBrownsResult<Option<Self::Key>> {
        self.next.write().await.delete(key).await?;
        self.inner.write().await.delete(key).await
    }

    async fn take<Q: Borrow<Self::Key> + Sync>(
        &mut self,
        key: &Q,
    ) -> CacheBrownsResult<Option<(Self::Key, Self::Value)>> {
        // Delete needs to succeed or fail at base to avoid rollbacks
        match self.next.write().await.take(key).await? {
            // No failure, but not present. May be present here, so try take.
            None => self.inner.write().await.take(key).await,
            // Attempt to cascade up the stack, we already have value so use cheaper delete
            Some(lower_value) => {
                match self.inner.write().await.delete(key).await {
                    Ok(_) => Ok(Some(lower_value)),
                    // We have a value, but we need to ensure higher tiers don't attempt deletes to
                    // prevent rollbacks
                    // TODO: Consider having the option always returned so it could still propagate the value?
                    Err(e) => Err(e),
                }
            }
        }
    }

    async fn flush(&mut self) -> Self::FlushResultIterator {
        let low_tiers = self.next.write().await.flush().await;
        let flushed_here = self.inner.write().await.flush().await;
        low_tiers.chain(flushed_here)
    }

    async fn keys(&self) -> Self::KeyRefIterator<'_> {
        // From an external perspective, store is a monolith. Propagate to lowest (biggest) tier.
        let lock = self.next.read().await;
        let keys = lock.keys().await;

        let mut derp = Vec::new();
        for key in keys {
            let cow: Cow<'_, Self::Key> = Cow::Owned(key.into_owned());
            derp.push(cow)
        }

        // keys.into_as_owned()
        //let keys = keys.map(|v| Cow::Owned(v.into_owned()));
        // keys
        //let keys: Vec<Cow<'_, Self::Key>> = keys.collect_vec();
        derp.into_iter()
        // keys.into_as_owned().collect_vec().into_iter()
        //map(|v| v.as_owned())
    }

    async fn contains<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) -> bool {
        // From an external perspective, store is a monolith. Propagate to lowest (biggest) tier.
        self.next.read().await.contains(key).await
    }
}

impl<Tier> Store for TieredStore<(), Tier>
where
    Tier: Store + Sync,
{
    type Key = Tier::Key;
    type Value = Tier::Value;
    type KeyRefIterator<'k>
        = NestedCowIterator!('k, <Tier as Store>::Key, std::slice::Iter<'k, &'k <Tier as Store>::Key>)
    where
        <Tier as Store>::Key: 'k,
        Self: 'k;
    type FlushResultIterator = Tier::FlushResultIterator;

    async fn get<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) -> Option<Self::Value> {
        let lock = self.next.read().await;
        lock.get(key).await.to_owned()
    }

    async fn poke<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) {
        unsafe { (*self.next.read().await).poke(key).await }
    }

    async fn peek<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) -> Option<Self::Value> {
        unsafe { (*self.next.read().await).peek(key).await }
    }

    async fn put(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()> {
        self.next.write().await.put(key, value).await
    }

    async fn update(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()> {
        self.next.write().await.update(key, value).await
    }

    async fn delete<Q: Borrow<Self::Key> + Sync>(
        &mut self,
        key: &Q,
    ) -> CacheBrownsResult<Option<Self::Key>> {
        self.next.write().await.delete(key).await
    }

    async fn take<Q: Borrow<Self::Key> + Sync>(
        &mut self,
        key: &Q,
    ) -> CacheBrownsResult<Option<(Self::Key, Self::Value)>> {
        self.next.write().await.take(key).await
    }

    async fn flush(&mut self) -> Self::FlushResultIterator {
        self.next.write().await.flush().await
    }

    async fn keys(&self) -> Self::KeyRefIterator<'_> {
        let next = self.next.read().await;
        let keys = next.keys().await;
        keys.collect::<Vec<&'_ Self::Key>>().iter()
    }

    async fn contains<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) -> bool {
        self.next.read().await.contains(key).await
    }
}

// TODO: Consider converting from struct to enum

#[cfg(test)]
mod tests {
    use crate::store::memory::MemoryStore;
    use crate::store::tiered::{RippleMode, TieredStore};
    use crate::store::Store;

    #[tokio::test]
    async fn happy_path() {
        let mut store =
            TieredStore::new(RippleMode::ShortCircuit, MemoryStore::new()).tier(MemoryStore::new());

        assert!(store.put(0, 0).await.is_ok());
        assert_eq!(0, store.get(&0).await.unwrap())
    }

    #[tokio::test]
    async fn unified_cache_side_effects_propagate() {}

    #[tokio::test]
    async fn short_circuit_cache_side_effects_do_not_propagate() {}

    #[tokio::test]
    async fn get_from_lower_tier() {}

    #[tokio::test]
    async fn get_from_lower_tier_repeats_due_to_upper_tier_insert_failure() {}

    #[tokio::test]
    async fn poke_always_propagates() {}

    #[tokio::test]
    async fn flush_with_mixed_failures() {}

    #[tokio::test]
    async fn flush_happy_path() {}

    #[tokio::test]
    async fn update_cascades() {}

    #[tokio::test]
    async fn update_lower_tier_fails() {}

    #[tokio::test]
    async fn update_upper_tier_fails_then_evicts_no_rollbacks_occur() {}

    #[tokio::test]
    async fn take_propagates_delete() {}

    #[tokio::test]
    async fn keys_merges_all_layers() {}

    #[tokio::test]
    async fn contains_cascades() {}

    #[tokio::test]
    async fn contains_early_exit() {}
}
