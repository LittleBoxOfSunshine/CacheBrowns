use crate::store::Store;
use crate::CacheBrownsResult;
use std::borrow::{Borrow, Cow};

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
    /// For example, if we have a two tier cache of Lru<Mem>; Lru<Disk> and a [`Store::get`] call
    /// was satisfied by the Mem store, but we still want the usage order to be impacted in both
    /// tiers. If you have different tiers of different sizes, you may want the eviction policies
    /// + state to match.
    Unified,
    /// If a higher tier services the request, no side effects will propagate to the unused layers.
    /// This is used when you want each tier to consider data eviction independently based on its
    /// specific usage pattern rather than the usage pattern of the tiered store as a whole.
    ShortCircuit,
}

pub struct TieredStore<Tier, Next> {
    inner: Tier,
    next: Next,
    ripple_mode: RippleMode,
}

impl<Tier> TieredStore<(), Tier>
where
    Tier: Store,
{
    pub fn new(ripple_mode: RippleMode, tier: Tier) -> Self {
        Self {
            inner: (),
            next: tier,
            ripple_mode,
        }
    }

    pub fn tier<Next>(self, next: Next) -> TieredStore<(), TieredStore<Tier, Next>>
    where
        Next: Store,
    {
        TieredStore {
            inner: self.inner,
            next: TieredStore {
                inner: self.next,
                next,
                ripple_mode: self.ripple_mode.clone(),
            },
            ripple_mode: self.ripple_mode,
        }
    }
}

impl<Tier> Store for TieredStore<(), Tier>
where
    Tier: Store,
{
    type Key = Tier::Key;
    type Value = Tier::Value;
    type KeyRefIterator<'k> = Tier::KeyRefIterator<'k> where <Tier as Store>::Key: 'k, Self: 'k;
    type FlushResultIterator = Tier::FlushResultIterator;

    fn get<Q: Borrow<Self::Key>>(&self, key: &Q) -> Option<Cow<Self::Value>> {
        self.next.get(key)
    }

    fn poke<Q: Borrow<Self::Key>>(&self, key: &Q) {
        self.next.poke(key)
    }

    fn peek<Q: Borrow<Self::Key>>(&self, key: &Q) -> Option<Cow<Self::Value>> {
        self.next.peek(key)
    }

    fn put(&mut self, key: Self::Key, value: Self::Value) {
        self.next.put(key, value)
    }

    fn update(&mut self, key: Self::Key, value: Self::Value) {
        self.next.update(key, value)
    }

    fn delete<Q: Borrow<Self::Key>>(&mut self, key: &Q) -> CacheBrownsResult<Option<Self::Key>> {
        self.next.delete(key)
    }

    fn take<Q: Borrow<Self::Key>>(&mut self, key: &Q) -> CacheBrownsResult<Option<(Self::Key, Cow<Self::Value>)>> {
        self.next.take(key)
    }

    fn flush(&mut self) -> Self::FlushResultIterator {
        self.next.flush()
    }

    fn keys(&self) -> Self::KeyRefIterator<'_> {
        self.next.keys()
    }

    fn contains<Q: Borrow<Self::Key>>(&self, key: &Q) -> bool {
        self.next.contains(key)
    }
}

impl<K, Tier, Next> Store for TieredStore<Tier, Next>
where
    K: Clone,
    Tier: Store<Key = K, Value = Next::Value>,
    Next: Store<Key = K>,
{
    type Key = Tier::Key;
    type Value = Tier::Value;
    type KeyRefIterator<'k> = Next::KeyRefIterator<'k> where Self::Key: 'k, Self: 'k;
    type FlushResultIterator = Next::FlushResultIterator;

    fn get<Q: Borrow<Self::Key>>(&self, key: &Q) -> Option<Cow<Self::Value>> {
        // Walk down tiers until value is found.
        match self.inner.get(key) {
            Some(value) => {
                // Tiers closer to top of stack are faster, so we want to exit early, but we need to
                // apply the appropriate ripple mode effects first.
                match self.ripple_mode {
                    RippleMode::Unified => {
                        self.next.poke(key);
                        Some(value)
                    }
                    RippleMode::ShortCircuit => Some(value),
                }
            }
            None => self.next.get(key),
        }
    }

    fn poke<Q: Borrow<Self::Key>>(&self, _key: &Q) {
        todo!()
    }

    fn peek<Q: Borrow<Self::Key>>(&self, key: &Q) -> Option<Cow<Self::Value>> {
        // We want to exit on the cheapest value, as it will match all the way down.
        // No side effects exist to consider, so no propagation is needed.
        match self.inner.peek(key) {
            None => self.next.peek(key),
            Some(value) => Some(value),
        }
    }

    fn put(&mut self, key: Self::Key, value: Self::Value) {
        self.next.put(key.clone(), value.clone());
        self.inner.put(key, value);
    }

    fn update(&mut self, key: Self::Key, value: Self::Value) {
        self.next.update(key.clone(), value.clone());
        self.inner.update(key, value);
    }

    fn delete<Q: Borrow<Self::Key>>(&mut self, key: &Q) -> CacheBrownsResult<Option<Self::Key>> {
        // TODO: think through propagation
        let _ = self.next.delete(key);
        self.inner.delete(key)
    }

    fn take<Q: Borrow<Self::Key>>(&mut self, key: &Q) -> CacheBrownsResult<Option<(Self::Key, Cow<Self::Value>)>> {
        todo!()
    }

    fn flush(&mut self) -> Self::FlushResultIterator {
        // TODO: If puts aren't transactional (they aren't currently because success/fail isn't communicated)
        // then this may not match. Other issue that comes to mind is coherency. If update fails at lower
        // layer we'll have mixed results. Probably need to make put communicate now since new scenarios came up.
        self.inner.flush();
        self.next.flush()
    }

    fn keys(&self) -> Self::KeyRefIterator<'_> {
        // From an external perspective, store is a monolith. Propagate to lowest (biggest) tier.
        self.next.keys()
    }

    fn contains<Q: Borrow<Self::Key>>(&self, key: &Q) -> bool {
        // From an external perspective, store is a monolith. Propagate to lowest (biggest) tier.
        self.next.contains(key)
    }
}

// Because Store isn't object safe, we can't use a container like Vec. Ideally, we would just box
// since the difference is near zero, and it's much more readable. We need to effectively create an
// "array" of arbitrary store impls. Each entry is a Store with an arbitrary "next" store. We need
// to be able to represent the end of the array in some way. Rather than evaluating Option's at
// runtime, we can create a fake no-op store as an end cap.

// TODO: Consider converting from struct to enum

#[cfg(test)]
mod tests {
    use crate::store::memory::MemoryStore;
    use crate::store::tiered::{RippleMode, TieredStore};
    use crate::store::Store;

    #[test]
    fn test() {
        let mut store =
            TieredStore::new(RippleMode::ShortCircuit, MemoryStore::new()).tier(MemoryStore::new());

        store.put(0, 0);
        assert_eq!(0, *store.get(&0).unwrap())
    }
}
