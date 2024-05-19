use crate::store::Store;
use crate::CacheBrownsResult;
use std::borrow::{Borrow, Cow};
use std::cell::RefCell;

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
    inner: RefCell<Tier>,
    next: RefCell<Next>,
    ripple_mode: RippleMode,
}

impl<K, V, Tier, Next> TieredStore<Tier, Next>
where
    K: Clone,
    V: Clone,
    Tier: Store<Key = K, Value = V>,
    Next: Store<Key = K, Value = V>,
{
    fn inner_put(next: &mut Next, inner: &mut Tier, key: K, value: V) -> CacheBrownsResult<()> {
        next.put(key.clone(), value.clone())?;
        inner.put(key, value)
    }
}

impl<Tier> TieredStore<(), Tier>
where
    Tier: Store,
{
    pub fn new(ripple_mode: RippleMode, tier: Tier) -> Self {
        Self {
            inner: RefCell::new(()),
            next: RefCell::new(tier),
            ripple_mode,
        }
    }

    pub fn tier<Next>(self, next: Next) -> TieredStore<(), TieredStore<Tier, Next>>
    where
        Next: Store,
    {
        TieredStore {
            inner: self.inner,
            next: RefCell::new(TieredStore {
                inner: self.next,
                next: RefCell::new(next),
                ripple_mode: self.ripple_mode.clone(),
            }),
            ripple_mode: self.ripple_mode,
        }
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
        let borrow = self.inner.as_ptr();
        let asdf = unsafe { (*borrow).get(key) };

        // Walk down tiers until value is found.
        match asdf {
            Some(value) => {
                // Tiers closer to top of stack are faster, so we want to exit early, but we need to
                // apply the appropriate ripple mode effects first.
                match self.ripple_mode {
                    RippleMode::Unified => {
                        self.next.borrow().poke(key);
                        Some(value)
                    }
                    RippleMode::ShortCircuit => Some(value),
                }
            }
            None => {
                match unsafe { (*self.inner.as_ptr()).get(key) } {
                    None => None,
                    Some(value) => {
                        // Best effort, there is no correctness issue we're just attempting to use
                        // the higher cache tier (i.e. self) for future reads.
                        let _ = self.inner.borrow_mut().put(key.borrow().clone(), value.clone().into_owned());
                        Some(value)
                    }
                }
            },
        }
    }

    fn poke<Q: Borrow<Self::Key>>(&self, key: &Q) {
        self.next.borrow().poke(key);
        self.inner.borrow().poke(key);
    }

    fn peek<Q: Borrow<Self::Key>>(&self, key: &Q) -> Option<Cow<Self::Value>> {
        // We want to exit on the cheapest value, as it will match all the way down.
        // No side effects exist to consider, so no propagation is needed.
        match unsafe { (*self.inner.as_ptr()).peek(key) } {
            None => unsafe { (*self.next.as_ptr()).peek(key) },
            Some(value) => Some(value),
        }
    }

    fn put(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()> {
        Self::inner_put(&mut*self.next.borrow_mut(), &mut*self.inner.borrow_mut(), key, value)
    }

    fn update(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()> {
        self.next.borrow_mut().update(key.clone(), value.clone())?;
        self.inner.borrow_mut().update(key, value)
    }

    fn delete<Q: Borrow<Self::Key>>(&mut self, key: &Q) -> CacheBrownsResult<Option<Self::Key>> {
        self.next.borrow_mut().delete(key)?;
        self.inner.borrow_mut().delete(key)
    }

    fn take<Q: Borrow<Self::Key>>(&mut self, key: &Q) -> CacheBrownsResult<Option<(Self::Key, Self::Value)>> {
        // Delete needs to succeed or fail at base to avoid rollbacks
        match self.next.borrow_mut().take(key)? {
            // No failure, but not present. May be present here, so try take.
            None => {
                self.inner.borrow_mut().take(key)
            }
            // Attempt to cascade up the stack, we already have value so use cheaper delete
            Some(lower_value) => {
                match self.inner.borrow_mut().delete(key) {
                    Ok(_) => Ok(Some(lower_value)),
                    // We have a value, but we need to ensure higher tiers don't attempt deletes to
                    // prevent rollbacks
                    // TODO: Consider having the option always returned so it could still propagate the value?
                    Err(e) => Err(e)
                }
            }
        }
    }

    fn flush(&mut self) -> Self::FlushResultIterator {
        // TODO: If puts aren't transactional (they aren't currently because success/fail isn't communicated)
        // then this may not match. Other issue that comes to mind is coherency. If update fails at lower
        // layer we'll have mixed results. Probably need to make put communicate now since new scenarios came up.
        self.inner.borrow_mut().flush();
        self.next.borrow_mut().flush()
    }

    fn keys(&self) -> Self::KeyRefIterator<'_> {
        // From an external perspective, store is a monolith. Propagate to lowest (biggest) tier.
        unsafe { (*self.next.as_ptr()).keys() }
    }

    fn contains<Q: Borrow<Self::Key>>(&self, key: &Q) -> bool {
        // From an external perspective, store is a monolith. Propagate to lowest (biggest) tier.
        self.next.borrow().contains(key)
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
        unsafe { (*self.next.as_ptr()).get(key) }
    }

    fn poke<Q: Borrow<Self::Key>>(&self, key: &Q) {
        unsafe { (*self.next.as_ptr()).poke(key) }
    }

    fn peek<Q: Borrow<Self::Key>>(&self, key: &Q) -> Option<Cow<Self::Value>> {
        unsafe { (*self.next.as_ptr()).peek(key) }
    }

    fn put(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()> {
        self.next.borrow_mut().put(key, value)
    }

    fn update(&mut self, key: Self::Key, value: Self::Value) -> CacheBrownsResult<()> {
        self.next.borrow_mut().update(key, value)
    }

    fn delete<Q: Borrow<Self::Key>>(&mut self, key: &Q) -> CacheBrownsResult<Option<Self::Key>> {
        self.next.borrow_mut().delete(key)
    }

    fn take<Q: Borrow<Self::Key>>(&mut self, key: &Q) -> CacheBrownsResult<Option<(Self::Key, Self::Value)>> {
        //unsafe { (*self.next.as_ptr()).peek(key) }
        self.next.borrow_mut().take(key)
    }

    fn flush(&mut self) -> Self::FlushResultIterator {
        self.next.borrow_mut().flush()
    }

    fn keys(&self) -> Self::KeyRefIterator<'_> {
        unsafe { (*self.next.as_ptr()).keys() }
    }

    fn contains<Q: Borrow<Self::Key>>(&self, key: &Q) -> bool {
        self.next.borrow().contains(key)
    }
}

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

        assert!(store.put(0, 0).is_ok());
        assert_eq!(0, *store.get(&0).unwrap())
    }
}
