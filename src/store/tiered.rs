use std::borrow::{Borrow, Cow};
use std::marker::PhantomData;
use crate::CacheBrownsResult;
use crate::store::Store;

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

// pub trait Tier<S> {
//     type Store;
//
//     fn tier(&self, inner: S) -> Self::Store;
// }

pub struct TieredStore<Tier, Next>
// where
//     Tier: Store,
//     Next: Store
{
    inner: Tier,
    next: Next,
    ripple_mode: RippleMode,
}

// impl<Tier> From<Tier> for TieredStore<Identity, Tier> {
//     fn from(tier: Tier) -> Self {
//         Self {
//             tier: Identity::new(),
//             next: tier,
//         }
//     }
// }

impl<Tier> TieredStore<Identity, Tier>
    where
        Tier: Store,
{
    pub fn new(ripple_mode: RippleMode, tier: Tier) -> Self {
        Self {
            inner: Identity::new(),
            next: tier,
            ripple_mode,
        }
    }

    pub fn tier<Next>(self, next: Next) -> TieredStore<Identity, TieredStore<Tier, Next>>
    where
        Next: Store
    {
        TieredStore {
            inner: self.inner,
            next: TieredStore {
                inner: self.next,
                next,
                ripple_mode: self.ripple_mode.clone()
            },
            ripple_mode: self.ripple_mode
        }
    }
}

impl<K, V, Tier, Next> Store for TieredStore<Tier, Next>
where
    Tier: Store<Key = K, Value = V>,
    Next: Store<Key = K, Value = V>,
    V: Clone,
{
    type Key = K;
    type Value = V;
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
                    RippleMode::ShortCircuit => Some(value)
                }
            }
            None => self.next.get(key)
        }
    }

    fn peek<Q: Borrow<Self::Key>>(&self, key: &Q) -> Option<Cow<Self::Value>> {
        // We want to exit on the cheapest value, as it will match all the way down.
        // No side effects exist to consider, so no propagation is needed.
        match self.inner.peek(key) {
            None => self.next.peek(key),
            Some(value) => Some(value)
        }
    }

    fn put(&mut self, key: Self::Key, value: Self::Value) {

    }

    fn update(&mut self, key: Self::Key, value: Self::Value) {
        self.inner.update(key, value)
    }

    fn delete<Q: Borrow<Self::Key>>(&mut self, key: &Q) -> CacheBrownsResult<Option<Self::Key>> {
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
        todo!()
    }

    fn contains<Q: Borrow<Self::Key>>(&self, key: &Q) -> bool {
        todo!()
    }
}

// Because Store isn't object safe, we can't use a container like Vec. Ideally, we would just box
// since the difference is near zero, and it's much more readable. We need to effectively create an
// "array" of arbitrary store impls. Each entry is a Store with an arbitrary "next" store. We need
// to be able to represent the end of the array in some way. Rather than evaluating Option's at
// runtime, we can create a fake no-op store as an end cap.

/// A no-op tier
///
/// The [`Identity`] tier immediately "fails" to induce the Tiered store to try the next tier.
struct Identity {
    _p: (),
}

impl Identity {
    pub fn new() -> Self {
        Self { _p: () }
    }
}




// pub struct TieredStoreBuilder<S> {
//     base:
// }
//
// impl<S> TieredStoreBuilder<S> {
//     pub fn new(base: S) -> Self {
//         Self {
//
//         }
//     }
// }
//
// struct Stack<Inner, Outer> {
//
// }