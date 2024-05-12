<!-- SPDX-License-Identifier: MIT OR Apache-2.0 -->

# CacheBrowns

This crate provides a declarative, programmable managed cached for native applications as well as pre-implemented strategies
for common cache designs.

It emphasizes *correctness* above all else. We're not trying to set performance records, we're eliminating entire
classes of cache invalidation bugs. This doesn't mean it's slow though. In fact, the loosely coupled design enables you
to substitute in faster storage layers by wrapping popular cache stores.

The second priority is flexibility. For maximum interoperability of components, any constraint that isn't necessary for
correctness is not a part of `Trait` contracts and certain optimizations (such as return by reference) are not mandatory
for implementors. 

Finally, speed is still important. References or [Cow](https://doc.rust-lang.org/std/borrow/enum.Cow.html) are used 
whenever possible (meaning when doing so doesn't preclude by value usage) to avoid unnecessary copies and to allow for
trait implementations that hold no in memory state to return references to.

Spiritually the concepts leveraged here are similar to "one-way data flow" and managed memory. It was inspired by a need
to address cache invalidation bugs at scale for the millions of nodes in the eventually-consistent, distributed systems
powering Azure IMDS and Azure Boost.

## Usage

TODO: EXAMPLE

See the [docs.rs page]() for full details. This README focuses on the theory behind the project.

## Background

This is a condensed discussion. For a more thorough, but still informal, exploration of these concepts refer to 
[the project announcement blog post](). For a complete, formal discussion of the theory independent of this 
implementation see ["the whitepaper"]().

### Cache Taxonomy

- Replacement: Algorithm that guides how (or if) entries are evicted when the capacity limit is reached.
- Hydration: The method by which data is retrieve and then stored
- Store: The underlying location + format of cached data
- Source of Record (SoR): The location the real data resides, used to hydrate the cache
- Cache Usage Patterns:
    - Cache-aside: Application managed cache. Application has connections to both the cache and the SoR. If entries are
      missing from the cache, it pulls the value from the SoR directly, then hydrates the cache with the returned value
    - Cache-as-SoR (managed caches): In these patterns the application operates on the cache directly, making it
      transparent
        - Read-through: Hit cache directly, if value is missing it is hydrated from SoR then returned
        - Write-through: Write to cache, make a synchronous write to SoR before returning control flow
        - Write-behind: Write to cache, queue a non-blocking write to the SoR

###  

For the sake of this discussion we'll think of caches as either `custom` and `shared`. Shared implementations would
be examples like caching built into a client such as for HTTP or a database. Custom caches exist because there is no
shared implementation for the application, or as a layer of customization on top of a shared implementation.

## Problem Statement

There are only 2 hard problems in computer science: naming thing, cache invalidation, and off-by-one errors. Most applications
rely upon some sort of external data store that locally stores the data to accelerate lookups. They are the source of much
consternation and cost:

1. Bugs. Cache invalidation is hard, and it's rare for an off the cuff implementation to be correct.
1. Large scale duplication of effort (including redundant testing) as each service builds their own LRU and/or polling
   implementation, often times even multiple cache implementations per project.
1. Poor test coverage. Unit tests usually don't exist, and general test coverage usually isn't good enough (there's a
   lot of ways for the cache to be invalidated, are you covering *all* of them? Have you covered parallel use?)
1. Typically low code quality. The cache implementation is often tightly coupled to application specific details and or
   test coverage is black box end to end scenario testing.

### Root Cause

Cache validation is inherently hard, but unforced errors often make it worse:

- **Unwarranted Optimization** - Scale matters. If an implementation detail is *slightly* slower but safer, do you really want to make that trade off? Remember, caching is meant to avoid operations that are orders of magnitude more expensive. You generally have headroom.
- **Encapsulation / Single Responsibility Principle** - The store and the replacement algorithm shouldn't be coupled, neither should application specific concepts.
- **Manual Management (i.e. cache writes)** - *Writing* to the cache is where cache invalidation issues appear. A bad read is itself a failure to have written first. Much in the way that you rarely need `unsafe` for performance, you rarely need direct write access to a cache.

## Requirements

We can address these issues by providing a generic *read-through* cache that fully encapsulates the underlying store, writes,
evictions, and staleness checks. The *cache-aside* pattern is generally an anti-pattern that stems from poorly factored caches,
not from a legitimate application specific need. Even in the case where writes or record invalidations are needed they do
not need to be handled by the application code, they can be injected as strategies.

1. **Fully-Managed:** The cache should expose only read operations, never write. If application code ever considers when to
   insert, update, or evict from the cache, then it can never be decoupled. This allows for invariants to be broken which in turn is where bugs are introduced.
1. **Programmable:** In order for a cache to be fully-managed, cache writes must be fully generic. Retrieval logic and
   validation logic must be injectable, and in this sense the subset of a typical cache helper that truly is custom and
   application-specific can be programmed into the broader implementation.
1. **Declarative:** Common strategies should be made available, so that a consumer can simply declare what type of cache
   they need by chaining together the appropriate strategies. The interface can be made easier by naming common
   combinations.
1. **Trackable:** It must be possible to easily see what happened and why when servicing a read request for performance
   tracking and debugging purposes. This does preclude certain types of minor optimizations, but is well worth it given
   that caching is bugprone and the performance data emitted can inform other system design tradeoffs that would net
   bigger gains than the added costs.

## Contributing

This project welcomes contributions and suggestions. By making contributions, you agree that they will be made available
under both licenses of this project.

## Acknowledgments / Why Fork?

This project was originally started as a Hackathon project at Microsoft. It sat around as a PoC for awhile in C++ before
it was eventually ported to Rust as a proposed design for work supporting Azure Boost. That project was funded, but the
initial investments focused on adding strategy implementations that supported the highly specialized / single purpose embedded
software applications we were developing.

This meant the initial work was disjointed enough that there wasn't much to publish on the public crate in the meantime.
It wasn't a case of not wanting to publish, it truly was specialized to target the custom embedded hardware + software in Azure Boost to the point most of it would even be useless for other parts of Azure, let alone to the world more broadly.
I believe that publishing that code in the crate would detract from it by adding noise and a source of confusion.
It would be neat to publish somewhere as a demo of how far you can push the architecture, but it doesn't belong in the crate proper.

That set of challenges just gave the idea even more credence in my mind though, since the same architecture
could even be effectively applied in that esoteric use case despite never being intended for it (the hackathon predated
our Azure Boost work). While I always had support for open sourcing the project, the reality is the full version was way
outside my job responsibilities and I couldn't find time to continue it. I've forked the initial concept repo so that I
can finish this in my personal time and add public facing features like additional optimization and greater generalization of traits.

I have received feedback over the years as this sat on the back burner from some of my colleagues at Microsoft, and I am happy 
to list them out here when they confirm they're comfortable being identified publicly that way.

## License

This project is licensed under either of

- [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) ([`LICENSE-APACHE`](LICENSE-APACHE))
- [MIT license](https://opensource.org/licenses/MIT) ([`LICENSE-MIT`](LICENSE-MIT))

at your option.

The [SPDX](https://spdx.dev) license identifier for this project is `MIT OR Apache-2.0`.
