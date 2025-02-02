use crate::source_of_record::SourceOfRecord;
use http::{Request, Response};
use http_cache_semantics::{BeforeRequest, CacheOptions, CachePolicy};
use std::borrow::Borrow;
use std::time::SystemTime;

/// Implements an HTTP [`SourceOfRecord`] that respects cache control headers by wrapping an
/// implementation of [`HttpFetcher`].
pub struct HttpDataSourceOfRecord<F> {
    fetcher: F,
    cache_options: CacheOptions,
}

impl<F> HttpDataSourceOfRecord<F> {
    /// Create a [`HttpDataSourceOfRecord`] with default values for [`CacheOptions`]. The defaults
    /// are from [`DEFAULT_CACHE_OPTIONS`]
    pub fn new(fetcher: F) -> Self {
        Self {
            fetcher,
            cache_options: DEFAULT_CACHE_OPTIONS,
        }
    }

    /// Create the `HttpDataSourceOfRecord` with custom values for [`CacheOptions`].
    pub fn new_with_custom_cache_options(fetcher: F, cache_options: CacheOptions) -> Self {
        Self {
            fetcher,
            cache_options,
        }
    }
}

/// An HTTP client that can retrieve and extract values. Users of implementations of this trait can
/// abstract out the request generation, data fetch, and parse operations, allowing them to
/// implement generic logic for HTTP operations such as integrating with cache control headers.
#[trait_variant::make(Send + Sync)]
pub trait HttpFetcher {
    type Key: Clone + Send + Sync;
    type Value: Clone + Send + Sync;
    type RequestBody: Clone + Send + Sync;
    type ResponseBody: Send + Sync;

    /// Create (not send) a [`Request`] for the given key. Implementors don't need to consider cache
    /// control headers. Create the request as you normally would if caching wasn't considered.
    ///
    /// Return `None` if the request cannot be created for some reason (e.g. if auth headers are
    /// required and a login attempt failed).
    async fn create_request(&self, key: &Self::Key) -> Option<Request<Self::RequestBody>>;

    /// Execute the [`Request`]. Do **not** modify the request in *any* way before sending it. The
    /// user of your implementation will potentially inject cache-control headers. Any headers that
    /// you need to inject should have already been added in [`create_request`].
    async fn fetch(
        &self,
        request: Request<Self::RequestBody>,
    ) -> Option<(Response<Self::ResponseBody>, Self::Value)>;
}

/// Default [`CacheOptions`] when custom ones aren't provided.
const DEFAULT_CACHE_OPTIONS: CacheOptions = CacheOptions {
    shared: false,
    cache_heuristic: 0.1,
    immutable_min_time_to_live: std::time::Duration::from_secs(24 * 3600),
    ignore_cargo_cult: true,
};

impl<F> SourceOfRecord for HttpDataSourceOfRecord<F>
where
    F: HttpFetcher,
{
    type Key = F::Key;
    type Value = HttpRecord<F::Value>;

    async fn retrieve<Q: Borrow<Self::Key> + Sync>(&self, key: &Q) -> Option<Self::Value> {
        let request = self.fetcher.create_request(key.borrow()).await?;
        self.fetch_value(&request).await
    }

    async fn retrieve_with_hint<Q: Borrow<Self::Key> + Sync, V: Borrow<Self::Value> + Sync>(
        &self,
        key: &Q,
        current_value: &V,
    ) -> Option<Self::Value> {
        let request = self.fetcher.create_request(key.borrow()).await?;
        Some(
            match current_value
                .borrow()
                .cache_policy
                .before_request(&request, SystemTime::now())
            {
                BeforeRequest::Fresh(_) => (*current_value.borrow()).clone(),
                BeforeRequest::Stale {
                    request: _,
                    matches: _,
                } => self.fetch_value(&request).await?,
            },
        )
    }

    async fn is_valid(&self, _key: &Self::Key, value: &Self::Value) -> bool {
        value.cache_policy.is_stale(SystemTime::now())
    }
}

impl<F> HttpDataSourceOfRecord<F>
where
    F: HttpFetcher,
{
    async fn fetch_value(
        &self,
        request: &Request<<F as HttpFetcher>::RequestBody>,
    ) -> Option<HttpRecord<<F as HttpFetcher>::Value>> {
        let (response, value) = self.fetcher.fetch(request.clone()).await?;

        Some(HttpRecord {
            inner: value,
            cache_policy: CachePolicy::new_options(
                request,
                &response,
                SystemTime::now(),
                self.cache_options,
            ),
        })
    }
}

#[derive(Clone)]
pub struct HttpRecord<T> {
    inner: T,
    cache_policy: CachePolicy,
}

impl<T> HttpRecord<T> {
    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn into_inner(self) -> T {
        self.inner
    }
}

// TODO: Confirm there isn't a better way before implementing deserialize.
// impl<T> Serialize for HttpRecord<T>
// where
//     T: Serialize,
// {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: Serializer
//     {
//         let mut s = serializer.serialize_struct("HttpRecord", 2)?;
//         s.serialize_field("inner", &self.inner)?;
//         s.serialize_field("cache_policy", &self.cache_policy)
//     }
// }
