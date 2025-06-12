//! Trait for page cache
//!
//! A Page cache caches data in fixed-size pages.

use std::fmt::Debug;
use std::future::Future;
use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use object_store::ObjectMeta;
use object_store::path::Path;

use object_store::Result;

/// [PageCache] trait.
///
/// Caching fixed-size pages. Each page has a unique ID.
#[async_trait]
pub trait PageCache: Sync + Send + Debug + 'static {
    /// The size of each page.
    fn page_size(&self) -> usize;

    /// Cache capacity, in number of pages.
    fn capacity(&self) -> usize;

    /// Total used cache size in bytes.
    fn size(&self) -> usize;

    /// Read data of a page.
    ///
    /// # Parameters
    /// - `location`: the path of the object.
    /// - `page_id`: the ID of the page.
    ///
    /// # Returns
    /// - `Ok(Some(Bytes))` if the page exists and the data was read successfully.
    /// - `Ok(None)` if the page does not exist.
    /// - `Err(Error)` if an error occurred.
    async fn get_with(
        &self,
        location: &Path,
        page_id: u32,
        loader: impl Future<Output = Result<Bytes>> + Send,
    ) -> Result<Bytes>;

    /// Read cached page.
    ///
    /// # Parameters
    /// - `location`: the path of the object.
    /// - `page_id`: the ID of the page.
    ///
    /// # Returns
    /// - `Ok(Some(Bytes))` if the page exists and the data was read successfully.
    /// - `Ok(None)` if the cached page does not exist.
    /// - `Err(Error)` if an error occurred.
    async fn get(&self, location: &Path, page_id: u32) -> Result<Option<Bytes>>;

    /// Get range of data in the page.
    ///
    /// # Parameters
    /// - `id`: The ID of the page.
    /// - `range`: The range of data to read from the page. The range must be within the page size.
    ///
    /// # Returns
    /// See [Self::get_with()].
    async fn get_range_with(
        &self,
        location: &Path,
        page_id: u32,
        range: Range<usize>,
        loader: impl Future<Output = Result<Bytes>> + Send,
    ) -> Result<Bytes>;

    async fn get_range(&self, location: &Path, page_id: u32, range: Range<usize>) -> Result<Option<Bytes>>;

    /// Get metadata of the object.
    async fn head(
        &self,
        location: &Path,
        loader: impl Future<Output = Result<ObjectMeta>> + Send,
    ) -> Result<ObjectMeta>;

    /// Put data into the page.
    async fn put(&self, location: &Path, page_id: u32, data: Bytes) -> Result<()>;

    /// Remove all pages belong to the location.
    async fn invalidate(&self, location: &Path) -> Result<()>;
}
