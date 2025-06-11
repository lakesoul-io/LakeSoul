use fs::File;
use fs_err as fs;
use std::borrow::Borrow;
use std::boxed::Box;
use std::collections::hash_map::RandomState;
use std::error::Error as StdError;
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::hash::BuildHasher;
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use tracing::{debug, error, warn};

pub use super::lru_cache::{LruCache, Meter};
use walkdir::WalkDir;

#[derive(Debug)]
struct FileSize;

/// Given a tuple of (path, LruDiskCacheEntry), use the filesize for measurement.When key is path
impl Meter<OsString, LruDiskCacheEntry> for FileSize {
    type Measure = usize;
    fn measure<Q: ?Sized>(&self, _: &Q, v: &LruDiskCacheEntry) -> usize
    where
        OsString: Borrow<Q>,
    {
        v.size as usize
    }
}

/// Given a tuple of (file handle, filesize), use the filesize for measurement,when key is file handle.
impl<K> Meter<K, u64> for FileSize {
    type Measure = usize;
    fn measure<Q: ?Sized>(&self, _: &Q, v: &u64) -> usize
    where
        K: Borrow<Q>,
    {
        *v as usize
    }
}

#[derive(Debug)]
pub struct LruDiskCacheEntry {
    file: File,
    size: u64,
}

/// An LRU cache of files on disk.
#[derive(Debug)]
pub struct LruDiskCache<S: BuildHasher = RandomState> {
    lru: RwLock<LruCache<OsString, LruDiskCacheEntry, S, FileSize>>,
    root: PathBuf,
    pending_size: u64,
}

/// Return an iterator of `(path, size)` of files under `path` sorted by ascending last-modified
/// time, such that the oldest modified file is returned first.
fn get_all_files<P: AsRef<Path>>(path: P) -> Box<dyn Iterator<Item = (PathBuf, u64)>> {
    let files: Vec<_> = WalkDir::new(path.as_ref())
        .into_iter()
        .filter_map(|e| {
            e.ok().and_then(|f| {
                // Only look at files
                if f.file_type().is_file() {
                    // Get the last-modified time, size, and the full path.
                    f.metadata().ok().and_then(|m| {
                        Some((f.path().to_owned(), m.len()))
                        // m.modified()
                        //     .ok()
                        //     .map(|_| )
                    })
                } else {
                    None
                }
            })
        })
        .collect();
    // Sort by last-modified-time, so oldest file first.
    // files.sort_by_key(|k| k.0);
    Box::new(files.into_iter().map(|(path, size)| (path, size)))
}

impl<S: BuildHasher> Drop for LruDiskCache<S> {
    fn drop(&mut self) {
        let root = self.root.clone();
        for (file, _) in get_all_files(root) {
            fs::remove_file(file).unwrap();
        }
    }
}

/// Errors returned by this crate.
#[derive(Debug)]
pub enum Error {
    /// The file was too large to fit in the cache.
    FileTooLarge,
    /// The file was not in the cache.
    FileNotInCache,
    /// An IO Error occurred.
    Io(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::FileTooLarge => write!(f, "File too large"),
            Error::FileNotInCache => write!(f, "File not in cache"),
            // Error::Io(ref e) => write!(f, "{}", e),
            Error::Io(e) => write!(f, "{}", e),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::FileTooLarge => None,
            Error::FileNotInCache => None,
            // Error::Io(ref e) => Some(e),
            Error::Io(e) => Some(e),
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::Io(e)
    }
}

/// A convenience `Result` type
pub type Result<T> = std::result::Result<T, Error>;

/// Trait objects can't be bounded by more than one non-builtin trait.
pub trait ReadSeek: Read + Seek + Send {}

impl<T: Read + Seek + Send> ReadSeek for T {}

enum AddFile<'a> {
    _AbsPath(PathBuf),
    RelPath(&'a OsStr),
}

impl LruDiskCache {
    /// Create an `LruDiskCache` that stores files in `path`, limited to `size` bytes.
    ///
    /// Existing files in `path` will be stored with their last-modified time from the filesystem
    /// used as the order for the recency of their use. Any files that are individually larger
    /// than `size` bytes will be removed.
    ///
    /// The cache is not observant of changes to files under `path` from external sources, it
    /// expects to have sole maintence of the contents.
    pub fn new<T>(path: T, size: u64) -> Result<Self>
    where
        PathBuf: From<T>,
    {
        LruDiskCache {
            lru: RwLock::new(LruCache::with_meter(size, FileSize)),
            root: PathBuf::from(path),
            pending_size: 0,
        }
        .init()
    }

    /// Return the current size of all the files in the cache.
    pub fn size(&self) -> u64 {
        self.lru.read().unwrap().size() + self.pending_size
    }

    /// Return the count of entries in the cache.
    pub fn len(&self) -> usize {
        self.lru.read().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.lru.read().unwrap().len() == 0
    }

    /// Return the maximum size of the cache.
    pub fn capacity(&self) -> u64 {
        self.lru.read().unwrap().capacity()
    }

    /// Return the path in which the cache is stored.
    pub fn path(&self) -> &Path {
        self.root.as_path()
    }

    /// Return the path that `key` would be stored at.
    fn rel_to_abs_path<K: AsRef<Path>>(&self, rel_path: K) -> PathBuf {
        self.root.join(rel_path)
    }

    /// Scan `self.root` for existing files and store them.
    fn init(self) -> Result<Self> {
        fs::create_dir_all(&self.root)?;
        Ok(self)
    }

    /// Returns `true` if the disk cache can store a file of `size` bytes.
    pub fn can_store(&self, size: u64) -> bool {
        size <= self.lru.read().unwrap().capacity()
    }

    /// Make space for a file of `size` bytes.
    fn make_space(&self, size: u64) -> Result<()> {
        if !self.can_store(size) {
            return Err(Error::FileTooLarge);
        }
        //TODO: ideally LRUCache::insert would give us back the entries it had to remove.
        while self.size() + size > self.capacity() {
            let (rel_path, _) = self
                .lru
                .write()
                .unwrap()
                .remove_lru()
                .expect("Unexpectedly empty cache!");
            let remove_path = self.rel_to_abs_path(rel_path);
            //TODO: check that files are removable during `init`, so that this is only
            // due to outside interference.
            fs::remove_file(&remove_path).unwrap_or_else(|e| {
                // Sometimes the file has already been removed
                // this seems to happen when the max cache size has been reached
                if e.kind() == std::io::ErrorKind::NotFound {
                    debug!(
                        "Error removing file from cache as it was not found: `{:?}`",
                        remove_path
                    );
                } else {
                    panic!(
                        "Error removing file from cache: `{:?}`: {}, {:?}",
                        remove_path,
                        e,
                        e.kind()
                    )
                }
            });
        }
        Ok(())
    }

    /// Add the file at `path` of size `size` to the cache.
    fn add_file(&self, addfile_path: AddFile<'_>, size: u64) -> Result<()> {
        let rel_path = match addfile_path {
            AddFile::_AbsPath(ref p) => p.strip_prefix(&self.root).expect("Bad path?").as_os_str(),
            AddFile::RelPath(p) => p,
        };
        self.make_space(size)?;
        let path = self.rel_to_abs_path(rel_path);
        // let file =  File::open(path);
        let lru_disk_cache_entry = LruDiskCacheEntry {
            file: File::open(path)?,
            size,
        };
        self.lru
            .write()
            .unwrap()
            .insert(rel_path.to_owned(), lru_disk_cache_entry);
        Ok(())
    }

    /// Add a file by calling `by` with a `File` corresponding to the cache at path `key`.
    fn insert_by<K: AsRef<OsStr>, F: FnOnce(&Path) -> io::Result<()>>(
        &self,
        key: K,
        size: Option<u64>,
        by: F,
    ) -> Result<()> {
        if let Some(size) = size {
            if !self.can_store(size) {
                return Err(Error::FileTooLarge);
            }
        }
        let rel_path = key.as_ref();
        let path = self.rel_to_abs_path(rel_path);
        fs::create_dir_all(path.parent().expect("Bad path?"))?;
        by(&path)?;
        let size = match size {
            Some(size) => size,
            None => fs::metadata(path)?.len(),
        };
        self.add_file(AddFile::RelPath(rel_path), size).map_err(|e| {
            error!("Failed to insert file `{}`: {}", rel_path.to_string_lossy(), e);
            fs::remove_file(self.rel_to_abs_path(rel_path)).expect("Failed to remove file we just created!");
            e
        })
    }

    /// Add a file by calling `with` with the open `File` corresponding to the cache at path `key`.
    pub fn insert_with<K: AsRef<OsStr>, F: FnOnce(File) -> io::Result<()>>(&self, key: K, with: F) -> Result<()> {
        self.insert_by(key, None, |path| with(File::create(path)?))
    }

    /// Add a file with `bytes` as its contents to the cache at path `key`.
    pub fn insert_bytes<K: AsRef<OsStr>>(&self, key: K, bytes: &[u8]) -> Result<()> {
        self.insert_by(key, Some(bytes.len() as u64), |path| {
            let mut f = File::create(path)?;
            f.write_all(bytes)?;
            Ok(())
        })
    }

    /// Add an existing file at `path` to the cache at path `key`.
    pub fn insert_file<K: AsRef<OsStr>, P: AsRef<OsStr>>(&self, key: K, path: P) -> Result<()> {
        let size = fs::metadata(path.as_ref())?.len();
        self.insert_by(key, Some(size), |new_path| {
            fs::rename(path.as_ref(), new_path).or_else(|_| {
                warn!("fs::rename failed, falling back to copy!");
                fs::copy(path.as_ref(), new_path)?;
                fs::remove_file(path.as_ref())
                    .unwrap_or_else(|e| error!("Failed to remove original file in insert_file: {}", e));
                Ok(())
            })
        })
    }

    /// Return `true` if a file with path `key` is in the cache.
    pub fn contains_key<K: AsRef<OsStr>>(&self, key: K) -> bool {
        self.lru.read().unwrap().contains_key(key.as_ref())
    }

    /// Get an opened `File` for `key`, if one exists and can be opened. Updates the LRU state
    /// of the file if present. Avoid using this method if at all possible, prefer `.get`.
    pub fn get_file<K: AsRef<OsStr>>(&self, key: K) -> Option<File> {
        let rel_path = key.as_ref();
        let mut guard = self.lru.write().unwrap();
        let file = guard.get(rel_path);
        Some(file?.file.try_clone().unwrap())
    }

    /// Get an opened readable and seekable handle to the file at `key`, if one exists and can
    /// be opened. Updates the LRU state of the file if present.
    pub fn get<K: AsRef<OsStr>>(&self, key: K) -> Option<Vec<u8>> {
        match self.get_file(key).map(|f| Box::new(f) as Box<dyn ReadSeek>) {
            Some(mut f) => {
                let mut buf = vec![];
                f.by_ref().read_to_end(&mut buf).unwrap();
                // After reading, seek back to the beginning, so that the file can be read again.
                f.seek(io::SeekFrom::Start(0)).unwrap();
                return Some(buf);
            }
            None => None,
        }
    }

    /// Remove the given key from the cache.
    pub fn remove<K: AsRef<OsStr>>(&self, key: K) -> Result<()> {
        match self.lru.write().unwrap().remove(key.as_ref()) {
            Some(_) => {
                let path = self.rel_to_abs_path(key.as_ref());
                fs::remove_file(&path).map_err(|e| {
                    error!("Error removing file from cache: `{:?}`: {}", path, e);
                    Into::into(e)
                })
            }
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fs::{self, File};
    use super::{Error, LruDiskCache};
    use std::io::{self, Write};
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    struct TestFixture {
        /// Temp directory.
        pub tempdir: TempDir,
    }

    fn create_file<T: AsRef<Path>, F: FnOnce(File) -> io::Result<()>>(
        dir: &Path,
        path: T,
        fill_contents: F,
    ) -> io::Result<PathBuf> {
        let b = dir.join(path);
        fs::create_dir_all(b.parent().unwrap())?;
        let f = fs::File::create(&b)?;
        fill_contents(f)?;
        b.canonicalize()
    }

    impl TestFixture {
        pub fn new() -> TestFixture {
            TestFixture {
                tempdir: tempfile::Builder::new()
                    .prefix("lru-disk-cache-test")
                    .tempdir()
                    .unwrap(),
            }
        }

        pub fn tmp(&self) -> &Path {
            self.tempdir.path()
        }

        pub fn create_file<T: AsRef<Path>>(&self, path: T, size: usize) -> PathBuf {
            create_file(self.tempdir.path(), path, |mut f| f.write_all(&vec![0; size])).unwrap()
        }
    }

    #[test]
    fn test_empty_dir() {
        let f = TestFixture::new();
        LruDiskCache::new(f.tmp(), 1024).unwrap();
    }

    #[test]
    fn test_missing_root() {
        let f = TestFixture::new();
        LruDiskCache::new(f.tmp().join("not-here"), 1024).unwrap();
    }

    #[test]
    fn test_insert_bytes() {
        let f = TestFixture::new();
        let c = LruDiskCache::new(f.tmp(), 25).unwrap();
        c.insert_bytes("a/b/c", &[0; 10]).unwrap();
        assert!(c.contains_key("a/b/c"));
        c.insert_bytes("a/b/d", &[0; 10]).unwrap();
        assert_eq!(c.size(), 20);
        // Adding this third file should put the cache above the limit.
        c.insert_bytes("x/y/z", &[0; 10]).unwrap();
        assert_eq!(c.size(), 20);
        // The least-recently-used file should have been removed.
        assert!(!c.contains_key("a/b/c"));
        assert!(!f.tmp().join("a/b/c").exists());
    }

    #[test]
    fn test_insert_bytes_exact() {
        // Test that files adding up to exactly the size limit works.
        let f = TestFixture::new();
        let c = LruDiskCache::new(f.tmp(), 20).unwrap();
        c.insert_bytes("file1", &[1; 10]).unwrap();
        c.insert_bytes("file2", &[2; 10]).unwrap();
        assert_eq!(c.size(), 20);
        c.insert_bytes("file3", &[3; 10]).unwrap();
        assert_eq!(c.size(), 20);
        assert!(!c.contains_key("file1"));
    }

    #[test]
    fn test_add_get_lru() {
        let f = TestFixture::new();
        {
            let c = LruDiskCache::new(f.tmp(), 25).unwrap();
            c.insert_bytes("file1", &[1; 10]).unwrap();
            c.insert_bytes("file2", &[2; 10]).unwrap();
            // Get the file to bump its LRU status.
            c.get("file1").unwrap();

            assert_eq!(c.get("file1").unwrap(), vec![1u8; 10]);
            assert_eq!(c.get("file1").unwrap(), vec![1u8; 10]);
            assert_eq!(c.get("file1").unwrap(), vec![1u8; 10]);
            assert_eq!(c.get("file1").unwrap(), vec![1u8; 10]);
            // Adding this third file should put the cache above the limit.
            c.insert_bytes("file3", &[3; 10]).unwrap();
            assert_eq!(c.size(), 20);
            // The least-recently-used file should have been removed.
            assert!(!c.contains_key("file2"));
        }
        // Get rid of the cache, to test that the LRU persists on-disk as mtimes.
        // This is hacky, but mtime resolution on my mac with HFS+ is only 1 second, so we either
        // need to have a 1 second sleep in the test (boo) or adjust the mtimes back a bit so
        // that updating one file to the current time actually works to make it newer.
        // set_mtime_back(f.tmp().join("file1"), 5);
        // set_mtime_back(f.tmp().join("file3"), 5);
        // {
        //     let mut c = LruDiskCache::new(f.tmp(), 25).unwrap();
        //     // Bump file1 again.
        //     c.get("file1").unwrap();
        // }
        // // Now check that the on-disk mtimes were updated and used.
        // {
        //     let mut c = LruDiskCache::new(f.tmp(), 25).unwrap();
        //     assert!(c.contains_key("file1"));
        //     assert!(c.contains_key("file3"));
        //     assert_eq!(c.size(), 20);
        //     // Add another file to bump out the least-recently-used.
        //     c.insert_bytes("file4", &[4; 10]).unwrap();
        //     assert_eq!(c.size(), 20);
        //     assert!(!c.contains_key("file3"));
        //     assert!(c.contains_key("file1"));
        // }
    }

    #[test]
    fn test_insert_bytes_too_large() {
        let f = TestFixture::new();
        let c = LruDiskCache::new(f.tmp(), 1).unwrap();
        match c.insert_bytes("a/b/c", &[0; 2]) {
            Err(Error::FileTooLarge) => {}
            x => panic!("Unexpected result: {:?}", x),
        }
    }

    #[test]
    fn test_insert_file() {
        let f = TestFixture::new();
        let p1 = f.create_file("file1", 10);
        let p2 = f.create_file("file2", 10);
        let p3 = f.create_file("file3", 10);
        let c = LruDiskCache::new(f.tmp().join("cache"), 25).unwrap();
        c.insert_file("file1", &p1).unwrap();
        assert_eq!(c.len(), 1);
        c.insert_file("file2", &p2).unwrap();
        assert_eq!(c.len(), 2);
        // Get the file to bump its LRU status.
        assert_eq!(c.get("file1").unwrap(), vec![0u8; 10]);
        // Adding this third file should put the cache above the limit.
        c.insert_file("file3", &p3).unwrap();
        assert_eq!(c.len(), 2);
        assert_eq!(c.size(), 20);
        // The least-recently-used file should have been removed.
        assert!(!c.contains_key("file2"));
        assert!(!p1.exists());
        assert!(!p2.exists());
        assert!(!p3.exists());
    }

    #[test]
    fn test_remove() {
        let f = TestFixture::new();
        let p1 = f.create_file("file1", 10);
        let p2 = f.create_file("file2", 10);
        let p3 = f.create_file("file3", 10);
        let c = LruDiskCache::new(f.tmp().join("cache"), 25).unwrap();
        c.insert_file("file1", &p1).unwrap();
        c.insert_file("file2", &p2).unwrap();
        c.remove("file1").unwrap();
        c.insert_file("file3", &p3).unwrap();
        assert_eq!(c.len(), 2);
        assert_eq!(c.size(), 20);

        // file1 should have been removed.
        assert!(!c.contains_key("file1"));
        assert!(!f.tmp().join("cache").join("file1").exists());
        assert!(f.tmp().join("cache").join("file2").exists());
        assert!(f.tmp().join("cache").join("file3").exists());
        assert!(!p1.exists());
        assert!(!p2.exists());
        assert!(!p3.exists());

        let p4 = f.create_file("file1", 10);
        c.insert_file("file1", &p4).unwrap();
        assert_eq!(c.len(), 2);
        // file2 should have been removed.
        assert!(c.contains_key("file1"));
        assert!(!c.contains_key("file2"));
        assert!(!f.tmp().join("cache").join("file2").exists());
        assert!(!p4.exists());
    }

    fn get_zipf() -> usize {
        use rand::distributions::Distribution;

        let mut rng = rand::thread_rng();
        let zipf = zipf::ZipfDistribution::new(1024 * 1024, 1.03).unwrap();
        zipf.sample(&mut rng)
    }

    /// page = 1 * 1024 hit percent 0.78448486328125
    /// test lru disk cache hit percent from zipf
    /// disk cache size: 64 * 1024 * 1024
    /// page count: 1024 * 1024
    /// page size: 4 * 1024
    /// page hit percent: 0.78448486328125
    /// page miss percent: 0.21551513671875
    #[test]
    fn test_lru_disk_cache_from_zipf() {
        let f = TestFixture::new();
        let mut cache_hit = 0;
        let mut cache_miss = 0;
        // let mut vec_p: Vec<PathBuf>;
        {
            let c = LruDiskCache::new(f.tmp(), 1024 * 1024 * 64).unwrap();
            let file_prefix = "file";
            for i in 0..(1024 * 16) as usize {
                let file_name = format!("{}{}", file_prefix, i);
                c.insert_bytes(file_name, &[i as u8; 1024 * 4]).unwrap();
            }

            for _ in 0..1024 * 1024 * 12 {
                let object_id = get_zipf();
                let file_name = format!("{}{}", file_prefix, object_id);
                match c.get(&file_name) {
                    Some(_) => cache_hit += 1,
                    None => {
                        cache_miss += 1;
                        c.insert_bytes(file_name, &[object_id as u8; 1024 * 4]).unwrap();
                    } // _ => panic!("Unexpected result"),
                }
            }
            println!("cache hit: {}, cache miss: {}", cache_hit, cache_miss)
        }
    }

    // #[test]
    // fn test_create_file_performance() {
    //     let file_handle = File::open("file1").unwrap();
    //     file_handle.metadata().unwrap().len();
    // }
}
