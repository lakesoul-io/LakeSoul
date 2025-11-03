use fs::File;
// use fs_err::os::unix::fs::FileExt;
// use fs_err as fs;
use std::borrow::Borrow;
use std::boxed::Box;
use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::error::Error as StdError;
use std::ffi::{OsStr, OsString};
use std::hash::BuildHasher;
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::{fmt, fs};
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
    file_lock: RwLock<HashMap<OsString, Arc<RwLock<OsString>>>>,
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
                    f.metadata().ok().map(|m| (f.path().to_owned(), m.len()))
                } else {
                    None
                }
            })
        })
        .collect();
    Box::new(files.into_iter())
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
        // temp, remove all files in path
        let root = PathBuf::from(path).clone();
        for (file, _) in get_all_files(root.clone()) {
            fs::remove_file(file).unwrap();
        }
        // println!("drop LruDiskCache");
        LruDiskCache {
            lru: RwLock::new(LruCache::with_meter(size, FileSize)),
            file_lock: RwLock::new(HashMap::new()),
            root,
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
            // add file lock,sure threads don't remove the same file
            let mut files_lock = self.file_lock.write().unwrap();
            let mut lru_writer = self.lru.write().unwrap();
            let (rel_path, _) =
                lru_writer.remove_lru().expect("Unexpectedly empty cache!");
            let file_lock = files_lock.get(&rel_path).unwrap().clone();
            let _file_write_lock = file_lock.write().unwrap();
            files_lock.remove(&rel_path);
            drop(files_lock);
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
            AddFile::_AbsPath(ref p) => {
                p.strip_prefix(&self.root).expect("Bad path?").as_os_str()
            }
            AddFile::RelPath(p) => p,
        };
        self.make_space(size)?;
        let path = self.rel_to_abs_path(rel_path);
        // let file =  File::open(path);
        let mut lru_writer = self.lru.write().unwrap();
        let lru_disk_cache_entry = LruDiskCacheEntry {
            file: File::open(path)?,
            size,
        };

        lru_writer.insert(rel_path.to_owned(), lru_disk_cache_entry);
        Ok(())
    }

    /// Add a file by calling `by` with a `File` corresponding to the cache at path `key`.
    fn insert_by<K: AsRef<OsStr>, F: FnOnce(&Path) -> io::Result<()>>(
        &self,
        key: K,
        size: Option<u64>,
        by: F,
    ) -> Result<()> {
        if let Some(size) = size
            && !self.can_store(size) {
                return Err(Error::FileTooLarge);
            }
        let rel_path = key.as_ref();
        let path = self.rel_to_abs_path(rel_path);
        fs::create_dir_all(path.parent().expect("Bad path?"))?;
        let mut files_lock = self.file_lock.write().unwrap();
        if files_lock.contains_key(rel_path) {
            return Ok(());
        }
        let file_lock = Arc::new(RwLock::new(rel_path.to_owned()));
        files_lock.insert(rel_path.to_owned(), file_lock.clone());
        drop(files_lock);
        let _res = file_lock.write().unwrap();
        by(&path)?;
        let size = match size {
            Some(size) => size,
            None => fs::metadata(path)?.len(),
        };
        debug!(
            "[lakesoul::cache::lru_cache] Inserting file {:?} of size {:?}",
            rel_path.to_string_lossy(),
            size
        );
        self.add_file(AddFile::RelPath(rel_path), size)
            .map_err(|e| {
                error!(
                    "Failed to insert file `{}`: {}",
                    rel_path.to_string_lossy(),
                    e
                );
                fs::remove_file(self.rel_to_abs_path(rel_path))
                    .expect("Failed to remove file we just created!");
                e
            })
    }

    /// Add a file by calling `with` with the open `File` corresponding to the cache at path `key`.
    pub fn insert_with<K: AsRef<OsStr>, F: FnOnce(File) -> io::Result<()>>(
        &self,
        key: K,
        with: F,
    ) -> Result<()> {
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
    pub fn insert_file<K: AsRef<OsStr>, P: AsRef<OsStr>>(
        &self,
        key: K,
        path: P,
    ) -> Result<()> {
        let size = fs::metadata(path.as_ref())?.len();
        self.insert_by(key, Some(size), |new_path| {
            fs::rename(path.as_ref(), new_path).or_else(|_| {
                warn!("fs::rename failed, falling back to copy!");
                fs::copy(path.as_ref(), new_path)?;
                fs::remove_file(path.as_ref()).unwrap_or_else(|e| {
                    error!("Failed to remove original file in insert_file: {}", e)
                });
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
        // Due to LRU's nature, no read-write conflicts occur, so read locks are commented out.
        // Uncomment them if conflicts arise later.
        // let file_lock = match self.file_lock
        //     .read()
        //     .unwrap().get(key.as_ref()) {
        //     Some(lock) => lock.clone(),
        //     None => return None,
        //     };
        // let _read_file_lock = file_lock.read().unwrap();
        if let Some(file) = self.get_file(&key) {
            let file_size = file.metadata().unwrap().len() as usize;
            let mut buf = vec![0; file_size];
            // debug!("[laesoul::cache::lru_cache] read file: {:?}, size: {}", key.try_into(), file_size);
            // let _ = file.read_at(&mut buf, 0).unwrap();
            #[cfg(target_family = "windows")]
            {
                use std::os::windows::fs::FileExt;
                match file.seek_read(&mut buf, 0) {
                    Ok(v) => Some(buf),
                    Err(e) => {
                        error!(
                            "[laesoul::cache::lru_cache] Error reading file from cache."
                        );
                        return None;
                    }
                }
            }
            #[cfg(target_family = "unix")]
            {
                use std::os::unix::fs::FileExt;
                match file.read_at(&mut buf, 0) {
                    Ok(_) => Some(buf),
                    Err(_) => {
                        error!(
                            "[laesoul::cache::lru_cache] Error reading file from cache."
                        );
                        None
                    }
                }
            }
        } else {
            None
        }
    }

    /// Remove the given key from the cache.
    pub fn remove<K: AsRef<OsStr>>(&self, key: K) -> Result<()> {
        let mut files_lock = self.file_lock.write().unwrap();
        match self.lru.write().unwrap().remove(key.as_ref()) {
            Some(_) => {
                let file_lock = files_lock.get(key.as_ref()).unwrap().clone();
                let _file_write_lock = file_lock.write().unwrap();
                files_lock.remove(key.as_ref());
                drop(files_lock);
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
    use rand_distr::Zipf;
    use std::io::{self, Write};
    use std::path::{Path, PathBuf};
    use std::thread;
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
            create_file(self.tempdir.path(), path, |mut f| {
                f.write_all(&vec![0; size])
            })
            .unwrap()
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
            c.insert_bytes("file1", &[1; 10]).unwrap();
            c.insert_bytes("file1", &[1; 10]).unwrap();
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
        use rand::distr::Distribution;

        let mut rng = rand::rng();

        let zipf: Zipf<f64> = rand_distr::Zipf::new((1024 * 1024) as f64, 1.03).unwrap();
        zipf.sample(&mut rng) as usize
    }

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
                        c.insert_bytes(file_name, &[object_id as u8; 1024 * 4])
                            .unwrap();
                    } // _ => panic!("Unexpected result"),
                }
            }
            println!("cache hit: {}, cache miss: {}", cache_hit, cache_miss)
        }
    }

    #[test]
    fn test_multi_thread_get_lru() {
        let f = TestFixture::new();
        {
            pub const M: usize = 1024 * 1024 * 4;
            let c =
                std::sync::Arc::new(LruDiskCache::new(f.tmp(), 4 * M as u64).unwrap());
            // let c = LruDiskCache::new(f.tmp(), 25).unwrap();
            c.insert_bytes("file1", &[1; 2 * M]).unwrap();
            c.get("file1").unwrap();
            let mut handles = vec![];
            for _ in 0..40 {
                let c = c.clone();
                let handle = thread::spawn(move || {
                    assert_eq!(c.get("file1").unwrap(), vec![1u8; 2 * M]);
                });
                handles.push(handle);
            }

            assert_eq!(c.get("file1").unwrap(), vec![1u8; 2 * M]);
            assert_eq!(c.get("file1").unwrap(), vec![1u8; 2 * M]);
            assert_eq!(c.get("file1").unwrap(), vec![1u8; 2 * M]);
            assert_eq!(c.get("file1").unwrap(), vec![1u8; 2 * M]);
            // Adding this third file should put the cache above the limit.
            c.insert_bytes("file3", &[3; 10]).unwrap();
            handles
                .into_iter()
                .for_each(|handle| handle.join().unwrap());
        }
    }

    #[test]
    fn test_multi_thread_insert_lru() {
        let f = TestFixture::new();
        {
            pub const M: usize = 1024 * 4;
            let c =
                std::sync::Arc::new(LruDiskCache::new(f.tmp(), 4 * M as u64).unwrap());
            // let c = LruDiskCache::new(f.tmp(), 25).unwrap();
            c.insert_bytes("file1", &[1; 2 * M]).unwrap();
            c.get("file1").unwrap();
            let mut handles = vec![];
            for _ in 0..100 {
                let c = c.clone();
                let handle = thread::spawn(move || {
                    c.insert_bytes("file1", &vec![1u8; 2 * M]).unwrap();
                });
                handles.push(handle);
            }

            assert_eq!(c.get("file1").unwrap(), vec![1u8; 2 * M]);
            assert_eq!(c.get("file1").unwrap(), vec![1u8; 2 * M]);
            assert_eq!(c.get("file1").unwrap(), vec![1u8; 2 * M]);
            assert_eq!(c.get("file1").unwrap(), vec![1u8; 2 * M]);
            // Adding this third file should put the cache above the limit.
            c.insert_bytes("file3", &[3; 10]).unwrap();
            handles
                .into_iter()
                .for_each(|handle| handle.join().unwrap());
        }
    }
}
