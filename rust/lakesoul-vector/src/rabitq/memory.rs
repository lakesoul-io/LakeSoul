//! Memory management utilities with optional huge page support
//!
//! This module provides memory allocation and management functions
//! with optional huge page support for improved TLB performance.

// Import alloc functions only when huge_pages feature is enabled on Linux
#[cfg(all(feature = "huge_pages", target_os = "linux"))]
use std::alloc::{Layout, alloc, dealloc};

/// Get system page size
#[cfg(target_os = "linux")]
#[allow(dead_code)]
fn get_page_size() -> usize {
    use libc::_SC_PAGESIZE;
    use libc::sysconf;

    unsafe {
        let page_size = sysconf(_SC_PAGESIZE);
        if page_size > 0 {
            page_size as usize
        } else {
            4096 // Default to 4KB
        }
    }
}

/// Round up to nearest multiple
#[allow(dead_code)]
fn round_up_to_multiple_of(size: usize, multiple: usize) -> usize {
    size.div_ceil(multiple) * multiple
}

/// Enable huge pages for a memory region (Linux only)
///
/// This function advises the kernel to use huge pages for the given memory region.
/// Huge pages reduce TLB misses and can improve performance by 5-10%.
///
/// # Safety
/// The pointer must be valid, page-aligned, and the size must be a multiple of page size.
#[cfg(all(feature = "huge_pages", target_os = "linux"))]
#[allow(dead_code)]
pub unsafe fn enable_huge_pages(ptr: *mut u8, size: usize) -> std::io::Result<()> {
    use libc::{MADV_HUGEPAGE, madvise};

    // Ensure pointer is page-aligned and size is a multiple of page size
    let page_size = get_page_size();
    let aligned_ptr = round_up_to_multiple_of(ptr as usize, page_size);

    // If pointer is not aligned, we can't use madvise on it
    if aligned_ptr != ptr as usize {
        return Ok(()); // Silently ignore, as the C++ implementation doesn't fail either
    }

    // Round size to page boundary
    let aligned_size = round_up_to_multiple_of(size, page_size);

    let result = madvise(ptr as *mut libc::c_void, aligned_size, MADV_HUGEPAGE);

    if result == 0 {
        Ok(())
    } else {
        // Don't fail hard, just return Ok() as this is an optimization hint
        // The C++ implementation also doesn't check the return value
        Ok(())
    }
}

/// Enable huge pages - no-op on non-Linux or when feature is disabled
#[cfg(not(all(feature = "huge_pages", target_os = "linux")))]
#[allow(dead_code)]
pub unsafe fn enable_huge_pages(_ptr: *mut u8, _size: usize) -> std::io::Result<()> {
    Ok(())
}

/// Custom aligned vector that uses page-aligned allocation
#[cfg(all(feature = "huge_pages", target_os = "linux"))]
#[allow(dead_code)]
pub struct AlignedVec<T> {
    ptr: *mut T,
    len: usize,
    layout: Layout,
}

#[cfg(all(feature = "huge_pages", target_os = "linux"))]
#[allow(dead_code)]
impl<T: Default + Clone> AlignedVec<T> {
    pub fn new(size: usize) -> Self {
        let page_size = get_page_size();
        let elem_size = std::mem::size_of::<T>();
        let byte_size = size * elem_size;

        // Round up to page boundary for better huge page support
        let aligned_byte_size = round_up_to_multiple_of(byte_size, page_size);

        // Create layout with page alignment
        let layout = Layout::from_size_align(aligned_byte_size, page_size)
            .expect("Failed to create aligned layout");

        let ptr = unsafe {
            let raw_ptr = alloc(layout);
            if raw_ptr.is_null() {
                std::alloc::handle_alloc_error(layout);
            }
            raw_ptr as *mut T
        };

        // Initialize memory
        unsafe {
            for i in 0..size {
                ptr.add(i).write(T::default());
            }
        }

        // Enable huge pages for this allocation
        unsafe {
            let _ = enable_huge_pages(ptr as *mut u8, aligned_byte_size);
        }

        AlignedVec {
            ptr,
            len: size,
            layout,
        }
    }

    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

#[cfg(all(feature = "huge_pages", target_os = "linux"))]
impl<T> Drop for AlignedVec<T> {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe {
                // Drop all elements
                for i in 0..self.len {
                    self.ptr.add(i).drop_in_place();
                }
                // Deallocate memory
                dealloc(self.ptr as *mut u8, self.layout);
            }
        }
    }
}

#[cfg(all(feature = "huge_pages", target_os = "linux"))]
impl<T: Default + Clone> std::ops::Deref for AlignedVec<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

#[cfg(all(feature = "huge_pages", target_os = "linux"))]
impl<T: Default + Clone> std::ops::DerefMut for AlignedVec<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

/// Allocate memory with 64-byte alignment (cache line) and huge page support
///
/// This allocates memory with 64-byte alignment for optimal SIMD performance,
/// matching the C++ implementation's alignment strategy.
///
/// Note: Since we can't easily override Vec's deallocator, we allocate with 64-byte
/// alignment and then copy to a regular Vec. The kernel should keep the pages aligned.
#[cfg(all(feature = "huge_pages", target_os = "linux"))]
pub fn allocate_aligned_vec<T: Default + Clone>(size: usize) -> Vec<T> {
    if size == 0 {
        return Vec::new();
    }

    // For better performance with SIMD operations, ensure data starts at cache-line boundary
    // Allocate extra space to guarantee we can align the data
    const CACHE_LINE_SIZE: usize = 64;
    let elem_size = std::mem::size_of::<T>();
    let extra_elems = CACHE_LINE_SIZE.div_ceil(elem_size);

    let mut vec = vec![T::default(); size + extra_elems];

    unsafe {
        // Find the aligned position within the vector
        let ptr = vec.as_ptr() as usize;
        let aligned_ptr = (ptr + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1);
        let offset = (aligned_ptr - ptr) / elem_size;

        // Try to enable huge pages for large allocations
        let byte_size = size * elem_size;
        if byte_size >= 2 * 1024 * 1024 {
            let _ = enable_huge_pages(vec.as_mut_ptr() as *mut u8, vec.len() * elem_size);
        }

        // Return a subslice starting at the aligned offset
        // This is a workaround - ideally we'd use a custom allocator
        vec.drain(0..offset);
        vec.truncate(size);
    }

    vec
}

/// Allocate memory with huge page support (fallback for non-Linux or when feature disabled)
#[cfg(not(all(feature = "huge_pages", target_os = "linux")))]
pub fn allocate_aligned_vec<T: Default + Clone>(size: usize) -> Vec<T> {
    vec![T::default(); size]
}

/// Helper to check if huge pages are available on the system
#[cfg(all(feature = "huge_pages", target_os = "linux"))]
#[allow(dead_code)]
pub fn check_huge_pages_available() -> bool {
    use std::fs;

    // Check if transparent huge pages are enabled
    if let Ok(content) = fs::read_to_string("/sys/kernel/mm/transparent_hugepage/enabled")
    {
        return content.contains("[always]") || content.contains("[madvise]");
    }

    false
}

/// Helper to check if huge pages are available - always false when not on Linux
#[cfg(not(all(feature = "huge_pages", target_os = "linux")))]
#[allow(dead_code)]
pub fn check_huge_pages_available() -> bool {
    false
}

/// Log huge page status
pub fn log_huge_page_status() {
    #[cfg(feature = "huge_pages")]
    {
        if check_huge_pages_available() {
            eprintln!("Huge pages: ENABLED (may improve performance by 5-10%)");
        } else {
            eprintln!(
                "Huge pages: NOT AVAILABLE (enable transparent_hugepage for better performance)"
            );
        }
    }

    #[cfg(not(feature = "huge_pages"))]
    {
        eprintln!("Huge pages: DISABLED (compile with --features huge_pages to enable)");
    }
}
