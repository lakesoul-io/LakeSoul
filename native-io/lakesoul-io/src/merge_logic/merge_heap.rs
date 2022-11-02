use heapless::binary_heap::{BinaryHeap, Max};

pub struct MergeHeap<T, K, const N: usize>{
    binary_heap: BinaryHeap<T, K, N>,
}

impl<T, K, const N: usize> MergeHeap<T, K, N>{
    pub fn new()->MergeHeap<T, K, N> {
        MergeHeap{
            binary_heap:BinaryHeap::new()
        }
    }
}
