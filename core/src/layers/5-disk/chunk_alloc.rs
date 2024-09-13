use core::sync::atomic::{AtomicUsize, Ordering};

use super::sworndisk::Hba;
use crate::os::Mutex;
use crate::util::BitMap;
use crate::{prelude::*, Errno};

// Each chunk contains 1024 blocks
pub const CHUNK_SIZE: usize = 1024;
pub type ChunkId = usize;

// Currently ChunkAllocTable is not response for Block Alloc, it just records
// alloced hba and count the number of valid blocks in the chunk, which is used for GC

pub struct ChunkInfo {
    chunk_id: ChunkId,
    // valid_block statistic all empty blocks and blocks that have been marked as allocated,
    // it's used for GC to choose victim chunk, and is initialized with nblocks when chunk is created
    // when a block is deallocated, valid_block is decremented
    // TODO: Currently, valid_block is only associated with block deallocation, we need to consider block reallocation
    valid_block: AtomicUsize,
    // bitmap of blocks in the chunk
    bitmap: Arc<Mutex<BitMap>>,
    nblocks: usize,
    free_space: AtomicUsize,
}

impl ChunkInfo {
    pub fn new(chunk_id: ChunkId, nblocks: usize, bitmap: Arc<Mutex<BitMap>>) -> Self {
        Self {
            valid_block: AtomicUsize::new(nblocks),
            bitmap,
            nblocks,
            free_space: AtomicUsize::new(nblocks),
            chunk_id,
        }
    }
    pub fn chunk_id(&self) -> ChunkId {
        self.chunk_id
    }
    pub fn nblocks(&self) -> usize {
        self.nblocks
    }

    // num_valid_blocks is only related to block deallcation
    pub fn num_valid_blocks(&self) -> usize {
        self.valid_block.load(Ordering::Acquire)
    }

    // free_space is related to both block allocation and deallocation,
    // represent the number of empty blocks and deallocated blocks in the chunk
    pub fn free_space(&self) -> usize {
        self.free_space.load(Ordering::Acquire)
    }

    pub fn num_invalid_blocks(&self) -> usize {
        self.nblocks - self.num_valid_blocks()
    }

    pub fn mark_alloc(&self) {
        self.free_space.fetch_sub(1, Ordering::Release);
    }

    pub fn mark_alloc_batch(&self, nblocks: usize) {
        self.free_space.fetch_sub(nblocks, Ordering::Release);
    }

    pub fn mark_deallocated(&self) {
        self.free_space.fetch_add(1, Ordering::Release);
        self.valid_block.fetch_sub(1, Ordering::Release);
    }

    pub fn mark_deallocated_batch(&self, nblocks: usize) {
        self.free_space.fetch_add(nblocks, Ordering::Release);
        self.valid_block.fetch_sub(nblocks, Ordering::Release);
    }

    // All blocks that have been marked as allocated
    pub fn find_all_allocated_blocks(&self) -> Vec<Hba> {
        let lower_bound = self.chunk_id * CHUNK_SIZE;
        let upper_bound = lower_bound + self.nblocks;
        let mut valid_blocks = Vec::new();
        let guard = self.bitmap.lock();
        for idx in lower_bound..upper_bound {
            if !guard.test_bit(idx) {
                valid_blocks.push(idx);
            }
        }
        valid_blocks
    }

    // Find empty blocks and blocks that have been marked as deallocated,
    // this function is used for choosing target blocks in GC
    pub fn find_all_free_blocks(&self) -> Vec<Hba> {
        let lower_bound = self.chunk_id * CHUNK_SIZE;
        let upper_bound = lower_bound + self.nblocks;
        let mut free_blocks = Vec::new();
        let guard = self.bitmap.lock();
        for idx in lower_bound..upper_bound {
            if guard.test_bit(idx) {
                free_blocks.push(idx);
            }
        }
        free_blocks
    }
}

pub struct ChunkAlloc {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_alloc_table() {
        let bitmap = Arc::new(Mutex::new(BitMap::repeat(true, 1024)));
        let chunk_alloc_table = ChunkInfo::new(0, 1024, bitmap);
        chunk_alloc_table.mark_alloc();
        assert_eq!(chunk_alloc_table.num_valid_blocks(), 1024);
    }

    #[test]
    fn test_chunk_alloc_table_batch() {
        let bitmap = Arc::new(Mutex::new(BitMap::repeat(true, 20 * 1024)));
        let chunk_alloc_table = ChunkInfo::new(0, 1024, bitmap);
        chunk_alloc_table.mark_alloc_batch(10);
        assert_eq!(chunk_alloc_table.num_valid_blocks(), 1024);
        assert_eq!(chunk_alloc_table.free_space(), 1014);
        chunk_alloc_table.mark_deallocated();
        assert_eq!(chunk_alloc_table.num_valid_blocks(), 1023);
        assert_eq!(chunk_alloc_table.free_space(), 1015);
    }
}
