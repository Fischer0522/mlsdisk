use super::block_alloc::{AllocDiff, AllocTable};
use super::sworndisk::Hba;
use crate::layers::log::{TxLog, TxLogStore};
use crate::os::{BTreeMap, Mutex};
use crate::util::BitMap;
use crate::{prelude::*, BlockSet, Errno};
use core::sync::atomic::{AtomicUsize, Ordering};
// Each chunk contains 1024 blocks
pub const CHUNK_SIZE: usize = 1024;
pub type ChunkId = usize;

// Currently ChunkAllocTable is not response for Block Alloc, it just records
// alloced hba and count the number of valid blocks in the chunk, which is used for GC

// Persistent data: ChunkId, valid_block, free_space
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

    pub fn clear_chunk(&self) {
        let mut guard = self.bitmap.lock();
        let lower_bound = self.chunk_id * CHUNK_SIZE;
        let upper_bound = lower_bound + self.nblocks;
        for idx in lower_bound..upper_bound {
            guard.set(idx, true);
        }
        self.valid_block.store(self.nblocks, Ordering::Release);
        self.free_space.store(self.nblocks, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use hashbrown::HashSet;

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

    #[test]
    fn find_free_and_allocated_blocks() {
        let bitmap = Arc::new(Mutex::new(BitMap::repeat(true, 3 * 1024)));
        {
            let mut guard = bitmap.lock();
            guard.set(0, false);
            guard.set(1, false);

            guard.set(1024, false);
            guard.set(1025, false);
            guard.set(1026, false);
        }

        let chunk_alloc_tables = vec![
            ChunkInfo::new(0, 1024, bitmap.clone()),
            ChunkInfo::new(1, 1024, bitmap.clone()),
            ChunkInfo::new(2, 1024, bitmap.clone()),
        ];

        assert_eq!(chunk_alloc_tables[0].find_all_free_blocks().len(), 1022);
        assert_eq!(chunk_alloc_tables[1].find_all_free_blocks().len(), 1021);
        assert_eq!(chunk_alloc_tables[2].find_all_free_blocks().len(), 1024);

        let free_set0: HashSet<Hba> = chunk_alloc_tables[0]
            .find_all_free_blocks()
            .into_iter()
            .collect();
        let free_set1: HashSet<Hba> = chunk_alloc_tables[1]
            .find_all_free_blocks()
            .into_iter()
            .collect();
        let free_set2: HashSet<Hba> = chunk_alloc_tables[2]
            .find_all_free_blocks()
            .into_iter()
            .collect();
        assert_eq!(free_set0.len(), 1022);
        assert_eq!(free_set1.len(), 1021);
        assert_eq!(free_set2.len(), 1024);
        assert!(!free_set0.contains(&0));
        assert!(!free_set0.contains(&1));

        assert!(!free_set1.contains(&1024));
        assert!(!free_set1.contains(&1025));
        assert!(!free_set1.contains(&1026));

        assert_eq!(chunk_alloc_tables[0].find_all_allocated_blocks().len(), 2);
        assert_eq!(chunk_alloc_tables[1].find_all_allocated_blocks().len(), 3);
        assert_eq!(chunk_alloc_tables[2].find_all_allocated_blocks().len(), 0);
    }
}
