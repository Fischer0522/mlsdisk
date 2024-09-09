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
    valid_block: AtomicUsize,
    // bitmap of blocks in the chunk
    bitmap: Mutex<BitMap>,
    nblocks: usize,
    // next_valid doesn't work now, in future work, we may alloc chunk first and then let ChunkAllocTable to alloc block in chunk
    _next_valid: usize,
}

impl ChunkInfo {
    pub fn new(chunk_id: ChunkId, nblocks: usize) -> Self {
        Self {
            valid_block: AtomicUsize::new(nblocks),
            bitmap: Mutex::new(BitMap::repeat(true, nblocks)),
            nblocks,
            _next_valid: 0,
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
        self.bitmap.lock().count_ones()
    }

    pub fn num_invalid_blocks(&self) -> usize {
        self.nblocks - self.num_valid_blocks()
    }

    pub fn mark_alloc(&self, chunk_offset: Hba) -> Result<()> {
        if chunk_offset >= self.nblocks {
            return Err(Error::with_msg(Errno::InvalidArgs, "Hba is out of range"));
        }
        let mut guard = self.bitmap.lock();
        debug_assert!(guard.test_bit(chunk_offset));
        guard.set(chunk_offset, false);
        Ok(())
    }

    pub fn mark_alloc_batch(&self, hbas: &[Hba]) -> Result<()> {
        for &hba in hbas {
            self.mark_alloc(hba)?;
        }
        Ok(())
    }

    pub fn mark_deallocated(&self, chunk_offset: Hba) -> Result<()> {
        if chunk_offset >= self.nblocks {
            return Err(Error::new(Errno::InvalidArgs));
        }
        let mut guard = self.bitmap.lock();
        debug_assert!(
            !guard.test_bit(chunk_offset),
            "block {} hasn't been allocated",
            chunk_offset
        );
        guard.set(chunk_offset, true);
        self.valid_block.fetch_sub(1, Ordering::Release);
        Ok(())
    }

    pub fn mark_deallocated_batch(&self, hbas: &[Hba]) -> Result<()> {
        for &hba in hbas {
            self.mark_deallocated(hba)?;
        }
        Ok(())
    }

    // All blocks that have been marked as allocated
    pub fn find_all_allocated_blocks(&self) -> Vec<Hba> {
        let mut valid_blocks = Vec::new();
        let guard = self.bitmap.lock();
        for i in 0..self.nblocks {
            if !guard.test_bit(i) {
                // transfer local offset to global offset
                let offset = self.chunk_id * CHUNK_SIZE + i;
                valid_blocks.push(offset);
            }
        }
        valid_blocks
    }

    // Find empty blocks and blocks that have been marked as deallocated,
    // this function is used for choosing target blocks in GC
    pub fn find_all_free_blocks(&self) -> Vec<Hba> {
        let mut free_blocks = Vec::new();
        let guard = self.bitmap.lock();
        for i in 0..self.nblocks {
            if guard.test_bit(i) {
                let offset = self.chunk_id * CHUNK_SIZE + i;
                free_blocks.push(offset);
            }
        }
        free_blocks
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_alloc_table() {
        let chunk_alloc_table = ChunkInfo::new(0, 1024);
        chunk_alloc_table.mark_alloc(0).unwrap();
        assert_eq!(chunk_alloc_table.num_valid_blocks(), 1024);
    }

    #[test]
    fn test_chunk_alloc_table_batch() {
        let chunk_alloc_table = ChunkInfo::new(0, 1024);
        chunk_alloc_table
            .mark_alloc_batch(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
            .unwrap();
        assert_eq!(chunk_alloc_table.num_valid_blocks(), 1024);
        assert_eq!(chunk_alloc_table.free_space(), 1014);
        chunk_alloc_table.mark_deallocated(0).unwrap();
        assert_eq!(chunk_alloc_table.num_valid_blocks(), 1023);
        assert_eq!(chunk_alloc_table.free_space(), 1015);
    }

    #[test]
    fn test_chunk_alloc_table_invalid() {
        let chunk_alloc_table = ChunkInfo::new(0, 1024);
        assert!(chunk_alloc_table.mark_alloc(1024).is_err());
    }
}
