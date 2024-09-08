use core::sync::atomic::{AtomicUsize, Ordering};

use super::sworndisk::Hba;
use crate::os::Mutex;
use crate::util::BitMap;
use crate::{prelude::*, Errno};

// Each chunk contains 1024 blocks
pub const CHUNK_SIZE: usize = 1024;

// Currently ChunkAllocTable is not response for Block Alloc, it just records
// alloced hba and count the number of valid blocks in the chunk, which is used for GC

pub struct ChunkAllocTable {
    chunk_id: usize,
    valid_block: AtomicUsize,
    bitmap: Mutex<BitMap>,
    nblocks: usize,
    // next_valid doesn't work now, in future work, we will alloc chunk first and then let ChunkAllocTable to alloc block in chunk
    next_valid: usize,
}

impl ChunkAllocTable {
    pub fn new(chunk_id: usize, nblocks: usize) -> Self {
        Self {
            valid_block: AtomicUsize::new(0),
            bitmap: Mutex::new(BitMap::repeat(false, nblocks)),
            nblocks,
            next_valid: 0,
            chunk_id,
        }
    }
    pub fn chunk_id(&self) -> usize {
        self.chunk_id
    }

    pub fn num_valid_blocks(&self) -> usize {
        self.valid_block.load(Ordering::Acquire)
    }

    pub fn free_size(&self) -> usize {
        self.nblocks - self.num_valid_blocks()
    }

    pub fn mark_alloc(&self, chunk_offset: Hba) -> Result<()> {
        if chunk_offset >= self.nblocks {
            return Err(Error::new(Errno::InvalidArgs));
        }
        let mut guard = self.bitmap.lock();
        guard.set(chunk_offset, true);
        self.valid_block.fetch_add(1, Ordering::Release);
        Ok(())
    }

    pub fn mark_alloc_batch(&self, hbas: &[Hba]) -> Result<()> {
        for &hba in hbas {
            self.mark_alloc(hba)?;
        }
        Ok(())
    }

    pub fn mark_dealloc(&self, chunk_offset: Hba) -> Result<()> {
        if chunk_offset >= self.nblocks {
            return Err(Error::new(Errno::InvalidArgs));
        }
        let mut guard = self.bitmap.lock();
        guard.set(chunk_offset, false);
        self.valid_block.fetch_sub(1, Ordering::Release);
        Ok(())
    }

    pub fn mark_dealloc_batch(&self, hbas: &[Hba]) -> Result<()> {
        for &hba in hbas {
            self.mark_dealloc(hba)?;
        }
        Ok(())
    }

    pub fn find_all_valid_blocks(&self) -> Vec<Hba> {
        let mut valid_blocks = Vec::new();
        let guard = self.bitmap.lock();
        for i in 0..self.nblocks {
            if guard.test_bit(i) {
                // transfer local offset to global offset
                let offset = self.chunk_id * CHUNK_SIZE + i;
                valid_blocks.push(offset);
            }
        }
        valid_blocks
    }

    pub fn find_all_free_blocks(&self) -> Vec<Hba> {
        let mut free_blocks = Vec::new();
        let guard = self.bitmap.lock();
        for i in 0..self.nblocks {
            if !guard.test_bit(i) {
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
        let chunk_alloc_table = ChunkAllocTable::new(0, 1024);
        chunk_alloc_table.mark_alloc(0).unwrap();
        assert_eq!(chunk_alloc_table.num_valid_blocks(), 1);
    }

    #[test]
    fn test_chunk_alloc_table_batch() {
        let chunk_alloc_table = ChunkAllocTable::new(0, 1024);
        chunk_alloc_table
            .mark_alloc_batch(&vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
            .unwrap();
        assert_eq!(chunk_alloc_table.num_valid_blocks(), 10);
    }

    #[test]
    fn test_chunk_alloc_table_invalid() {
        let chunk_alloc_table = ChunkAllocTable::new(0, 1024);
        assert!(chunk_alloc_table.mark_alloc(1024).is_err());
    }
}
