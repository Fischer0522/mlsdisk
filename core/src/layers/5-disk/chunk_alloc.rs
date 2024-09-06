use core::sync::atomic::{AtomicUsize, Ordering};

use super::sworndisk::Hba;
use crate::os::Mutex;
use crate::util::BitMap;
use crate::{prelude::*, Errno};

// Each chunk contains 1024 blocks
pub const CHUNK_SIZE: usize = 1024;

// Currently ChunkAllocTable is not response for Block Alloc, it just records
// alloced hba and count the number of valid blocks in the chunk, which is used for GC

// ChunkAllocTable is not thread safe, but we can use Mutex in AllocTable to make it thread safe
pub(super) struct ChunkAllocTable {
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
            valid_block: AtomicUsize::new(nblocks),
            bitmap: Mutex::new(BitMap::repeat(false, nblocks)),
            nblocks,
            next_valid: 0,
            chunk_id,
        }
    }

    pub fn num_valid_blocks(&self) -> usize {
        self.valid_block.load(Ordering::Acquire)
    }
    pub fn mark_alloc(&self, hba: Hba) -> Result<()> {
        let lower_bound = self.chunk_id * CHUNK_SIZE;
        let upper_bound = lower_bound + self.nblocks;
        if hba < lower_bound || hba >= upper_bound {
            return Err(Error::new(Errno::InvalidArgs));
        }

        let local_offset = hba - lower_bound;
        let mut guard = self.bitmap.lock();
        guard.set(local_offset, true);
        self.valid_block.fetch_sub(1, Ordering::Release);
        Ok(())
    }

    pub fn mark_alloc_batch(&mut self, hbas: &[Hba]) -> Result<()> {
        for &hba in hbas {
            self.mark_alloc(hba)?;
        }
        Ok(())
    }
}
