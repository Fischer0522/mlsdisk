use super::{
    block_alloc::BlockAlloc,
    chunk_alloc::ChunkAllocTable,
    sworndisk::{RecordKey, RecordValue},
};
use crate::{layers::disk::bio::BlockBuf, prelude::Result, Buf, BLOCK_SIZE};
use crate::{layers::lsm::TxLsmTree, BlockSet};
use std::sync::Arc;

pub trait VictimPolicy: Send + Sync {
    fn victim<'a>(&'a self, chunk_alloc_tables: &'a [ChunkAllocTable]) -> &'a ChunkAllocTable;
}

pub type VictimPolicyRef = Arc<dyn VictimPolicy>;

pub struct GreedyVictimPolicy;

impl VictimPolicy for GreedyVictimPolicy {
    // pick the non-empty chunk with the minimum number of valid blocks
    fn victim<'a>(&'a self, chunk_alloc_tables: &'a [ChunkAllocTable]) -> &'a ChunkAllocTable {
        let mut min_num_valid_blocks = usize::MAX;
        let mut victim = &chunk_alloc_tables[0];
        chunk_alloc_tables
            .iter()
            .enumerate()
            .for_each(|(i, alloc_table)| {
                if alloc_table.num_valid_blocks() != 0
                    && alloc_table.num_valid_blocks() < min_num_valid_blocks
                {
                    min_num_valid_blocks = alloc_table.num_valid_blocks();
                    victim = &chunk_alloc_tables[i];
                }
            });
        victim
    }
}

pub(super) struct GcWorker<D> {
    victim_policy: VictimPolicyRef,
    logical_block_table: TxLsmTree<RecordKey, RecordValue, D>,
    block_alloc: Arc<BlockAlloc<D>>,
    user_data_disk: Arc<D>,
}

impl<D: BlockSet + 'static> GcWorker<D> {
    pub fn new(
        victim_policy: VictimPolicyRef,
        logical_block_table: TxLsmTree<RecordKey, RecordValue, D>,
        block_alloc: Arc<BlockAlloc<D>>,
        user_data_disk: Arc<D>,
    ) -> Self {
        Self {
            victim_policy,
            logical_block_table,
            block_alloc,
            user_data_disk,
        }
    }

    pub fn do_gc(&self) -> Result<()> {
        let victim = self
            .victim_policy
            .victim(self.block_alloc.get_chunk_alloc_table_ref());
        if !self.trigger_gc(victim) {
            return Ok(());
        }

        // TODO: use tx to migrate data from victim to other chunk ?
        let victim_hbas = victim.find_all_valid_blocks();
        let mut target_hbas = Vec::new();
        let mut found_enough_blocks = false;
        for chunk in self.block_alloc.get_chunk_alloc_table_ref() {
            if chunk.free_size() == 0 || chunk.chunk_id() == victim.chunk_id() {
                continue;
            }
            let free_hbas = chunk.find_all_free_blocks();
            for hba in free_hbas {
                target_hbas.push(hba);
                if target_hbas.len() >= victim_hbas.len() {
                    found_enough_blocks = true;
                    break;
                }
            }
            if found_enough_blocks {
                break;
            }
        }
        // TODO: use batch to migrate data

        debug_assert!(victim_hbas.len() == target_hbas.len());
        for victim_hba in victim_hbas {
            let mut victim_block = Buf::alloc(1)?;
            self.user_data_disk
                .read(victim_hba, victim_block.as_mut())?;
            let target_hba = target_hbas.pop().unwrap();
            self.user_data_disk
                .write(target_hba, victim_block.as_ref())?;
        }

        // TODO: update logical block table

        Ok(())
    }

    fn trigger_gc(&self, chunk: &ChunkAllocTable) -> bool {
        // TODO: add more rules
        if chunk.num_valid_blocks() == 0 {
            return false;
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        layers::{
            bio::MemDisk,
            disk::{
                block_alloc::{AllocTable, BlockAlloc},
                chunk_alloc::{ChunkAllocTable, CHUNK_SIZE},
                gc::{GreedyVictimPolicy, VictimPolicy},
            },
            log::TxLogStore,
            lsm::{AsKV, SyncIdStore, TxEventListener, TxEventListenerFactory, TxLsmTree, TxType},
        },
        tx::Tx,
        AeadKey, RandomInit, SwornDisk,
    };
    use core::num::NonZeroUsize;
    use std::sync::Arc;

    #[test]
    fn greedy_victim_policy_test() {
        let chunk_alloc_tables = vec![
            ChunkAllocTable::new(0, 1024),
            ChunkAllocTable::new(1, 1024),
            ChunkAllocTable::new(2, 1024),
        ];
        let policy = GreedyVictimPolicy {};
        let victim = policy.victim(&chunk_alloc_tables);
        assert_eq!(victim.chunk_id(), 0);
        chunk_alloc_tables[0].mark_alloc(0).unwrap();
        let victim = policy.victim(&chunk_alloc_tables);
        assert_eq!(victim.chunk_id(), 1);
    }

    #[test]
    fn simple_data_migration() {
        let nblocks = 64 * CHUNK_SIZE;
        let mem_disk = MemDisk::create(nblocks).unwrap();
        let greedy_victim_policy = GreedyVictimPolicy {};
        let root_key = AeadKey::random();

        let disk = SwornDisk::create(mem_disk, root_key, None).unwrap();
        let gc_worker = disk
            .create_gc_worker(Arc::new(greedy_victim_policy))
            .unwrap();
        // doesn't trigger gc
        gc_worker.do_gc().unwrap();

        let content: Vec<u8> = vec![1; BLOCK_SIZE];
        let mut buf = Buf::alloc(1).unwrap();
        buf.as_mut_slice().copy_from_slice(&content);
        disk.write(0, buf.as_ref()).unwrap();
        disk.sync().unwrap();

        gc_worker.do_gc().unwrap();
        // after gc, the block at offset 0 should be migrated to another chunk
        // TODO: check the content of the block after support remapping LBA -> HBA
    }
}
