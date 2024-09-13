use super::{
    block_alloc::{AllocTable, BlockAlloc},
    chunk_alloc::{ChunkId, ChunkInfo},
    sworndisk::{Hba, RecordKey, RecordValue},
};
use crate::os::Arc;
use crate::{layers::lsm::TxLsmTree, BlockSet};
use crate::{
    layers::{
        disk::{bio::BlockBuf, block_alloc},
        log::TxLogStore,
    },
    prelude::Result,
    Buf, BLOCK_SIZE,
};
use core::{
    sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    time::Duration,
    usize,
};
use log::debug;
// Default gc interval time is 30 seconds
const DEFAULT_GC_INTERVAL_TIME: std::time::Duration = std::time::Duration::from_secs(30);
const GC_WATERMARK: usize = 16;
const DEFAULT_GC_THRESHOLD: f64 = 0.2;

pub struct Victim {
    chunk_id: ChunkId,
    blocks: Vec<Hba>,
}

pub trait VictimPolicy: Send + Sync {
    fn pick_victim(&self, chunk_alloc_tables: &[ChunkInfo], threshold: f64) -> Option<Victim>;
}

pub type VictimPolicyRef = Arc<dyn VictimPolicy>;

pub struct GreedyVictimPolicy;

impl VictimPolicy for GreedyVictimPolicy {
    // pick the chunk with the maximum number of invalid blocks
    fn pick_victim(&self, chunk_alloc_tables: &[ChunkInfo], threshold: f64) -> Option<Victim> {
        let mut max_num_invalid_blocks = usize::MIN;
        let mut victim = None;
        chunk_alloc_tables
            .iter()
            .enumerate()
            .for_each(|(i, alloc_table)| {
                let invalid_block_fraction =
                    alloc_table.num_invalid_blocks() as f64 / alloc_table.nblocks() as f64;
                if invalid_block_fraction > threshold
                    && alloc_table.num_invalid_blocks() > max_num_invalid_blocks
                {
                    max_num_invalid_blocks = alloc_table.num_invalid_blocks();
                    victim = Some(Victim {
                        chunk_id: i,
                        blocks: vec![],
                    });
                }
            });
        victim.map(|mut victim| {
            let victim_chunk = &chunk_alloc_tables[victim.chunk_id];
            victim.blocks = victim_chunk.find_all_allocated_blocks();
            victim
        })
    }
}

pub struct LoopScanVictimPolicy {
    cursor: AtomicUsize,
}

impl VictimPolicy for LoopScanVictimPolicy {
    fn pick_victim(&self, chunk_info_tables: &[ChunkInfo], threshold: f64) -> Option<Victim> {
        let last_cursor = self.cursor.load(Ordering::Relaxed);
        let mut cursor = last_cursor;
        loop {
            cursor = (cursor + 1) % chunk_info_tables.len();
            if cursor == last_cursor {
                return None;
            }
            let chunk = &chunk_info_tables[cursor];
            let invalid_block_fraction = chunk.num_invalid_blocks() as f64 / chunk.nblocks() as f64;
            if invalid_block_fraction > threshold {
                self.cursor.store(cursor, Ordering::Release);
                return Some(Victim {
                    chunk_id: cursor,
                    blocks: chunk.find_all_allocated_blocks(),
                });
            }
        }
    }
}

pub(super) struct GcWorker<D> {
    victim_policy: VictimPolicyRef,
    logical_block_table: TxLsmTree<RecordKey, RecordValue, D>,
    block_validity_table: Arc<AllocTable>,
    tx_log_store: Arc<TxLogStore<D>>,
    user_data_disk: Arc<D>,
    doing_background_gc: AtomicBool,
}

impl<D: BlockSet + 'static> GcWorker<D> {
    pub fn new(
        victim_policy: VictimPolicyRef,
        logical_block_table: TxLsmTree<RecordKey, RecordValue, D>,
        tx_log_store: Arc<TxLogStore<D>>,
        block_validity_table: Arc<AllocTable>,
        user_data_disk: Arc<D>,
    ) -> Self {
        Self {
            victim_policy,
            logical_block_table,
            block_validity_table,
            tx_log_store,
            user_data_disk,
            doing_background_gc: AtomicBool::new(false),
        }
    }

    pub fn run(&self) -> Result<()> {
        loop {
            self.doing_background_gc.store(true, Ordering::Release);
            self.background_gc()?;
            self.doing_background_gc.store(false, Ordering::Release);
            // FIXME: use a cross-platform sleep function
            std::thread::sleep(DEFAULT_GC_INTERVAL_TIME);
        }
    }

    pub fn foreground_gc(&self) -> Result<()> {
        if self.doing_background_gc.load(Ordering::Acquire) {
            debug!("Background GC is running, skip foreground GC");
            return Ok(());
        }
        let victim = self.victim_policy.pick_victim(
            self.block_validity_table.get_chunk_info_table_ref(),
            DEFAULT_GC_THRESHOLD,
        );
        if !self.trigger_gc(victim.as_ref()) {
            return Ok(());
        }
        // Safety: if victim is none, the function will return early
        self.clean_and_migrate_data(victim.unwrap())?;
        Ok(())
    }

    // TODO: use tx to migrate data from victim to other chunk and update metadata
    pub fn background_gc(&self) -> Result<()> {
        let mut chunk_cnt = 0;
        for _ in 0..GC_WATERMARK {
            let victim = self.victim_policy.pick_victim(
                self.block_validity_table.get_chunk_info_table_ref(),
                DEFAULT_GC_THRESHOLD,
            );

            // Generally, the VictimPolicy will pick a victim chunk that most needs GC
            // if it returned None, it means there is no chunk needs GC, we can return
            let Some(victim) = victim else {
                break;
            };
            chunk_cnt += 1;
            self.clean_and_migrate_data(victim)?;
        }
        debug!("Background GC succeed, freed {} chunks", chunk_cnt);
        Ok(())
    }

    pub fn clean_and_migrate_data(&self, victim: Victim) -> Result<()> {
        let victim_chunk = &self.block_validity_table.get_chunk_info_table_ref()[victim.chunk_id];
        // TODO: use tx to migrate data from victim to other chunk ?
        let victim_hbas = victim_chunk.find_all_allocated_blocks();
        let mut target_hbas = Vec::new();
        let mut found_enough_blocks = false;
        for chunk in self.block_validity_table.get_chunk_info_table_ref() {
            if chunk.free_space() == 0 || chunk.chunk_id() == victim_chunk.chunk_id() {
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
        for (victim_hba, target_hba) in victim_hbas.iter().zip(target_hbas.clone()) {
            let mut victim_block = Buf::alloc(1)?;
            self.user_data_disk
                .read(*victim_hba, victim_block.as_mut())?;
            self.user_data_disk
                .write(target_hba, victim_block.as_ref())?;
        }

        let block_alloc =
            BlockAlloc::new(self.block_validity_table.clone(), self.tx_log_store.clone());

        victim_hbas
            .into_iter()
            .try_for_each(|hba| block_alloc.dealloc_block(hba))?;

        target_hbas
            .into_iter()
            .try_for_each(|hba| block_alloc.alloc_block(hba))?;

        block_alloc.update_alloc_table();

        // TODO: update logical block table
        todo!()
    }

    fn trigger_gc(&self, victim: Option<&Victim>) -> bool {
        if victim.is_none() {
            return false;
        }
        debug!(
            "Triggered background GC, victim chunk: {}",
            victim.unwrap().chunk_id
        );
        true
    }
}

#[cfg(test)]
mod tests {
    use spin::Mutex;

    use super::*;
    use crate::{
        layers::{
            bio::MemDisk,
            disk::{
                block_alloc::{AllocTable, BlockAlloc},
                chunk_alloc::{ChunkInfo, CHUNK_SIZE},
                gc::{GreedyVictimPolicy, VictimPolicy},
            },
            log::TxLogStore,
            lsm::{AsKV, SyncIdStore, TxEventListener, TxEventListenerFactory, TxLsmTree, TxType},
        },
        tx::Tx,
        util::BitMap,
        AeadKey, RandomInit, SwornDisk,
    };
    use core::num::NonZeroUsize;
    use std::sync::Arc;

    #[test]
    fn greedy_victim_policy_test() {
        let bitmap = Arc::new(Mutex::new(BitMap::repeat(true, 3 * 1024)));
        let chunk_alloc_tables = vec![
            ChunkInfo::new(0, 1024, bitmap.clone()),
            ChunkInfo::new(1, 1024, bitmap.clone()),
            ChunkInfo::new(2, 1024, bitmap.clone()),
        ];
        let policy = GreedyVictimPolicy {};
        let victim = policy.pick_victim(&chunk_alloc_tables, 0.);
        assert!(victim.is_none());
        chunk_alloc_tables[1].mark_alloc();
        // After dealloc, there will be an invalid block in the chunk, chunk 1 will be the victim
        chunk_alloc_tables[1].mark_deallocated();
        let victim = policy.pick_victim(&chunk_alloc_tables, 0.);
        assert_eq!(victim.unwrap().chunk_id, 1);
    }

    #[test]
    fn threshold_test() {
        let bitmap = Arc::new(Mutex::new(BitMap::repeat(true, 3 * 1024)));
        let chunk_alloc_tables = vec![
            ChunkInfo::new(0, 1024, bitmap.clone()),
            ChunkInfo::new(1, 1024, bitmap.clone()),
            ChunkInfo::new(2, 1024, bitmap.clone()),
        ];
        let policy = GreedyVictimPolicy {};
        let threshold = 0.2;
        let victim = policy.pick_victim(&chunk_alloc_tables, threshold);
        assert!(victim.is_none());

        // deallocate enough blocks to pick the chunk as victim
        for _ in 0..((2 * CHUNK_SIZE) as f64 * threshold) as usize {
            chunk_alloc_tables[1].mark_alloc();
            chunk_alloc_tables[1].mark_deallocated();
        }
        let victim = policy.pick_victim(&chunk_alloc_tables, threshold);
        assert_eq!(victim.unwrap().chunk_id, 1);
    }

    #[test]
    fn find_free_blocks() {
        todo!()
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
        gc_worker.background_gc().unwrap();

        let content: Vec<u8> = vec![1; BLOCK_SIZE];
        let mut buf = Buf::alloc(1).unwrap();
        buf.as_mut_slice().copy_from_slice(&content);
        disk.write(0, buf.as_ref()).unwrap();
        disk.sync().unwrap();

        gc_worker.background_gc().unwrap();
        // after gc, the block at offset 0 should be migrated to another chunk
        // TODO: check the content of the block after support remapping LBA -> HBA
    }
}
