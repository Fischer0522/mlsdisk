use log::debug;
use pod::Pod;

use super::sworndisk::{Hba, Lba, RecordKey, RecordValue};
use crate::layers::crypto::{Key, Mac};
use crate::layers::lsm::ColumnFamily;
use crate::prelude::{Error, Result, Vec};
use crate::{
    layers::lsm::TxLsmTree,
    os::{BTreeMap, HashMap, Mutex},
    BlockSet,
};
pub(super) struct DeallocTable {
    dealloc_table: Mutex<HashMap<Lba, Hba>>,
}

impl DeallocTable {
    pub fn new() -> Self {
        Self {
            dealloc_table: Mutex::new(HashMap::new()),
        }
    }

    pub fn has_deallocated(&self, lba: Lba) -> bool {
        let dealloc_table = self.dealloc_table.lock();
        dealloc_table.contains_key(&lba)
    }
    pub fn finish_deallocated(&self, lba: Lba) {
        let mut dealloc_table = self.dealloc_table.lock();
        dealloc_table.remove(&lba);
    }

    pub fn mark_deallocated(&self, lba: Lba, hba: Hba) {
        let mut dealloc_table = self.dealloc_table.lock();
        dealloc_table.insert(lba, hba);
    }

    // TODO: move this function to GcWorker
    // After data migration in GC task, we need:
    // 1. update the hba of the records in lsm tree
    // 2. update the reverse index table, record the old hba of the migrated blocks and insert the new hba -> lba mapping
    // 3. insert the lba -> old hba mapping into the dealloc table to prevent double deallocation in compaction
    pub fn remap_index_batch<D: BlockSet + 'static>(
        &self,
        remapped_hbas: Vec<(Hba, Hba)>,
        tx_lsm_tree: &TxLsmTree<RecordKey, RecordValue, D>,
    ) -> Result<()> {
        remapped_hbas
            .into_iter()
            .try_for_each(|(old_hba, new_hba)| {
                // Get the lba of the old hba
                // Safety: hba should exist in index table, otherwise it means system is inconsistent
                let key = RecordKey { lba: old_hba };
                let lba = tx_lsm_tree
                    .get(&key, Some(ColumnFamily::ReverseIndex))
                    .map(|lba| lba.hba)
                    .expect("hba should exist in index table");
                let record_key = RecordKey { lba };

                // get mac and key of the old hba record
                // Safety: hba should exist in lsm tree, otherwise it means system is inconsistent
                let mut record_value = tx_lsm_tree
                    .get(&record_key, None)
                    .expect("record key should exist in lsm tree");

                // Update the hba of the record but keep the key and mac unchanged
                // This will trigger deallocation of the old hba in MemTable
                record_value.hba = new_hba;

                // write the record back to lsm tree
                tx_lsm_tree.put(record_key, record_value, None)?;

                let reverse_index_key = RecordKey { lba: new_hba };

                // update the reverse index table
                let reverse_index_value = RecordValue {
                    hba: lba,
                    key: Key::new_uninit(),
                    mac: Mac::new_uninit(),
                };
                tx_lsm_tree.put(
                    reverse_index_key,
                    reverse_index_value,
                    Some(ColumnFamily::ReverseIndex),
                )?;
                Ok::<_, Error>(())
            })?;
        Ok::<_, Error>(())
    }

    pub fn recover<D: BlockSet + 'static>(
        _tx_lsm_tree: &TxLsmTree<RecordKey, RecordValue, D>,
    ) -> Result<Self> {
        todo!()
    }
}
