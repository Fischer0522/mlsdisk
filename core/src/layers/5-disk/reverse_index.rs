use super::sworndisk::{Hba, Lba, RecordKey, RecordValue};
use crate::prelude::{Error, Result, Vec};
use crate::{
    layers::lsm::TxLsmTree,
    os::{BTreeMap, HashMap, Mutex},
    BlockSet,
};
pub(super) struct ReverseIndexTable {
    index_table: Mutex<BTreeMap<Hba, Lba>>,
    dealloc_table: Mutex<HashMap<Lba, Hba>>,
}

impl ReverseIndexTable {
    pub fn new() -> Self {
        Self {
            index_table: Mutex::new(BTreeMap::new()),
            dealloc_table: Mutex::new(HashMap::new()),
        }
    }

    pub fn get_lba(&self, old_hba: &Hba) -> Lba {
        let index_table = self.index_table.lock();
        index_table
            .get(&old_hba)
            .map(|lba| *lba)
            .expect("hba should exist in index table")
    }

    pub fn update_index_batch(&self, records: impl Iterator<Item = (RecordKey, RecordValue)>) {
        let mut index_table = self.index_table.lock();
        records.for_each(|(key, value)| {
            index_table.insert(value.hba, key.lba);
        });
    }

    // TODO use btree_map::range to get hbas from index table in batch
    // After data migration in GC task, we need:
    // 1. update the hba of the records in lsm tree
    // 2. update the reverse index table, record the old hba of the migrated blocks and insert the new hba -> lba mapping
    // 3. insert the lba -> old hba mapping into the dealloc table to prevent double deallocation in compaction
    pub fn remap_index_batch<D: BlockSet + 'static>(
        &self,
        remapped_hbas: Vec<(Hba, Hba)>,
        discard_hbas: Vec<(Lba, Hba)>,
        tx_lsm_tree: &TxLsmTree<RecordKey, RecordValue, D>,
    ) -> Result<()> {
        let mut index_table = self.index_table.lock();
        let mut dealloc_table = self.dealloc_table.lock();

        discard_hbas.into_iter().for_each(|(lba, hba)| {
            dealloc_table.insert(lba, hba);
        });
        remapped_hbas
            .into_iter()
            .try_for_each(|(old_hba, new_hba)| {
                // Get the lba of the old hba
                // Safety: hba should exist in index table, otherwise it means system is inconsistent
                let lba = index_table
                    .get(&old_hba)
                    .map(|lba| *lba)
                    .expect("hba should exist in index table");
                let record_key = RecordKey { lba };

                // get mac and key of the old hba record
                // Safety: hba should exist in lsm tree, otherwise it means system is inconsistent
                let mut record_value = tx_lsm_tree
                    .get(&record_key)
                    .expect("record key should exist in lsm tree");

                // Update the hba of the record but keep the key and mac unchanged
                record_value.hba = new_hba;

                // write the record back to lsm tree
                tx_lsm_tree.put(record_key, record_value)?;

                // update the reverse index table
                index_table.insert(new_hba, lba);
                index_table.remove(&old_hba);
                Ok::<_, Error>(())
            })
    }

    pub fn recover<D: BlockSet + 'static>(
        _tx_lsm_tree: &TxLsmTree<RecordKey, RecordValue, D>,
    ) -> Result<Self> {
        todo!()
    }
}
