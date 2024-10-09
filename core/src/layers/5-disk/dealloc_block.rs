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

    pub fn recover<D: BlockSet + 'static>(
        _tx_lsm_tree: &TxLsmTree<RecordKey, RecordValue, D>,
    ) -> Result<Self> {
        todo!()
    }
}
