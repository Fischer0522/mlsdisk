use core::ops::Range;

use pod::Pod;
use spin::Mutex;

use crate::layers::crypto::crypto_log::{
    CryptBuf, DataNode, MhtInner, MhtNode, MhtNodeEntry, Pbid, ATTACHED_DATA_NODES_COUNT,
    CHILD_MHT_NODES_COUNT,
};
use crate::layers::crypto::{Iv, RootMhtMeta};
use crate::{layers::crypto::mht::MhtNodeRef, AeadKey as Key};
use crate::{prelude::*, Aead, AeadIv, BlockSet, BufMut, BufRef};
pub struct MhtDisk<D> {
    inner: Arc<Mutex<DiskInner<D>>>,
}

pub struct DiskInner<D> {
    disk: D,
    root_key: Key,
    size: u64,
    crypt_buf: CryptBuf,
    root: Option<(RootMhtMeta, MhtNodeRef)>,
}

impl<D: BlockSet> DiskInner<D> {
    pub fn new(disk: D, root_key: Key) -> Self {
        Self {
            disk,
            root_key,
            root: None,
            size: 0,
            crypt_buf: CryptBuf::new(),
        }
    }

    pub fn disk(&self) -> &D {
        &self.disk
    }

    pub fn root_key(&self) -> &Key {
        &self.root_key
    }

    fn update_mht_node(&self, pos: BlockId, node: &MhtNodeRef) -> Result<MhtNodeEntry> {
        let (cipher, entry) = {
            let node_ref = node.lock();
            let plain = node_ref.inner.as_bytes();
            let mut cipher = self.crypt_buf.cipher.borrow_mut();
            let iv = Iv::new_zeroed();
            let key = if pos == 0 {
                self.root_key.clone()
            } else {
                Key::random()
            };
            let mac = Aead::new().encrypt(&plain, &key, &iv, &[], cipher.as_mut_slice())?;
            (cipher, MhtNodeEntry { pos, key, mac })
        };

        self.disk.write(pos, cipher.as_ref())?;
        Ok(entry)
    }

    fn append_mht_node(&self, logical_number: u64) -> Result<MhtNodeRef> {
        let physical_number = logical_number * (ATTACHED_DATA_NODES_COUNT as u64 + 1);
        let parent_mht_node =
            self.read_mht_node((logical_number - 1) / CHILD_MHT_NODES_COUNT as u64)?;
        let mht_node = Arc::new(Mutex::new(MhtNode::new_uninit()));
        {
            let mut mht_node_guard = mht_node.lock();
            mht_node_guard.parent = Some(parent_mht_node.clone());
            mht_node_guard.logical_number = logical_number as usize;
            mht_node_guard.physical_number = physical_number as usize;
        }
        Ok(mht_node)
    }

    fn read_mht_node(&self, logical_number: u64) -> Result<MhtNodeRef> {
        if logical_number == 0 {
            return Ok(self.root.as_ref().unwrap().1.clone());
        }
        let physical_number = logical_number * (ATTACHED_DATA_NODES_COUNT + 1) as u64;

        let parent_mht_node =
            self.read_mht_node((logical_number - 1) / CHILD_MHT_NODES_COUNT as u64)?;
        let mut mht_node = MhtNode::new_uninit();
        mht_node.parent = Some(parent_mht_node.clone());
        mht_node.logical_number = logical_number as usize;
        mht_node.physical_number = physical_number as usize;

        let mut cipher = self.crypt_buf.cipher.borrow_mut();
        let mut plain = self.crypt_buf.plain.borrow_mut();
        self.disk.read(physical_number as usize, cipher.as_mut())?;
        // info!("Reading MHT node at logical number: {}, physical number: {}", logical_number, physical_number);
        let entry = mht_node.node_entry(logical_number).unwrap();

        // decrypt

        Aead::new()
            .decrypt(
                cipher.as_slice(),
                &entry.key,
                &Iv::new_zeroed(),
                &[],
                &entry.mac,
                plain.as_mut_slice(),
            )
            .unwrap();

        mht_node.inner = MhtInner::from_bytes(plain.as_slice());
        let mht_node = Arc::new(Mutex::new(mht_node));
        Ok(mht_node)
    }

    fn append_data_node(&mut self, node: &Arc<DataNode>, pos: BlockId) -> Result<MhtNodeEntry> {
        let (logic_number, physical_number) = self.get_data_node_numbers(pos as u64);

        let entry = {
            let mut cipher = self.crypt_buf.cipher.borrow_mut();
            let key = Key::random();
            let mac = Aead::new().encrypt(
                &node.inner.0,
                &key,
                &Iv::new_zeroed(),
                &[],
                cipher.as_mut_slice(),
            )?;
            let pos = physical_number as Pbid;
            self.disk.write(physical_number as usize, cipher.as_ref())?;
            MhtNodeEntry { pos, key, mac }
        };

        let mht_node = self.get_mht_node(pos as u64, pos as u64)?;
        let new_node = Arc::new(DataNode {
            inner: node.inner.clone(),
            logical_number: logic_number as usize,
            physical_number: entry.pos,
            parent: Some(mht_node.clone()),
        });
        // info!("Appending data node at logical offset: {}, physical position: {}, parent is {}", self.logical_offset, entry.pos, mht_node.lock().logical_number);

        // set the new entry in parent node

        self.update_mht_entries(&new_node, entry, pos)?;
        Ok(entry)
    }

    fn update_mht_entries(
        &mut self,
        node: &Arc<DataNode>,
        entry: MhtNodeEntry,
        pos: BlockId,
    ) -> Result<()> {
        // update current datanode entry
        node.update_node_entry(pos as u64, entry);

        // update all parent nodes until we reach the root node
        let mut parent_node = node.parent.clone();

        while let Some(node) = parent_node.clone() {
            let (logical_number, pos) = {
                let node_ref = node.lock();
                let logical_number = node_ref.logical_number;
                let pos = node_ref.physical_number;
                (logical_number, pos)
            };

            let new_entry = self.update_mht_node(pos, &node)?;
            //point node to it's parent and get a new parent node
            let node_ref = node.lock();
            node_ref.update_node_entry(logical_number as u64, new_entry);
            parent_node = node_ref.parent.clone();

            // current node is root node
            if parent_node.is_none() {
                let root_meta = RootMhtMeta {
                    pos,
                    mac: new_entry.mac,
                    iv: Iv::new_zeroed(),
                };
                self.root = Some((root_meta, node.clone()));
            }
        }
        Ok(())
    }

    fn get_mht_node(&self, logical_offset: u64, data_offset: u64) -> Result<MhtNodeRef> {
        let (logic_number, _) = self.get_mht_node_numbers(logical_offset);
        if logic_number == 0 {
            return Ok(self.root.as_ref().unwrap().1.clone());
        }

        if logical_offset % ATTACHED_DATA_NODES_COUNT as u64 == 0 && data_offset == self.size as u64
        {
            self.append_mht_node(logic_number)
        } else {
            self.read_mht_node(logic_number)
        }
    }

    fn get_node_numbers(&self, logical_offset: u64) -> (u64, u64, u64, u64) {
        // node 0 - mht
        // nodes 1-102 - data (MHT_NBRANCHES == 102)
        // node 103 - mht
        // node 104-205 - data
        // etc.
        let data_logic_number = logical_offset;
        let mht_logic_number = data_logic_number / ATTACHED_DATA_NODES_COUNT as u64;

        // + 1 - mht root
        // + mht_logic_number - number of mht nodes in the middle (the root mht mht_node_number is 0)
        let data_physical_number = data_logic_number + 1 + mht_logic_number;

        let mht_physical_number =
            data_physical_number - data_logic_number % ATTACHED_DATA_NODES_COUNT as u64 - 1;

        (
            mht_logic_number,
            data_logic_number,
            mht_physical_number,
            data_physical_number,
        )
    }

    #[inline]
    pub fn get_data_node_numbers(&self, logical_offset: u64) -> (u64, u64) {
        let (_, logic, _, physical) = self.get_node_numbers(logical_offset);
        (logic, physical)
    }

    #[inline]
    fn get_mht_node_numbers(&self, logical_offset: u64) -> (u64, u64) {
        let (logic, _, physical, _) = self.get_node_numbers(logical_offset);
        (logic, physical)
    }
}
