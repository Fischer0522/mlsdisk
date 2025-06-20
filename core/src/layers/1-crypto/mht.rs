use std::sync::Arc;
use super::{Iv, Key, Mac};
use crate::{layers::{bio::BlockLog, crypto::{crypto_log::{CryptBuf, DataNode, MhtNode, MhtNodeEntry, Pbid, SearchCtx, MHT_NBRANCHES}, NodeCache, RootMhtMeta}}, Aead, Buf};
use crate::prelude::*;
use pod::Pod;

const ENABLED_CACHING: bool = true;

pub trait MHTInterface<L> {
    fn root_key(&self) -> Key;
    fn root_meta(&self) -> Option<RootMhtMeta>;
    fn root_node(&self) -> Option<&Arc<MhtNode>>;
    fn total_data_nodes(&self) -> usize;
    fn search(&self, search_ctx: &mut SearchCtx<'_>) -> Result<()>;
    fn append_data_nodes(&mut self, data_nodes: Vec<Arc<DataNode>>) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
    fn display(&self);

}

#[derive(Clone)]
pub enum Node {
    MhtNode(Arc<MhtNode>),
    DataNode(Arc<DataNode>),
}

// In-place MHT
pub struct IMht<L> {
    root: Option<(RootMhtMeta, Arc<MhtNode>)>,
    root_key: Key,
    storage: Arc<IMhtStorage<L>>,
}

struct IMhtStorage<L> {
    root: Option<(RootMhtMeta, Arc<MhtNode>)>,
    root_key: Key,
    block_log: L,
    node_cache: Arc<dyn NodeCache>,
    crypt_buf: CryptBuf,
    logical_offset: usize,
}

impl<L: BlockLog + 'static> IMhtStorage<L> {
    pub fn new(root: Option<(RootMhtMeta, Arc<MhtNode>)>, root_key: Key, block_log: L, node_cache: Arc<dyn NodeCache>) -> Self {
        Self {
            root: None,
            root_key: Key::random(),
            block_log,
            node_cache,
            crypt_buf: CryptBuf::new(),
            logical_offset: 0,
        }
    }

    pub fn flush(&self) -> Result<()> {
        self.block_log.flush()
    }
     pub fn root_mht_node(&self, root_key: &Key, root_meta: &RootMhtMeta) -> Result<Arc<MhtNode>> {
        todo!()
      //zw  self.read_mht_node(root_meta.pos, root_key, &root_meta.mac, &root_meta.iv)
    }

    pub fn append_root_mht_node(&self, root_key: &Key, node: &Arc<MhtNode>) -> Result<RootMhtMeta> {
        // always store root node at position 0
        let pos = 0;
        let (cipher, mac, iv) = {
            let plain = node.inner.as_bytes();
            let mut cipher = self.crypt_buf.cipher.borrow_mut();
            let iv = Iv::random();
            let mac = Aead::new().encrypt(&plain, root_key, &iv, &[], cipher.as_mut_slice())?;
            (cipher, mac, iv)
        };

        self.block_log.update(pos,cipher.as_ref())?;
        if ENABLED_CACHING {
            self.node_cache.put(pos, Node::MhtNode(node.clone()));
        }
        Ok(RootMhtMeta { pos, mac, iv })
    }

    fn update_mht_node(&self, pos: BlockId,node: &Arc<MhtNode>) -> Result<MhtNodeEntry> {
        let (cipher, entry) = {
            let plain = node.inner.as_bytes();
            let mut cipher = self.crypt_buf.cipher.borrow_mut();
            let iv = Iv::random();
            let key = Key::random();
            let mac = Aead::new().encrypt(&plain, &key, &iv, &[], cipher.as_mut_slice())?;
            (cipher, MhtNodeEntry { pos, key, mac })
        };

        self.block_log.update(pos,cipher.as_ref())?;
        if ENABLED_CACHING {
            self.node_cache.put(pos, Node::MhtNode(node.clone()));
        }
        Ok(entry)
    }

    fn append_data_nodes(&self, nodes: &[Arc<DataNode>]) -> Result<Vec<MhtNodeEntry>> {
        let num_append = nodes.len();
        let mut node_entries = Vec::with_capacity(num_append);
        if num_append == 0 {
            return Ok(node_entries);
        }

        let mut cipher_buf = Buf::alloc(num_append)?;
        let mut pos = self.block_log.nblocks() as BlockId;
        let start_pos = pos;
        for (i, node) in nodes.iter().enumerate() {
            let cipher = &mut cipher_buf.as_mut_slice()[i * BLOCK_SIZE..(i + 1) * BLOCK_SIZE];
            let key = Key::random();
            let mac = Aead::new().encrypt(&node.inner.0, &key, &Iv::new_zeroed(), &[], cipher)?;

            node_entries.push(MhtNodeEntry { pos, key, mac });
            pos += 1;
        }

        let append_pos = self.block_log.append(cipher_buf.as_ref())?;
        debug_assert_eq!(start_pos, append_pos);
        Ok(node_entries)
    }
    fn append_data_node(&self, node: &Arc<DataNode>) -> Result<MhtNodeEntry> {
        let (entry) = {
            let mut cipher = self.crypt_buf.cipher.borrow_mut();
            let key = Key::random();
            let mac = Aead::new().encrypt(&node.inner.0, &key, &Iv::new_zeroed(), &[], cipher.as_mut_slice())?;
            let pos = self.block_log.append(cipher.as_ref())?;
            (MhtNodeEntry { pos, key, mac })
        };
        if ENABLED_CACHING {
            self.node_cache.put(entry.pos, Node::DataNode(node.clone()));
        }
        Ok(entry)
    }



    fn read_data_node(&self, entry: &MhtNodeEntry, node_buf: &mut [u8]) -> Result<()> {
        todo!()
    }

    fn get_data_node(&self, entry: &MhtNodeEntry, node_buf: &mut [u8]) -> Result<()> {
        todo!()
    }

    fn append_mht_node(&self, logical_number: u64) -> Result<Arc<MhtNode>> {
        let physical_number = logical_number * (MHT_NBRANCHES as u64 + 1);
        let mht_node = Arc::new(MhtNode::new_uninit());
        self.node_cache.put(physical_number as usize, Node::MhtNode(mht_node.clone()));
        Ok(mht_node)
    }

    fn read_mht_node(&self, logical_number: u64) -> Result<Arc<MhtNode>> {
        if (logical_number == 0) {
            return Ok(self.root.as_ref().unwrap().1.clone());
        }
        let physical_number = logical_number * (MHT_NBRANCHES as u64 + 1);
        if let Some(Node::MhtNode(node)) = self.node_cache.get(physical_number as usize) {
            return Ok(node);
        }

        // iter from root to target node
        let mut parent_node = self.root.as_ref().unwrap().1.clone();

        for i in 1..logical_number {
            let physical_number = i * (MHT_NBRANCHES as u64 + 1);
            if let Some(Node::MhtNode(node)) = self.node_cache.get(physical_number as usize) {
                parent_node = node;
            } else {
                let mut cipher = self.crypt_buf.cipher.borrow_mut();
                let mut plain = self.crypt_buf.plain.borrow_mut();
                let current_node = self.block_log.read(physical_number as usize, cipher.as_mut())?;

            }
        }
        todo!()
        //info!("miss cache for MHT node at pos {}", pos);
        // let mht_node = {
        //     let mut cipher = self.crypt_buf.cipher.borrow_mut();
        //     let mut plain = self.crypt_buf.plain.borrow_mut();
        //     self.block_log.read(pos, cipher.as_mut())?;
        //     Aead::new().decrypt(cipher.as_slice(), key, iv, &[], mac, plain.as_mut_slice())?;
        //     Arc::new(MhtNode::from_bytes(plain.as_slice()))
        // };

        // if ENABLED_CACHING {
        //     self.node_cache.put(pos, Node::MhtNode(mht_node.clone()));
        // }
    }


    fn get_mht_node(&self, logical_number: u64) -> Result<Arc<MhtNode>> {
        let mut parent_node = None;
        for i in 0..logical_number {
            let physical_number = logical_number * (MHT_NBRANCHES as u64 + 1);
            // root node
            if let Some(Node::MhtNode(parent)) = self.node_cache.get(physical_number as usize) {
                parent_node = Some(parent);
            } else {

            }
        }
        todo!()
    }

    fn get_node_numbers(&self) -> (u64, u64, u64, u64) {
    if self.logical_offset < 1 {
        return (0, 0, 0, 0);
    }

    // node 0 - mht
    // nodes 1-102 - data (MHT_NBRANCHES == 102)
    // node 103 - mht
    // node 104-205 - data
    // etc.
    let data_logic_number = self.logical_offset  as u64;
    let mht_logic_number = data_logic_number / MHT_NBRANCHES as u64;

    // + 1 - mht root
    // + mht_logic_number - number of mht nodes in the middle (the root mht mht_node_number is 0)
    let data_physical_number = data_logic_number + 1 + mht_logic_number;

    let mht_physical_number =
        data_physical_number - data_logic_number % MHT_NBRANCHES as u64 - 1;


    (
        mht_logic_number,
        data_logic_number,
        mht_physical_number,
        data_physical_number,
    )
}
}


impl<L: BlockLog + 'static> IMht<L> {
     pub fn new(block_log: L, root_key: Key, node_cache: Arc<dyn NodeCache>) -> Self {
        Self {
            root: None,
            root_key,
            storage: Arc::new(IMhtStorage::new(None, root_key, block_log, node_cache)),
        }
    }

    pub fn open(
        block_log: L,
        root_key: Key,
        root_meta: RootMhtMeta,
        node_cache: Arc<dyn NodeCache>,
    ) -> Result<Self> {
        // read root mht node from block log
        todo!()
    }
}

impl <L: BlockLog> MHTInterface<L> for IMht<L> {
    fn root_key(&self) -> Key {
        self.root_key
    }

    fn root_meta(&self) -> Option<RootMhtMeta> {
        self.root.as_ref().map(|(root_meta, _)| root_meta.clone())
    }

    fn root_node(&self) -> Option<&Arc<MhtNode>> {
        todo!()
    }

    fn total_data_nodes(&self) -> usize {
        todo!()
    }

    fn search(&self, search_ctx: &mut SearchCtx<'_>) -> Result<()> {
        todo!()
    }

    fn append_data_nodes(&mut self, data_nodes: Vec<Arc<DataNode>>) -> Result<()> {
        todo!()
    }

    fn flush(&mut self) -> Result<()> {
        todo!()
    }

    fn display(&self) {
        todo!()
    }
}

