use std::sync::Arc;
use super::{Iv, Key, Mac};
use crate::{layers::{bio::BlockLog, crypto::{crypto_log::{CryptBuf, DataNode, MhtNode, MhtNodeEntry, Pbid, SearchCtx}, NodeCache, RootMhtMeta}}, Aead, Buf};
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
    block_log: L,
    node_cache: Arc<dyn NodeCache>,
    crypt_buf: CryptBuf,
}

impl<L: BlockLog + 'static> IMhtStorage<L> {
    pub fn new(block_log: L, node_cache: Arc<dyn NodeCache>) -> Self {
        Self {
            block_log,
            node_cache,
            crypt_buf: CryptBuf::new(),
        }
    }

    pub fn flush(&self) -> Result<()> {
        self.block_log.flush()
    }
     pub fn root_mht_node(&self, root_key: &Key, root_meta: &RootMhtMeta) -> Result<Arc<MhtNode>> {
        self.read_mht_node(root_meta.pos, root_key, &root_meta.mac, &root_meta.iv)
    }

    pub fn append_root_mht_node(&self, root_key: &Key, node: &Arc<MhtNode>) -> Result<RootMhtMeta> {
        unimplemented!("append_root_mht_node");
        let (cipher, mac, iv) = {
            let plain = node.as_bytes();
            let mut cipher = self.crypt_buf.cipher.borrow_mut();
            let iv = Iv::random();
            let mac = Aead::new().encrypt(&plain, root_key, &iv, &[], cipher.as_mut_slice())?;
            (cipher, mac, iv)
        };

        let pos = self.block_log.append(cipher.as_ref())?;
        if ENABLED_CACHING {
            self.node_cache.put(pos, Node::MhtNode(node.clone()));
        }
        Ok(RootMhtMeta { pos, mac, iv })
    }

    fn append_mht_nodes(&self, nodes: &[Arc<MhtNode>]) -> Result<Vec<MhtNodeEntry>> {
        unimplemented!("append_mht_nodes");
        let num_append = nodes.len();
        let mut node_entries = Vec::with_capacity(num_append);
        let mut cipher_buf = Buf::alloc(num_append)?;
        let mut pos = self.block_log.nblocks() as BlockId;
        let start_pos = pos;
        for (i, node) in nodes.iter().enumerate() {
            let plain = node.as_bytes();
            let cipher = &mut cipher_buf.as_mut_slice()[i * BLOCK_SIZE..(i + 1) * BLOCK_SIZE];
            let key = Key::random();
            let mac = Aead::new().encrypt(&plain, &key, &Iv::new_zeroed(), &[], cipher)?;

            node_entries.push(MhtNodeEntry { pos, key, mac });
            if ENABLED_CACHING {
                self.node_cache.put(pos, Node::MhtNode(node.clone()));
            }
            pos += 1;
        }

        let append_pos = self.block_log.append(cipher_buf.as_ref())?;
        debug_assert_eq!(start_pos, append_pos);
        Ok(node_entries)
    }

    fn append_data_nodes(&self, nodes: &[Arc<DataNode>]) -> Result<Vec<MhtNodeEntry>> {
        unimplemented!("append_data_nodes");
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
            let mac = Aead::new().encrypt(&node.0, &key, &Iv::new_zeroed(), &[], cipher)?;

            node_entries.push(MhtNodeEntry { pos, key, mac });
            pos += 1;
        }

        let append_pos = self.block_log.append(cipher_buf.as_ref())?;
        debug_assert_eq!(start_pos, append_pos);
        Ok(node_entries)
    }

    fn read_mht_node(&self, pos: Pbid, key: &Key, mac: &Mac, iv: &Iv) -> Result<Arc<MhtNode>> {
        unimplemented!("read_mht_node");
        if let Some(Node::MhtNode(node)) = self.node_cache.get(pos) {
            return Ok(node);
        }
        //info!("miss cache for MHT node at pos {}", pos);
        let mht_node = {
            let mut cipher = self.crypt_buf.cipher.borrow_mut();
            let mut plain = self.crypt_buf.plain.borrow_mut();
            self.block_log.read(pos, cipher.as_mut())?;
            Aead::new().decrypt(cipher.as_slice(), key, iv, &[], mac, plain.as_mut_slice())?;
            Arc::new(MhtNode::from_bytes(plain.as_slice()))
        };

        if ENABLED_CACHING {
            self.node_cache.put(pos, Node::MhtNode(mht_node.clone()));
        }
        Ok(mht_node)
    }

    fn read_data_node(&self, entry: &MhtNodeEntry, node_buf: &mut [u8]) -> Result<()> {
        todo!()
    }

    fn get_data_node(&self, entry: &MhtNodeEntry, node_buf: &mut [u8]) -> Result<()> {
        todo!()
    }
}


impl<L: BlockLog + 'static> IMht<L> {
     pub fn new(block_log: L, root_key: Key, node_cache: Arc<dyn NodeCache>) -> Self {
        Self {
            root: None,
            root_key,
            storage: Arc::new(IMhtStorage::new(block_log, node_cache)),
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

