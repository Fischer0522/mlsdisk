use super::{Iv, Key, Mac};
use crate::prelude::*;
use crate::{
    layers::{
        bio::BlockLog,
        crypto::{
            crypto_log::{
                CryptBuf, DataInner, DataNode, MhtInner, MhtNode, MhtNodeEntry, Pbid, SearchCtx,
                ATTACHED_DATA_NODES_COUNT, CHILD_MHT_NODES_COUNT, MHT_NBRANCHES,
            },
            mht, NodeCache, RootMhtMeta,
        },
        lsm::SSTABLE_CAPACITY,
    },
    Aead, Buf, Errno,
};
use core::cell::RefCell;
use openssl::{cipher, cipher_ctx::CipherCtxRef};
use pod::Pod;
use spin::mutex::Mutex;
use std::sync::Arc;

const ENABLED_CACHING: bool = true;

pub trait MHTInterface<L> {
    fn root_key(&self) -> Key;
    fn root_meta(&self) -> Option<RootMhtMeta>;
    fn root_node(&self) -> Option<Arc<MhtNode>>;
    fn total_data_nodes(&self) -> usize;
    fn search(&self, search_ctx: &mut SearchCtx<'_>) -> Result<()>;
    fn append_data_nodes(&mut self, data_nodes: Vec<Arc<DataNode>>) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
    fn display(&self);
}

#[derive(Clone)]
pub enum Node {
    MhtNode(Arc<MhtNode>),
    MhtNodeRef(MhtNodeRef),
    DataNode(Arc<DataNode>),
}

pub type MhtNodeRef = Arc<Mutex<MhtNode>>;

// In-place MHT
pub struct IMht<L> {
    root: Option<(RootMhtMeta, MhtNodeRef)>,
    root_key: Key,
    storage: Box<IMhtStorage<L>>,
}

struct IMhtStorage<L> {
    init: bool,
    root: Option<(RootMhtMeta, MhtNodeRef)>,
    root_key: Key,
    block_log: L,
    node_cache: Arc<dyn NodeCache>,
    crypt_buf: CryptBuf,
    logical_offset: usize,
}

impl<L: BlockLog + 'static> IMhtStorage<L> {
    pub fn reserve_capacity(&self) {
        // reserve one block for the root node
        let block_log = &self.block_log;
        let reserved_capacity = ((SSTABLE_CAPACITY * 48 / BLOCK_SIZE) as f64 * 1.2) as u64;
        let buf = Buf::alloc(reserved_capacity as usize).unwrap();
        info!(
            "Creating IMhtStorage with reserved capacity: {}",
            reserved_capacity
        );
        block_log.append(buf.as_ref()).unwrap();
        info!(
            "Block log created with reserved capacity: {}",
            reserved_capacity
        );
    }
    pub fn new(root_key: Key, block_log: L, node_cache: Arc<dyn NodeCache>) -> Self {
        let root_mht = MhtNode::new_uninit();
        let root_mht_meta = RootMhtMeta {
            pos: 0,
            mac: Mac::default(),
            iv: Iv::new_zeroed(),
        };
        let root_mht_ref = Arc::new(Mutex::new(root_mht));
        let root = Some((root_mht_meta, root_mht_ref));

        Self {
            init: false,
            root: root,
            root_key: root_key,
            block_log,
            node_cache,
            crypt_buf: CryptBuf::new(),
            logical_offset: 0,
        }
    }

    pub fn open(
        root_key: Key,
        block_log: L,
        root_meta: RootMhtMeta,
        node_cache: Arc<dyn NodeCache>,
    ) -> Result<Self> {
        info!("Opening IMht with root key: {:?}", root_key);
        let pos = 0;
        let mut cipher = Buf::alloc(1)?;
        let mut plain = Buf::alloc(1)?;
        block_log.read(pos, cipher.as_mut())?;
        // decrypt
        Aead::new().decrypt(
            cipher.as_slice(),
            &root_key,
            &root_meta.iv,
            &[],
            &root_meta.mac,
            plain.as_mut_slice(),
        )?;
        let root_mht = MhtNode {
            inner: MhtInner::from_bytes(plain.as_slice()),
            logical_number: 0,
            physical_number: pos,
            parent: None,
        };
        let mht_storage = IMhtStorage {
            init: true,
            root: Some((root_meta, Arc::new(Mutex::new(root_mht)))),
            root_key,
            block_log,
            node_cache,
            crypt_buf: CryptBuf::new(),
            logical_offset: 1, // root node is already present
        };
        info!(
            "IMhtStorage opened successfully with root node at position: {}",
            pos
        );
        Ok(mht_storage)
    }

    pub fn total_data_nodes(&self) -> usize {
        self.logical_offset
    }

    pub fn flush(&self) -> Result<()> {
        self.block_log.flush()
    }
    pub fn root_mht_node(&self) -> Result<Arc<MhtNode>> {
        let root_node = self.read_mht_node(0)?;
        let guard = root_node.lock();
        let mut copied_root = MhtNode::new_uninit();
        copied_root.inner = guard.inner.clone();
        copied_root.logical_number = guard.logical_number;
        copied_root.physical_number = guard.physical_number;

        Ok(Arc::new(copied_root))
        //zw  self.read_mht_node(root_meta.pos, root_key, &root_meta.mac, &root_meta.iv)
    }

    pub fn root_meta(&self) -> Option<RootMhtMeta> {
        self.root.as_ref().map(|(meta, _)| meta.clone())
    }

    // pub fn append_root_mht_node(&self, root_key: &Key, node: &Arc<MhtNode>) -> Result<RootMhtMeta> {
    //     // always store root node at position 0
    //     let pos = 0;
    //     let (cipher, mac, iv) = {
    //         let plain = node.inner.as_bytes();
    //         let mut cipher = self.crypt_buf.cipher.borrow_mut();
    //         let iv = Iv::random();
    //         let mac = Aead::new().encrypt(&plain, root_key, &iv, &[], cipher.as_mut_slice())?;
    //         (cipher, mac, iv)
    //     };

    //     self.block_log.update(pos,cipher.as_ref())?;
    //     if ENABLED_CACHING {
    //         self.node_cache.put(pos, Node::MhtNode(MhtNodeRef::new(node.clone())));
    //     }
    //     Ok(RootMhtMeta { pos, mac, iv })
    // }

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

        self.block_log.write(pos, cipher.as_ref())?;
        if ENABLED_CACHING {
            self.node_cache.put(pos, Node::MhtNodeRef(node.clone()));
        }
        Ok(entry)
    }

    fn append_data_nodes(&mut self, nodes: &[Arc<DataNode>]) -> Result<Vec<MhtNodeEntry>> {
        if !self.init {
            self.reserve_capacity();
            self.init = true;
        }
        let num_append = nodes.len();
        let mut node_entries = Vec::with_capacity(num_append);
        if num_append == 0 {
            return Ok(node_entries);
        }

        for (i, node) in nodes.iter().enumerate() {
            let entry = self.append_data_node(node)?;
            node_entries.push(entry);
        }
        Ok(node_entries)
    }

    fn append_data_node(&mut self, node: &Arc<DataNode>) -> Result<MhtNodeEntry> {
        let (logic_number, physical_number) =
            self.get_data_node_numbers(self.logical_offset as u64);

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
            self.block_log
                .write(physical_number as usize, cipher.as_ref())?;
            MhtNodeEntry { pos, key, mac }
        };

        let mht_node = self.get_mht_node(self.logical_offset as u64, self.logical_offset as u64)?;
        let new_node = Arc::new(DataNode {
            inner: node.inner.clone(),
            logical_number: logic_number as usize,
            physical_number: entry.pos,
            parent: Some(mht_node.clone()),
        });
        // info!("Appending data node at logical offset: {}, physical position: {}, parent is {}", self.logical_offset, entry.pos, mht_node.lock().logical_number);

        // set the new entry in parent node

        self.update_mht_entries(&new_node, entry)?;
        self.logical_offset += 1;
        if ENABLED_CACHING {
            self.node_cache
                .put(entry.pos, Node::DataNode(new_node.clone()));
        }
        Ok(entry)
    }

    fn update_mht_entries(&mut self, node: &Arc<DataNode>, entry: MhtNodeEntry) -> Result<()> {
        // update current datanode entry
        node.update_node_entry(self.logical_offset as u64, entry);

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

    fn read_data_node(&self, logical_number: u64) -> Result<Arc<DataNode>> {
        let (logic_number, physical_number) = self.get_data_node_numbers(logical_number);

        if let Some(data_node) = self.node_cache.get(physical_number as usize) {
            if let Node::DataNode(node) = data_node {
                return Ok(node.clone());
            } else {
                return Err(Error::new(Errno::NotFound));
            }
        }

        let mht_node = self.get_mht_node(logic_number, logic_number)?;

        let mut data_node = DataNode::new_uninit();
        data_node.physical_number = physical_number as Pbid;
        data_node.parent = Some(mht_node);

        let mht_entry = data_node.node_entry(logical_number);

        let Some(mht_entry) = mht_entry else {
            return_errno_with_msg!(Errno::NotFound, "MHT entry not found for logical number")
        };

        let mut cipher = self.crypt_buf.cipher.borrow_mut();
        let mut plain = self.crypt_buf.plain.borrow_mut();
        self.block_log
            .read(physical_number as usize, cipher.as_mut())?;

        // decrypt
        Aead::new()
            .decrypt(
                cipher.as_slice(),
                &mht_entry.key,
                &Iv::new_zeroed(),
                &[],
                &mht_entry.mac,
                plain.as_mut_slice(),
            )
            .unwrap();
        data_node.inner = DataInner::from_bytes(plain.as_slice());

        let data_node = Arc::new(data_node);
        Ok(data_node)
    }

    fn get_data_node(&self, entry: &MhtNodeEntry, node_buf: &mut [u8]) -> Result<()> {
        todo!()
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
        self.node_cache
            .put(physical_number as usize, Node::MhtNodeRef(mht_node.clone()));
        Ok(mht_node)
    }

    fn read_mht_node(&self, logical_number: u64) -> Result<MhtNodeRef> {
        if logical_number == 0 {
            return Ok(self.root.as_ref().unwrap().1.clone());
        }
        let physical_number = logical_number * (ATTACHED_DATA_NODES_COUNT + 1) as u64;
        if let Some(Node::MhtNodeRef(node)) = self.node_cache.get(physical_number as usize) {
            return Ok(node.clone());
        }

        let parent_mht_node =
            self.read_mht_node((logical_number - 1) / CHILD_MHT_NODES_COUNT as u64)?;
        let mut mht_node = MhtNode::new_uninit();
        mht_node.parent = Some(parent_mht_node.clone());
        mht_node.logical_number = logical_number as usize;
        mht_node.physical_number = physical_number as usize;

        let mut cipher = self.crypt_buf.cipher.borrow_mut();
        let mut plain = self.crypt_buf.plain.borrow_mut();
        self.block_log
            .read(physical_number as usize, cipher.as_mut())?;
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
        self.node_cache
            .put(physical_number as usize, Node::MhtNodeRef(mht_node.clone()));
        Ok(mht_node)
    }

    fn get_mht_node(&self, logical_offset: u64, data_offset: u64) -> Result<MhtNodeRef> {
        let (logic_number, _) = self.get_mht_node_numbers(logical_offset);
        if logic_number == 0 {
            return Ok(self.root.as_ref().unwrap().1.clone());
        }

        if logical_offset % ATTACHED_DATA_NODES_COUNT as u64 == 0
            && data_offset == self.logical_offset as u64
        {
            self.append_mht_node(logic_number)
        } else {
            self.read_mht_node(logic_number)
        }
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
}

impl<L: BlockLog + 'static> IMht<L> {
    pub fn new(block_log: L, root_key: Key, node_cache: Arc<dyn NodeCache>) -> Self {
        Self {
            root: None,
            root_key,
            storage: Box::new(IMhtStorage::new(root_key, block_log, node_cache)),
        }
    }

    pub fn open(
        block_log: L,
        root_key: Key,
        root_meta: RootMhtMeta,
        node_cache: Arc<dyn NodeCache>,
    ) -> Result<Self> {
        info!("Opening IMht with root key: {:?}", root_key);
        let mht_storage = IMhtStorage::open(root_key, block_log, root_meta, node_cache)?;
        let mht = Self {
            root: mht_storage.root.clone(),
            root_key,
            storage: Box::new(mht_storage),
        };
        // read root mht node from block log
        Ok(mht)
    }
}

impl<L: BlockLog + 'static> MHTInterface<L> for IMht<L> {
    fn root_key(&self) -> Key {
        self.root_key
    }

    fn root_meta(&self) -> Option<RootMhtMeta> {
        self.storage.root_meta()
    }

    fn root_node(&self) -> Option<Arc<MhtNode>> {
        self.storage.root_mht_node().ok()
    }

    fn total_data_nodes(&self) -> usize {
        self.storage.total_data_nodes().clone()
    }

    fn search(&self, search_ctx: &mut SearchCtx<'_>) -> Result<()> {
        for offset in 0..search_ctx.num {
            let logical_number = search_ctx.pos + offset;
            let data_node = self.storage.read_data_node(logical_number as u64)?;
            search_ctx
                .node_buf(offset)
                .copy_from_slice(&data_node.inner.0);
        }
        search_ctx.is_completed = true;
        Ok(())
    }

    fn append_data_nodes(&mut self, data_nodes: Vec<Arc<DataNode>>) -> Result<()> {
        if data_nodes.is_empty() {
            return Ok(());
        }

        self.storage.append_data_nodes(&data_nodes)?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.storage.flush()
    }

    fn display(&self) {
        if let Some((root_meta, root_node)) = &self.root {
            println!("Root MHT Meta: {:?}", root_meta);
        } else {
            println!("No root MHT node available.");
        }
    }
}

#[cfg(test)]
mod tests {

    use core::num::NonZeroUsize;

    use lru::LruCache;
    use spin::once::Once;

    use super::*;
    use crate::layers::bio::MemLog;

    static INIT_LOG: Once = Once::new();

    fn init_logger() {
        INIT_LOG.call_once(|| {
            env_logger::builder()
                .is_test(true)
                .filter_level(log::LevelFilter::Info)
                .try_init()
                .unwrap();
        });
    }

    struct NoCache;
    impl NodeCache for NoCache {
        fn get(&self, _pos: Pbid) -> Option<Node> {
            None
        }
        fn put(&self, _pos: Pbid, _value: Node) -> Option<Node> {
            None
        }
    }

    pub struct MemCache {
        cache: Mutex<LruCache<u64, Node>>,
    }
    impl MemCache {
        pub fn new(capacity: usize) -> Self {
            Self {
                cache: Mutex::new(LruCache::new(NonZeroUsize::new(capacity).unwrap())),
            }
        }
    }

    impl NodeCache for MemCache {
        fn get(&self, pos: usize) -> Option<Node> {
            self.cache.lock().get(&(pos as u64)).cloned()
        }

        fn put(&self, pos: Pbid, value: Node) -> Option<Node> {
            self.cache.lock().put(pos as u64, value)
        }
    }

    fn mht_create() -> Result<(MemLog, IMht<MemLog>)> {
        let block_log = MemLog::create(30000)?;
        let copyed_block_log = block_log.clone();
        let node_cache = Arc::new(NoCache {});
        let root_key = Key::random();
        let imht = IMht::<MemLog>::new(block_log, root_key, node_cache);
        info!("Created IMHT successfully");
        Ok((copyed_block_log, imht))
    }

    #[test]
    fn imht_create() {
        let (_, imht) = mht_create().unwrap();
        assert!(imht.root.is_none());
        assert_eq!(imht.storage.total_data_nodes(), 0);
        imht.display();
    }

    #[test]
    fn imht_append_data_nodes() {
        let (_, mut imht) = mht_create().unwrap();
        let data_nodes: Vec<Arc<DataNode>> = (0..10)
            .map(|i| {
                Arc::new(DataNode {
                    inner: DataInner::from_bytes(&[i as u8; BLOCK_SIZE]),
                    logical_number: i as usize,
                    physical_number: 0,
                    parent: None,
                })
            })
            .collect();
        imht.append_data_nodes(data_nodes).unwrap();
        assert_eq!(imht.storage.total_data_nodes(), 10);

        let mut buf = Buf::alloc(10).unwrap();
        let mut search_ctx = SearchCtx::new(0, buf.as_mut());
        imht.search(&mut search_ctx).unwrap();
        assert!(search_ctx.is_completed);
        for i in 0..10 {
            assert_eq!(search_ctx.node_buf(i), &[i as u8; BLOCK_SIZE]);
        }
    }

    #[test]
    fn imht_multi_append() {
        init_logger();
        let (_, mut imht) = mht_create().unwrap();
        let data_nodes: Vec<Arc<DataNode>> = (0..1000)
            .map(|i| {
                Arc::new(DataNode {
                    inner: DataInner::from_bytes(&[i as u8; BLOCK_SIZE]),
                    logical_number: i as usize,
                    physical_number: 0,
                    parent: None,
                })
            })
            .collect();
        imht.append_data_nodes(data_nodes).unwrap();
        assert_eq!(imht.storage.total_data_nodes(), 1000);

        // Append more data nodes
        let more_data_nodes: Vec<Arc<DataNode>> = (10..20)
            .map(|i| {
                Arc::new(DataNode {
                    inner: DataInner::from_bytes(&[i as u8; BLOCK_SIZE]),
                    logical_number: i as usize,
                    physical_number: 0,
                    parent: None,
                })
            })
            .collect();
        imht.append_data_nodes(more_data_nodes).unwrap();
        assert_eq!(imht.storage.total_data_nodes(), 1010);
    }

    #[test]
    fn imht_search() {
        init_logger();
        let (_, mut imht) = mht_create().unwrap();
        let data_nodes: Vec<Arc<DataNode>> = (0..1000)
            .map(|i| {
                Arc::new(DataNode {
                    inner: DataInner::from_bytes(&[i as u8; BLOCK_SIZE]),
                    logical_number: i as usize,
                    physical_number: 0,
                    parent: None,
                })
            })
            .collect();
        imht.append_data_nodes(data_nodes).unwrap();
        assert_eq!(imht.storage.total_data_nodes(), 1000);
        let mut buf = Buf::alloc(100).unwrap();
        let mut search_ctx = SearchCtx::new(60, buf.as_mut());
        imht.search(&mut search_ctx).unwrap();
        assert!(search_ctx.is_completed);
        for i in 0..100 {
            assert_eq!(search_ctx.node_buf(i), &[60 + i as u8; BLOCK_SIZE]);
        }
    }

    #[test]
    fn imht_open() {
        init_logger();
        let (mem_log, mut imht) = mht_create().unwrap();
        let data_nodes: Vec<Arc<DataNode>> = (0..1000)
            .map(|i| {
                Arc::new(DataNode {
                    inner: DataInner::from_bytes(&[i as u8; BLOCK_SIZE]),
                    logical_number: i as usize,
                    physical_number: 0,
                    parent: None,
                })
            })
            .collect();
        imht.append_data_nodes(data_nodes).unwrap();
        assert_eq!(imht.storage.total_data_nodes(), 1000);
        let mut buf = Buf::alloc(100).unwrap();
        let mut search_ctx = SearchCtx::new(60, buf.as_mut());
        imht.search(&mut search_ctx).unwrap();
        assert!(search_ctx.is_completed);
        for i in 0..100 {
            assert_eq!(search_ctx.node_buf(i), &[60 + i as u8; BLOCK_SIZE]);
        }
        imht.flush().unwrap();

        let root_meta = imht.root_meta().unwrap();
        let root_key = imht.root_key();
        drop(imht); // Drop the original IMHT
        let node_cache = Arc::new(NoCache {});
        let imht = IMht::open(mem_log, root_key, root_meta, node_cache).unwrap();

        let mut buf = Buf::alloc(100).unwrap();
        let mut search_ctx = SearchCtx::new(60, buf.as_mut());
        imht.search(&mut search_ctx).unwrap();
        assert!(search_ctx.is_completed);
        for i in 0..100 {
            assert_eq!(search_ctx.node_buf(i), &[60 + i as u8; BLOCK_SIZE]);
        }
    }

    // Add more tests for append, search, flush, etc.
}
