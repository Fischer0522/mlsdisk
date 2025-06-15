use std::sync::Arc;
use super::{Iv, Key, Mac};
use crate::layers::crypto::{crypto_log::{DataNode, MhtNode, SearchCtx}, RootMhtMeta};
use crate::prelude::*;
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
pub enum CacheEntry {
    MhtNode(Arc<MhtNode>),
    DataNode(Arc<DataNode>),
}

// In-place MHT
pub struct IMht {
    root: Option<(RootMhtMeta, Arc<MhtNode>)>,
    root_key: Key,
}