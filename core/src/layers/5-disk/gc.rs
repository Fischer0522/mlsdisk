use super::{
    block_alloc::{AllocTable, BlockAlloc},
    sworndisk::{Hba, Lba, RecordKey, RecordValue},
};
use crate::{
    layers::lsm::{RecordKey as RecordK, RecordValue as RecordV, TxLsmTree},
    tx::TxProvider,
    BlockSet, Error,
};
use crate::{
    layers::{
        disk::{bio::BlockBuf, block_alloc},
        log::TxLogStore,
    },
    prelude::Result,
    Buf, BLOCK_SIZE,
};
use crate::{
    os::{Arc, BTreeMap, Condvar, CvarMutex, Mutex, Vec},
    prelude,
};
use core::{
    ops::{Add, Sub},
    sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU64, AtomicUsize, Ordering},
    time::Duration,
    usize,
};
use hashbrown::{HashMap, HashSet};
use log::debug;
use pod::Pod;

#[repr(C)]
#[derive(Clone, Copy, Pod, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct ReverseKey {
    pub hba: Hba,
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Debug)]
pub struct ReverseValue {
    pub lba: Lba,
}

impl Add<usize> for ReverseKey {
    type Output = Self;

    fn add(self, rhs: usize) -> Self::Output {
        Self {
            hba: self.hba + rhs,
        }
    }
}

impl Sub<ReverseKey> for ReverseKey {
    type Output = usize;

    fn sub(self, rhs: ReverseKey) -> Self::Output {
        self.hba - rhs.hba
    }
}

impl RecordK<ReverseKey> for ReverseKey {}
impl RecordV for ReverseValue {}

// SharedState is used to synchronize background GC and foreground I/O requests and lsm compaction
// 1. Background GC will stop the world, I/O requests and lsm compaction will be blocked
// 2. Background GC should wait until lsm compaction are done
// TODO: 3. Should background GC wait for all I/O requests to finished?

pub type SharedStateRef = Arc<SharedState>;
pub struct SharedState {
    gc_in_progress: CvarMutex<bool>,
    compaction_in_progress: CvarMutex<bool>,
    gc_condvar: Condvar,
    compaction_condvar: Condvar,
}

impl SharedState {
    pub fn new() -> Self {
        Self {
            gc_in_progress: CvarMutex::new(false),
            compaction_in_progress: CvarMutex::new(false),
            gc_condvar: Condvar::new(),
            compaction_condvar: Condvar::new(),
        }
    }

    // Compaction worker and I/O requests will call this function to wait for background GC
    pub fn wait_for_background_gc(&self) {
        let mut gc_in_progress = self.gc_in_progress.lock().unwrap();
        while *gc_in_progress {
            #[cfg(not(feature = "linux"))]
            debug!("Waiting for background GC to finish");
            gc_in_progress = self.gc_condvar.wait(gc_in_progress).unwrap();
        }
    }

    // Background GC will call this function to wait for compaction finished
    pub fn wait_for_compaction(&self) {
        let mut compaction_in_progress = self.compaction_in_progress.lock().unwrap();
        while *compaction_in_progress {
            #[cfg(not(feature = "linux"))]
            debug!("Waiting for compaction to finish");
            compaction_in_progress = self
                .compaction_condvar
                .wait(compaction_in_progress)
                .unwrap();
        }
    }

    pub fn start_gc(&self) {
        let mut gc_in_progress = self.gc_in_progress.lock().unwrap();
        *gc_in_progress = true;
    }

    pub fn start_compaction(&self) {
        #[cfg(not(feature = "linux"))]
        debug!("Background compaction started");
        let mut compaction_in_progress = self.compaction_in_progress.lock().unwrap();
        *compaction_in_progress = true;
    }

    pub fn notify_gc_finished(&self) {
        let mut gc_in_progress = self.gc_in_progress.lock().unwrap();
        *gc_in_progress = false;
        self.gc_condvar.notify_all();
    }

    pub fn notify_compaction_finished(&self) {
        #[cfg(not(feature = "linux"))]
        debug!("Background compaction finished");
        let mut compaction_in_progress = self.compaction_in_progress.lock().unwrap();
        *compaction_in_progress = false;
        self.compaction_condvar.notify_all();
    }
}
