use std::{any::Any, fmt::Debug, marker::PhantomData, os::fd::RawFd, sync::Arc};

use crate::{
    Device, IoEngine, IoHandle,
    io::bytes::{IoBuf, IoBufMut},
};

// struct ByteAddress {
//     pos: u64,
// }

// struct ObjectAddress {
//     name: String,
// }

// struct FileAddress {
//     fd: RawFd,
//     offset: u64,
// }

// pub trait Engine: Send + Sync + Any + 'static {
//     type Address;
// }

// pub trait Device<L, P>: Send + Sync + Any + 'static {}

// struct FileDevice;

// struct FsDevice;

// impl Device<ByteAddress, FileAddress> for FileDevice {}

// impl Device<ObjectAddress, ByteAddress> for FsDevice {}

// pub trait IoEngine: Send + Sync + Any + 'static {
//     type Address;
// }

// fn ensure_object_safe(_: Box<dyn Engine<Address = ByteAddress>>) {}

pub type SegmentId = u64;

pub trait Segment: Debug + Send + Sync + Any + 'static {
    fn id(&self) -> SegmentId;
    fn size(&self) -> usize;

    fn read(&self, data: Box<dyn IoBufMut>) -> IoHandle;
    fn write(&self, data: Box<dyn IoBuf>) -> IoHandle;
}

pub trait RandomAccessSegment: Segment {
    fn read_at(&self, offset: u64, data: Box<dyn IoBufMut>) -> IoHandle;
    fn write_at(&self, offset: u64, data: Box<dyn IoBuf>) -> IoHandle;
}

pub trait Storage: Send + Sync + Any + 'static {
    fn segments(&self) -> Vec<SegmentId>;
    fn create_segment(&self) -> Arc<dyn Segment>;
    fn remove_segment(&self, segment: SegmentId);
}

pub trait RandomAccessStorage: Storage {
    fn create_random_access_segment(&self) -> Arc<dyn RandomAccessSegment>;
}

pub struct VolumeStorage {
    io_engine: Arc<dyn IoEngine>,
    device: Arc<dyn Device>,
}

pub struct OpenDalStorage {
    opendal: PhantomData<()>,
}
