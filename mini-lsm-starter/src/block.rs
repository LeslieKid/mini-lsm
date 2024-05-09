#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut blk = self.data.clone();
        let off_len = self.offsets.len() as u16;
        for off in self.offsets.iter() {
            blk.put_u16(*off);
        }
        blk.put_u16(off_len);
        blk.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let blk = data.to_vec();
        let blk_len = blk.len();
        let off_len = (&blk[blk_len - 2..]).get_u16() as usize;
        let data_len = (blk_len - 2 - 2 * off_len) as usize;
        let offsets = blk[data_len..blk_len - 2]
            .chunks(2)
            .map(|mut x| x.get_u16())
            .collect();
        Self {
            data: blk[..data_len].to_vec(),
            offsets,
        }
    }
}
