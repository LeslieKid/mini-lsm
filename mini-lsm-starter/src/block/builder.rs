#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::mem::size_of;

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block.
    first_key: KeyVec,
    /// The current occupied size.
    current_size: usize,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: Vec::<u16>::new(),
            data: Vec::<u8>::new(),
            block_size,
            first_key: KeyVec::new(),
            // the initial size of the block is the size of the `elem_num`
            current_size: size_of::<u16>(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let key_len = key.len() as u16;
        let val_len = value.len() as u16;
        // key_len + value_len + sizeof(offset) + sizeof(key_len) + sizeof(val_len)
        let other = (key_len + val_len + (size_of::<u16>() * 3) as u16) as usize;
        if self.full_after_add(other) && !self.is_empty() {
            return false;
        }

        // Hint: the op for offsets should be executed before the op for data
        self.offsets.push(self.data.len() as u16);
        self.data.put_u16(key_len);
        self.data.append(&mut key.raw_ref().to_vec());
        self.data.put_u16(val_len);
        self.data.append(&mut value.to_vec());
        self.current_size += other;

        if self.first_key.is_empty() {
            self.first_key.append(key.raw_ref());
        }
        true
    }

    fn full_after_add(&self, other: usize) -> bool {
        (self.current_size + other) > self.block_size
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
