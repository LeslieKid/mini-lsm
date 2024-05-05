#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{mem::size_of, sync::Arc};

use bytes::Buf;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut block_iter = BlockIterator::new(block);
        block_iter.seek_to_first();
        block_iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut block_iter = BlockIterator::new(block);
        block_iter.seek_to_key(key);
        block_iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        let lower = self.value_range.0;
        let upper = self.value_range.1;
        &self.block.data[lower..upper]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key().is_empty()
    }

    fn seek_to_idx(&mut self, idx: usize) {
        self.idx = idx;
        let block = self.block.clone();
        let key_idx = block.offsets[idx] as usize;
        let mut entry = &block.data[key_idx..];
        let key_len = entry.get_u16() as usize;
        self.key = {
            let key = &entry[..key_len];
            KeyVec::from_vec(key.to_vec())
        };
        entry.advance(key_len);

        let val_len = entry.get_u16() as usize;
        let lower = key_idx + key_len + size_of::<u16>() * 2;
        let upper = lower + val_len;
        self.value_range = (lower, upper);
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_idx(0)
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if !self.is_valid() {
            panic!("the block iterator is invalid.");
        }
        if self.idx == self.block.offsets.len() - 1 {
            // if the block iterator is already move to the last key-value pair,
            // set the key to be empty.
            self.key.clear();
            return;
        }

        self.idx += 1;
        self.seek_to_idx(self.idx);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.seek_to_first();
        while self.is_valid() && !self.key().cmp(&key).is_ge() {
            self.next()
        }
    }
}
