#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::path::Path;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::{BufMut, Bytes};

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::<u8>::new(),
            last_key: Vec::<u8>::new(),
            data: Vec::<u8>::new(),
            meta: Vec::<BlockMeta>::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if !self.builder.add(key, value) {
            let full_block = {
                let new_builder = BlockBuilder::new(self.block_size);
                std::mem::replace(&mut self.builder, new_builder)
            }
            .build();
            let data_block = full_block.encode();
            self.data.append(&mut data_block.to_vec());
            let meta = BlockMeta {
                offset: self.meta.len() * self.block_size,
                first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.first_key)),
                last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.last_key)),
            };
            self.meta.push(meta);

            self.first_key.clear();
            let _ = self.builder.add(key, value);
        }

        self.last_key.clear();
        if self.first_key.is_empty() {
            self.first_key = key.raw_ref().to_vec();
        }
        self.last_key = key.raw_ref().to_vec();
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.meta.len() * self.block_size
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if !self.builder.is_empty() {
            let block = self.builder.build().encode();
            self.data.append(&mut block.to_vec());
            let meta = BlockMeta {
                offset: self.meta.len() * self.block_size,
                first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.first_key)),
                last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.last_key)),
            };
            self.meta.push(meta);
        }

        let mut buf = self.data;
        let meta_offset = buf.len() as u32;
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        buf.put_u32(meta_offset);
        let first_key = match self.meta.first() {
            Some(meta) => meta.first_key.clone(),
            None => KeyBytes::from_bytes(Bytes::new()),
        };
        let last_key = match self.meta.last() {
            Some(meta) => meta.last_key.clone(),
            None => KeyBytes::from_bytes(Bytes::new()),
        };
        let file = FileObject::create(path.as_ref(), buf)?;
        let sst = SsTable {
            file,
            block_meta_offset: self.meta.len() * self.block_size,
            id,
            block_cache,
            first_key,
            last_key,
            block_meta: self.meta,
            bloom: None,
            max_ts: 0,
        };
        Ok(sst)
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
