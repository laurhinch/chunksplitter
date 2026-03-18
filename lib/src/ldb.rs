use std::io::{Read, Write};
use std::path::Path;
use std::rc::Rc;

use anyhow::Result;
use flate2::{
    Compression,
    read::{DeflateDecoder, ZlibDecoder},
    write::{DeflateEncoder, ZlibEncoder},
};
use rusty_leveldb::{Compressor, CompressorList, DB, LdbIterator, Options, Status, StatusCode};

// Bedrock LevelDB compression type IDs:
//   0 = none (standard)
//   1 = snappy (standard)
//   2 = zlib with header
//   4 = raw deflate (no zlib header) - what current Bedrock worlds use
const ZLIB_ID: u8 = 2;
const DEFLATE_RAW_ID: u8 = 4;

// The ID we use when writing new databases (match what Bedrock currently writes).
const WRITE_COMPRESSOR_ID: u8 = DEFLATE_RAW_ID;

struct NoneCompressor;
impl Compressor for NoneCompressor {
    fn encode(&self, block: Vec<u8>) -> rusty_leveldb::Result<Vec<u8>> {
        Ok(block)
    }
    fn decode(&self, block: Vec<u8>) -> rusty_leveldb::Result<Vec<u8>> {
        Ok(block)
    }
}

struct ZlibCompressor;
impl Compressor for ZlibCompressor {
    fn encode(&self, block: Vec<u8>) -> rusty_leveldb::Result<Vec<u8>> {
        let mut enc = ZlibEncoder::new(Vec::new(), Compression::default());
        enc.write_all(&block).map_err(compress_err)?;
        enc.finish().map_err(compress_err)
    }
    fn decode(&self, block: Vec<u8>) -> rusty_leveldb::Result<Vec<u8>> {
        let mut out = Vec::new();
        ZlibDecoder::new(&block[..])
            .read_to_end(&mut out)
            .map_err(compress_err)?;
        Ok(out)
    }
}

struct RawDeflateCompressor;
impl Compressor for RawDeflateCompressor {
    fn encode(&self, block: Vec<u8>) -> rusty_leveldb::Result<Vec<u8>> {
        let mut enc = DeflateEncoder::new(Vec::new(), Compression::fast());
        enc.write_all(&block).map_err(compress_err)?;
        enc.finish().map_err(compress_err)
    }
    fn decode(&self, block: Vec<u8>) -> rusty_leveldb::Result<Vec<u8>> {
        let mut out = Vec::new();
        DeflateDecoder::new(&block[..])
            .read_to_end(&mut out)
            .map_err(compress_err)?;
        Ok(out)
    }
}

fn compress_err(e: impl std::fmt::Display) -> Status {
    Status {
        code: StatusCode::CompressionError,
        err: e.to_string(),
    }
}

pub fn make_compressor_list() -> Rc<CompressorList> {
    let mut list = CompressorList::new();
    list.set_with_id(0, NoneCompressor);
    list.set_with_id(ZLIB_ID, ZlibCompressor);
    list.set_with_id(DEFLATE_RAW_ID, RawDeflateCompressor);
    Rc::new(list)
}

/// Open an existing Bedrock LevelDB for reading.
pub fn open_world_db(path: &Path) -> Result<DB> {
    let opts = Options {
        compressor: 0,
        compressor_list: make_compressor_list(),
        create_if_missing: false,
        ..Default::default()
    };
    DB::open(path, opts)
        .map_err(|e| anyhow::anyhow!("Failed to open LevelDB at {}: {e}", path.display()))
}

/// Create a new LevelDB and write with raw deflate compression (matching current Bedrock format).
pub fn create_world_db(path: &Path) -> Result<DB> {
    let opts = Options {
        compressor: WRITE_COMPRESSOR_ID,
        compressor_list: make_compressor_list(),
        create_if_missing: true,
        error_if_exists: false,
        // Large write buffer defers compaction, reducing total compression work.
        write_buffer_size: 64 * 1024 * 1024,
        ..Default::default()
    };
    DB::open(path, opts)
        .map_err(|e| anyhow::anyhow!("Failed to create LevelDB at {}: {e}", path.display()))
}

/// Iterate all key-value pairs in the database.
/// Returns them sorted by raw key bytes (LevelDB's natural order).
pub fn read_all(db: &mut DB) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut iter = db
        .new_iter()
        .map_err(|e| anyhow::anyhow!("Iterator failed: {e}"))?;
    iter.seek_to_first();

    let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    while iter.valid() {
        if let Some((k, v)) = iter.current() {
            pairs.push((k.to_vec(), v.to_vec()));
        }
        iter.advance();
    }
    Ok(pairs)
}
