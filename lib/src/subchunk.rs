//! Bedrock Edition palette-based subchunk codec (tag 47).
//!
//! Binary layout (versions 8 and 9):
//!   [version: u8]
//!   [num_storages: u8]          -- present in both v8 and v9
//!   [y_index: i8]               -- version 9 ONLY: Y-section index (e.g. -4..20 for 1.18+)
//!   for each storage:
//!     [bpb_byte: u8]            -- bits_per_block = bpb_byte >> 1; is_runtime = bpb_byte & 1
//!     [words: u32 LE * n]       -- packed block indices (omitted when bits_per_block == 0)
//!     [palette_count: i32 LE]
//!     [TAG_Compound] * palette_count
//!
//! Blocks are addressed by `(x * 16 + z) * 16 + y` (all 0–15).
//! Block `i` lives in word `i / bpw` at bit offset `(i % bpw) * bpb`,
//! where `bpw = 32 / bpb`.
//!
//! JSON format produced by this module:
//! ```json
//! {
//!   "version": 9,
//!   "layers": [
//!     {
//!       "bits_per_block": 4,
//!       "palette": [ { "name": {"string": "minecraft:stone"}, "states": {"compound": {…}} }, … ],
//!       "blocks": [ 0, 1, 0, … ]   // 4096 palette indices
//!     }
//!   ]
//! }
//! ```
//! Palette entries use the same NBT-tagged JSON representation as the rest of
//! the codebase so they round-trip exactly.
//!
//! Unrecognised versions are preserved verbatim as `{"raw": "<hex>"}`.

use std::io::{Cursor, Read};

use anyhow::{Result, bail};
use serde_json::{Value, json};

use crate::nbt;

const NUM_BLOCKS: usize = 4096; // 16 × 16 × 16

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Decode a subchunk binary blob to a JSON object.
pub fn decode_subchunk(data: &[u8]) -> Result<Value> {
    if data.is_empty() {
        return Ok(json!({"version": 0, "layers": []}));
    }
    match data[0] {
        8 | 9 => decode_palette(data),
        _ => Ok(json!({"raw": hex::encode(data)})),
    }
}

/// Encode a subchunk JSON object (produced by [`decode_subchunk`]) back to binary.
pub fn encode_subchunk(json: &Value) -> Result<Vec<u8>> {
    if let Some(raw) = json.get("raw").and_then(|v| v.as_str()) {
        return hex::decode(raw).map_err(|e| anyhow::anyhow!("hex decode: {e}"));
    }

    let version = json
        .get("version")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow::anyhow!("subchunk missing 'version'"))? as u8;

    let layers = json
        .get("layers")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("subchunk missing 'layers'"))?;

    let mut out = Vec::new();
    out.push(version);
    out.push(layers.len() as u8);
    if version >= 9 {
        let y_index = json
            .get("y_index")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| anyhow::anyhow!("v9 subchunk missing 'y_index'"))? as i8;
        out.push(y_index as u8);
    }
    for layer in layers {
        encode_layer(&mut out, layer)?;
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Decode
// ---------------------------------------------------------------------------

fn decode_palette(data: &[u8]) -> Result<Value> {
    let mut cur = Cursor::new(data);
    let version = read_u8(&mut cur)?;
    let num_storages = read_u8(&mut cur)? as usize;

    // Version 9 adds a y_index byte (signed, e.g. -4..20 for 1.18+ worlds).
    let y_index: Option<i8> = if version >= 9 {
        Some(read_u8(&mut cur)? as i8)
    } else {
        None
    };

    let mut layers = Vec::with_capacity(num_storages);
    for _ in 0..num_storages {
        layers.push(decode_layer(&mut cur)?);
    }

    let mut obj = serde_json::Map::new();
    obj.insert("version".into(), version.into());
    if let Some(y) = y_index {
        obj.insert("y_index".into(), (y as i64).into());
    }
    obj.insert("layers".into(), Value::Array(layers));
    Ok(Value::Object(obj))
}

fn decode_layer(cur: &mut Cursor<&[u8]>) -> Result<Value> {
    let bpb_byte = read_u8(cur)?;
    let bpb = (bpb_byte >> 1) as usize;

    let indices: Vec<u32> = if bpb == 0 {
        vec![0; NUM_BLOCKS]
    } else {
        let bpw = 32 / bpb;
        let word_count = (NUM_BLOCKS + bpw - 1) / bpw;
        let mask = (1u32 << bpb) - 1;

        let mut words = Vec::with_capacity(word_count);
        for _ in 0..word_count {
            words.push(read_le_u32(cur)?);
        }

        (0..NUM_BLOCKS)
            .map(|i| (words[i / bpw] >> ((i % bpw) * bpb)) & mask)
            .collect()
    };

    let palette_count = read_le_i32(cur)?;
    if !(0..=4096).contains(&palette_count) {
        bail!("implausible palette count: {palette_count}");
    }
    let mut palette = Vec::with_capacity(palette_count as usize);
    for _ in 0..palette_count {
        palette.push(nbt::read_raw_nbt_cursor(cur)?);
    }

    let blocks: Vec<Value> = indices.into_iter().map(|i| i.into()).collect();
    Ok(json!({"bits_per_block": bpb, "palette": palette, "blocks": blocks}))
}

// ---------------------------------------------------------------------------
// Encode
// ---------------------------------------------------------------------------

fn encode_layer(out: &mut Vec<u8>, layer: &Value) -> Result<()> {
    let bpb = layer
        .get("bits_per_block")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow::anyhow!("layer missing 'bits_per_block'"))? as usize;

    let palette = layer
        .get("palette")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("layer missing 'palette'"))?;

    let blocks = layer
        .get("blocks")
        .and_then(|v| v.as_array())
        .ok_or_else(|| anyhow::anyhow!("layer missing 'blocks'"))?;

    // bpb_byte: high 7 bits = bpb, low bit = is_runtime (0 = persisted)
    out.push((bpb as u8) << 1);

    if bpb > 0 {
        if blocks.len() != NUM_BLOCKS {
            bail!("expected {NUM_BLOCKS} blocks, got {}", blocks.len());
        }
        let bpw = 32 / bpb;
        let word_count = (NUM_BLOCKS + bpw - 1) / bpw;
        let mask = (1u32 << bpb) - 1;

        let mut words = vec![0u32; word_count];
        for (i, block) in blocks.iter().enumerate() {
            let idx = block
                .as_u64()
                .ok_or_else(|| anyhow::anyhow!("block index must be integer"))? as u32;
            words[i / bpw] |= (idx & mask) << ((i % bpw) * bpb);
        }
        for w in words {
            out.extend_from_slice(&w.to_le_bytes());
        }
    }

    out.extend_from_slice(&(palette.len() as i32).to_le_bytes());
    for entry in palette {
        out.extend_from_slice(&nbt::write_raw_nbt(entry)?);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Cursor helpers
// ---------------------------------------------------------------------------

fn read_u8(cur: &mut Cursor<&[u8]>) -> Result<u8> {
    let mut buf = [0u8; 1];
    cur.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn read_le_u32(cur: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut buf = [0u8; 4];
    cur.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_le_i32(cur: &mut Cursor<&[u8]>) -> Result<i32> {
    let mut buf = [0u8; 4];
    cur.read_exact(&mut buf)?;
    Ok(i32::from_le_bytes(buf))
}
