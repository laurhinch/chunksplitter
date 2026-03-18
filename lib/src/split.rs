use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use rayon::prelude::*;

use anyhow::{Context, Result};
use serde_json::{Value, json};

use crate::ProgressEvent;
use crate::keys::{BedrockKey, dim_dir, is_nbt_tag, is_scalar_tag, tag_name};
use crate::ldb;
use crate::nbt;

pub fn split(world_path: &Path, out_path: &Path, cb: &mut dyn FnMut(ProgressEvent)) -> Result<()> {
    fs::create_dir_all(out_path).context("Failed to create output directory")?;

    cb(ProgressEvent::Phase("Copying files"));
    split_plain_files(world_path, out_path)?;
    split_level_dat(world_path, out_path)?;

    split_db(world_path, out_path, cb)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Plain files
// ---------------------------------------------------------------------------

fn split_plain_files(world: &Path, out: &Path) -> Result<()> {
    for name in &[
        "levelname.txt",
        "world_behavior_packs.json",
        "world_resource_packs.json",
    ] {
        let src = world.join(name);
        if src.exists() {
            fs::copy(&src, out.join(name)).with_context(|| format!("Failed to copy {name}"))?;
        }
    }
    Ok(())
}

fn split_level_dat(world: &Path, out: &Path) -> Result<()> {
    let data = fs::read(world.join("level.dat")).context("Failed to read level.dat")?;
    let json = nbt::level_dat_to_json(&data).context("Failed to decode level.dat")?;
    fs::write(
        out.join("level.dat.json"),
        serde_json::to_string_pretty(&json)?,
    )
    .context("Failed to write level.dat.json")
}

// ---------------------------------------------------------------------------
// LevelDB
// ---------------------------------------------------------------------------

fn split_db(world: &Path, out: &Path, cb: &mut dyn FnMut(ProgressEvent)) -> Result<()> {
    let db_out = out.join("db");
    fs::create_dir_all(&db_out)?;

    let mut db = ldb::open_world_db(&world.join("db"))?;

    cb(ProgressEvent::Phase("Reading database"));
    let pairs = ldb::read_all(&mut db)?;

    // Pass 1: collect digp and actorprefix entries.
    let mut digp_map: HashMap<(i32, i32), Vec<[u8; 8]>> = HashMap::new();
    let mut actor_map: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

    for (raw_key, value) in &pairs {
        match BedrockKey::parse(raw_key) {
            BedrockKey::ActorDigp { x, z } => {
                let ids: Vec<[u8; 8]> = value
                    .chunks_exact(8)
                    .map(|c| c.try_into().unwrap())
                    .collect();
                digp_map.insert((x, z), ids);
            }
            BedrockKey::Actor(id) => {
                actor_map.insert(id, value.clone());
            }
            _ => {}
        }
    }

    // Build chunk_actors in digp order so the reconstructed digp byte sequence
    // matches exactly (HashMap iteration order is non-deterministic).
    let mut chunk_actors: HashMap<(i32, i32), serde_json::Map<String, Value>> = HashMap::new();
    let mut seen_ids: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();

    for ((x, z), ids) in &digp_map {
        if ids.is_empty() {
            continue;
        }
        let mut actors_map = serde_json::Map::new();
        for id in ids {
            seen_ids.insert(id.to_vec());
            if let Some(raw_value) = actor_map.get(id.as_ref() as &[u8]) {
                let nbt_value = nbt::read_raw_nbt(raw_value)
                    .unwrap_or_else(|_| Value::String(hex_encode(raw_value)));
                actors_map.insert(hex_encode(id), nbt_value);
            }
        }
        if !actors_map.is_empty() {
            chunk_actors.insert((*x, *z), actors_map);
        }
    }

    // Orphan actors: present in actor_map but not referenced by any digp.
    let mut orphan_actors = serde_json::Map::new();
    for (id, raw_value) in &actor_map {
        if !seen_ids.contains(id) {
            let nbt_value = nbt::read_raw_nbt(raw_value)
                .unwrap_or_else(|_| Value::String(hex_encode(raw_value)));
            orphan_actors.insert(hex_encode(id), nbt_value);
        }
    }

    // Compact list of chunk coords with an empty digp entry.
    let empty_digp: Vec<[i32; 2]> = {
        let mut v: Vec<[i32; 2]> = digp_map
            .iter()
            .filter(|(_, ids)| ids.is_empty())
            .map(|((x, z), _)| [*x, *z])
            .collect();
        v.sort();
        v
    };

    // Pass 2: build per-chunk JSON maps and collect misc entries.
    let mut chunk_data: HashMap<(i32, i32, i32), serde_json::Map<String, Value>> = HashMap::new();
    let mut misc_named: Vec<(String, Value)> = Vec::new();
    let mut misc_binary: Vec<Value> = Vec::new();

    cb(ProgressEvent::Phase("Processing entries"));
    cb(ProgressEvent::Total(pairs.len() as u64));

    for (raw_key, value) in &pairs {
        cb(ProgressEvent::Advance(1));
        match BedrockKey::parse(raw_key) {
            BedrockKey::Actor(_) | BedrockKey::ActorDigp { .. } => {}

            BedrockKey::Chunk {
                x,
                z,
                dim,
                tag,
                subchunk,
            } => {
                let chunk = chunk_data.entry((x, z, dim)).or_default();

                if tag == 47 {
                    // Subchunk: store under "sub" keyed by Y index.
                    let subs = chunk
                        .entry("sub".to_string())
                        .or_insert_with(|| Value::Object(serde_json::Map::new()))
                        .as_object_mut()
                        .unwrap();
                    subs.insert(
                        subchunk.unwrap().to_string(),
                        Value::String(hex_encode(value)),
                    );
                } else if is_scalar_tag(tag) {
                    chunk.insert(tag_name(tag).to_string(), scalar_to_json(value));
                } else if is_nbt_tag(tag) {
                    let decoded = nbt::read_nbt_sequence(value).unwrap_or_default();
                    chunk.insert(tag_name(tag).to_string(), Value::Array(decoded));
                } else {
                    chunk.insert(tag_name(tag).to_string(), Value::String(hex_encode(value)));
                }
            }

            BedrockKey::Named(name) => {
                let val = decode_named_to_json(&name, value);
                misc_named.push((name, val));
            }

            BedrockKey::Binary(key_bytes) => {
                misc_binary.push(json!({
                    "key": hex_encode(&key_bytes),
                    "value": hex_encode(value),
                }));
            }
        }
    }

    // Fold actors into chunk data.
    for ((x, z), actors) in chunk_actors {
        let chunk = chunk_data.entry((x, z, 0)).or_default();
        chunk.insert("actors".to_string(), Value::Object(actors));
    }

    // Write one JSON file per chunk.
    cb(ProgressEvent::Phase("Writing chunks"));
    cb(ProgressEvent::Total(chunk_data.len() as u64));

    // Serialize all chunks in parallel (CPU-bound), then write sequentially.
    let write_jobs: Vec<(PathBuf, String)> = chunk_data
        .par_iter()
        .map(|((x, z, dim), data)| -> Result<(PathBuf, String)> {
            let path = db_out.join(dim_dir(*dim)).join(format!("{x}_{z}.json"));
            let json = serde_json::to_string_pretty(data)?;
            Ok((path, json))
        })
        .collect::<Result<_>>()?;

    // Create output dirs (serial, only a handful of unique dirs).
    let mut created_dirs: HashSet<PathBuf> = HashSet::new();
    for (path, _) in &write_jobs {
        if let Some(dir) = path.parent()
            && created_dirs.insert(dir.to_path_buf())
        {
            fs::create_dir_all(dir)?;
        }
    }

    for (path, json) in &write_jobs {
        fs::write(path, json)?;
        cb(ProgressEvent::Advance(1));
    }

    // Write misc files.
    let misc_dir = db_out.join("misc");

    if !empty_digp.is_empty() {
        fs::create_dir_all(&misc_dir)?;
        fs::write(
            misc_dir.join("empty_digp.json"),
            serde_json::to_string_pretty(&json!(empty_digp))?,
        )?;
    }

    if !orphan_actors.is_empty() {
        fs::create_dir_all(&misc_dir)?;
        fs::write(
            misc_dir.join("orphan_actors.json"),
            serde_json::to_string_pretty(&Value::Object(orphan_actors))?,
        )?;
    }

    for (name, val) in misc_named {
        fs::create_dir_all(&misc_dir)?;
        let safe = safe_filename(&name);
        fs::write(
            misc_dir.join(format!("{safe}.json")),
            serde_json::to_string_pretty(&val)?,
        )?;
    }

    if !misc_binary.is_empty() {
        fs::create_dir_all(&misc_dir)?;
        fs::write(
            misc_dir.join("binary_keys.json"),
            serde_json::to_string_pretty(&Value::Array(misc_binary))?,
        )?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub fn hex_encode(bytes: &[u8]) -> String {
    hex::encode(bytes)
}

fn scalar_to_json(bytes: &[u8]) -> Value {
    let n: u64 = match bytes.len() {
        1 => bytes[0] as u64,
        2 => u16::from_le_bytes(bytes.try_into().unwrap()) as u64,
        4 => u32::from_le_bytes(bytes.try_into().unwrap()) as u64,
        8 => u64::from_le_bytes(bytes.try_into().unwrap()),
        _ => return Value::String(hex_encode(bytes)),
    };
    json!(n)
}

/// Attempt to decode a named-key value as NBT. Falls back to {"raw": "hex"}.
fn decode_named_to_json(name: &str, value: &[u8]) -> Value {
    if let Ok(nbt_val) = nbt::read_raw_nbt(value) {
        nbt_val
    } else {
        let _ = name; // name kept for potential future use
        json!({ "raw": hex_encode(value) })
    }
}

fn safe_filename(name: &str) -> String {
    name.chars()
        .map(|c| {
            if matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|') {
                '_'
            } else {
                c
            }
        })
        .collect()
}
