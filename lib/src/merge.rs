use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde_json::Value;

use rayon::prelude::*;

use crate::keys::{BedrockKey, is_nbt_tag, is_scalar_tag};
use crate::ldb;
use crate::nbt;
use crate::ProgressEvent;

pub fn merge(split_path: &Path, world_path: &Path, cb: &mut dyn FnMut(ProgressEvent)) -> Result<()> {
    fs::create_dir_all(world_path).context("Failed to create world directory")?;

    cb(ProgressEvent::Phase("Copying files"));
    merge_plain_files(split_path, world_path)?;
    merge_level_dat(split_path, world_path)?;

    merge_db(split_path, world_path, cb)?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Plain files
// ---------------------------------------------------------------------------

fn merge_plain_files(split: &Path, world: &Path) -> Result<()> {
    for name in &["levelname.txt", "world_behavior_packs.json", "world_resource_packs.json"] {
        let src = split.join(name);
        if src.exists() {
            fs::copy(&src, world.join(name)).with_context(|| format!("Failed to copy {name}"))?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// level.dat
// ---------------------------------------------------------------------------

fn merge_level_dat(split: &Path, world: &Path) -> Result<()> {
    let text = fs::read_to_string(split.join("level.dat.json"))
        .context("Failed to read level.dat.json")?;
    let json: Value = serde_json::from_str(&text)?;
    let bytes = nbt::json_to_level_dat(&json).context("Failed to encode level.dat")?;
    fs::write(world.join("level.dat"), bytes).context("Failed to write level.dat")
}

// ---------------------------------------------------------------------------
// LevelDB
// ---------------------------------------------------------------------------

fn merge_db(split: &Path, world: &Path, cb: &mut dyn FnMut(ProgressEvent)) -> Result<()> {
    let db_split = split.join("db");
    let db_world = world.join("db");
    fs::create_dir_all(&db_world)?;

    let mut db = ldb::create_world_db(&db_world)?;

    cb(ProgressEvent::Phase("Scanning files"));
    let files = sorted_files(&db_split)?;

    // Parallel phase: read + parse every file into raw KV pairs.
    cb(ProgressEvent::Phase("Reading & parsing files"));
    cb(ProgressEvent::Total(files.len() as u64));

    let batches: Vec<Vec<(Vec<u8>, Vec<u8>)>> = files
        .par_iter()
        .map(|abs_path| -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            let rel = abs_path.strip_prefix(&db_split).unwrap();
            let parts: Vec<_> = rel.components().collect();
            match parts.as_slice() {
                [misc, file] if misc.as_os_str() == "misc" => {
                    let fname = file.as_os_str().to_string_lossy();
                    collect_misc_pairs(abs_path, &fname)
                }
                [dim_part, file_part] => {
                    let fname = file_part.as_os_str().to_string_lossy();
                    if let Some(stem) = fname.strip_suffix(".json") {
                        let dim = dim_from_name(&dim_part.as_os_str().to_string_lossy());
                        let (x, z) = parse_chunk_dir(stem)?;
                        collect_chunk_pairs(abs_path, x, z, dim)
                    } else {
                        Ok(vec![])
                    }
                }
                _ => Ok(vec![]),
            }
        })
        .collect::<Result<Vec<_>>>()?;

    // Serial phase: write all pre-decoded KV pairs to LevelDB.
    cb(ProgressEvent::Phase("Writing to database"));
    cb(ProgressEvent::Total(batches.len() as u64));

    for pairs in batches {
        for (key, value) in pairs {
            db.put(&key, &value).map_err(|e| anyhow::anyhow!("{e}"))?;
        }
        cb(ProgressEvent::Advance(1));
    }

    db.flush().map_err(|e| anyhow::anyhow!("Failed to flush DB: {e}"))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Chunk JSON → raw KV pairs
// ---------------------------------------------------------------------------

fn collect_chunk_pairs(abs_path: &Path, x: i32, z: i32, dim: i32) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let text = fs::read_to_string(abs_path)?;
    let map: serde_json::Map<String, Value> = serde_json::from_str(&text)?;
    let mut pairs = Vec::new();

    for (field, val) in &map {
        match field.as_str() {
            "actors" => {
                let actors = val.as_object().context("actors must be an object")?;
                collect_actor_pairs(actors, Some((x, z)), &mut pairs)?;
            }
            "sub" => {
                let subs = val.as_object().context("sub must be an object")?;
                for (y_str, sub_val) in subs {
                    let y: i8 = y_str
                        .parse()
                        .with_context(|| format!("Bad subchunk Y: {y_str}"))?;
                    let key = BedrockKey::Chunk { x, z, dim, tag: 47, subchunk: Some(y) }
                        .to_raw_bytes();
                    let bytes = hex_decode(sub_val.as_str().unwrap_or(""))?;
                    pairs.push((key, bytes));
                }
            }
            name => {
                let tag = name_to_tag(name)?;
                let key = BedrockKey::Chunk { x, z, dim, tag, subchunk: None }.to_raw_bytes();

                if is_scalar_tag(tag) {
                    let n = val.as_u64().with_context(|| format!("Bad scalar for {name}"))?;
                    pairs.push((key, scalar_to_bytes(n, tag)));
                } else if is_nbt_tag(tag) {
                    let arr = val.as_array().cloned().unwrap_or_default();
                    pairs.push((key, nbt::write_nbt_sequence(&arr)?));
                } else {
                    pairs.push((key, hex_decode(val.as_str().unwrap_or(""))?));
                }
            }
        }
    }

    Ok(pairs)
}

// ---------------------------------------------------------------------------
// Misc entry → raw KV pairs
// ---------------------------------------------------------------------------

fn collect_misc_pairs(abs_path: &Path, fname: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
    let mut pairs = Vec::new();

    if fname == "empty_digp.json" {
        let text = fs::read_to_string(abs_path)?;
        let coords: Vec<[i32; 2]> = serde_json::from_str(&text)?;
        for [x, z] in coords {
            pairs.push((BedrockKey::ActorDigp { x, z }.to_raw_bytes(), vec![]));
        }
        return Ok(pairs);
    }

    if fname == "orphan_actors.json" {
        let text = fs::read_to_string(abs_path)?;
        let obj: serde_json::Map<String, Value> = serde_json::from_str(&text)?;
        collect_actor_pairs(&obj, None, &mut pairs)?;
        return Ok(pairs);
    }

    if fname == "binary_keys.json" {
        let text = fs::read_to_string(abs_path)?;
        let entries: Vec<serde_json::Map<String, Value>> = serde_json::from_str(&text)?;
        for entry in entries {
            let key_hex = entry["key"].as_str().unwrap_or("");
            let val_hex = entry["value"].as_str().unwrap_or("");
            pairs.push((hex_decode(key_hex)?, hex_decode(val_hex)?));
        }
        return Ok(pairs);
    }

    if let Some(stem) = fname.strip_suffix(".json") {
        let text = fs::read_to_string(abs_path)?;
        let json: Value = serde_json::from_str(&text)?;

        let raw_bytes = if let Some(hex_str) = json.get("raw").and_then(|v| v.as_str()) {
            hex_decode(hex_str)?
        } else {
            nbt::write_raw_nbt(&json)?
        };

        pairs.push((BedrockKey::Named(stem.to_string()).to_raw_bytes(), raw_bytes));
    }

    Ok(pairs)
}

// ---------------------------------------------------------------------------
// Actor reconstruction
// ---------------------------------------------------------------------------

fn collect_actor_pairs(
    actors: &serde_json::Map<String, Value>,
    chunk: Option<(i32, i32)>,
    pairs: &mut Vec<(Vec<u8>, Vec<u8>)>,
) -> Result<()> {
    let mut actor_ids: Vec<[u8; 8]> = Vec::new();

    for (hex_id, nbt_val) in actors {
        let id_bytes = hex_decode(hex_id)?;
        if id_bytes.len() != 8 {
            bail!("Actor ID must be 8 bytes, got {}", id_bytes.len());
        }
        let id_arr: [u8; 8] = id_bytes.try_into().unwrap();
        actor_ids.push(id_arr);

        let actor_key = BedrockKey::Actor(id_arr.to_vec()).to_raw_bytes();
        pairs.push((actor_key, nbt::write_raw_nbt(nbt_val)?));
    }

    if let Some((x, z)) = chunk {
        let digp_key = BedrockKey::ActorDigp { x, z }.to_raw_bytes();
        let digp_value: Vec<u8> = actor_ids.iter().flat_map(|id| id.iter().copied()).collect();
        pairs.push((digp_key, digp_value));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Filesystem helpers
// ---------------------------------------------------------------------------

fn sorted_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_files(root, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_files(&path, out)?;
        } else {
            out.push(path);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Key reconstruction helpers
// ---------------------------------------------------------------------------

fn dim_from_name(name: &str) -> i32 {
    match name {
        "overworld" => 0,
        "nether" => 1,
        "the_end" => 2,
        other => other.strip_prefix("dim_").and_then(|n| n.parse().ok()).unwrap_or(0),
    }
}

fn parse_chunk_dir(dir: &str) -> Result<(i32, i32)> {
    let sep = dir[1..]
        .find('_')
        .map(|i| i + 1)
        .with_context(|| format!("Invalid chunk name: {dir}"))?;
    let x: i32 = dir[..sep].parse().with_context(|| format!("Bad x in {dir}"))?;
    let z: i32 = dir[sep + 1..].parse().with_context(|| format!("Bad z in {dir}"))?;
    Ok((x, z))
}

fn name_to_tag(name: &str) -> Result<u8> {
    Ok(match name {
        "data3d" => 43,
        "version" => 44,
        "data2d" => 45,
        "data2d_legacy" => 46,
        "terrain_legacy" => 48,
        "block_entities" => 49,
        "entities" => 50,
        "pending_ticks" => 51,
        "block_extra_data" => 52,
        "biome_state" => 53,
        "finalized" => 54,
        "conversion_data" => 55,
        "border_blocks" => 56,
        "hardcoded_spawners" => 57,
        "random_ticks" => 58,
        "checksums" => 59,
        "generation_seed" => 60,
        "pre_caves_blending" => 61,
        "blending_biome_height" => 62,
        "metadata_hash" => 63,
        "blending_data" => 64,
        "actor_digest_version" => 65,
        "chunk_metadata_dict" => 118,
        other => bail!("Unknown tag name: {other}"),
    })
}

fn scalar_to_bytes(n: u64, tag: u8) -> Vec<u8> {
    match tag {
        44 | 65 => vec![n as u8],
        54 => (n as u32).to_le_bytes().to_vec(),
        60 => n.to_le_bytes().to_vec(),
        _ => vec![n as u8],
    }
}

fn hex_decode(s: &str) -> Result<Vec<u8>> {
    hex::decode(s).map_err(|e| anyhow::anyhow!("{e}"))
}
