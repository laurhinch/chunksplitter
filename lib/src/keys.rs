/// Parsed representation of a Bedrock LevelDB key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BedrockKey {
    /// Chunk data key: x/z/dim/tag[/subchunk]
    Chunk {
        x: i32,
        z: i32,
        /// 0 = overworld, 1 = nether, 2 = the_end
        dim: i32,
        tag: u8,
        /// Only Some for tag 47 (SubChunkPrefix)
        subchunk: Option<i8>,
    },
    /// `actorprefix<8-byte-id>` - entity NBT stored globally
    Actor(Vec<u8>),
    /// `digp<x4><z4>` - maps a chunk to its actor IDs
    ActorDigp { x: i32, z: i32 },
    /// Plain printable-ASCII key (e.g. `~local_player`, `BiomeData`)
    Named(String),
    /// Anything else
    Binary(Vec<u8>),
}

const ACTOR_PREFIX: &[u8] = b"actorprefix";
const DIGP_PREFIX: &[u8] = b"digp";

impl BedrockKey {
    pub fn parse(key: &[u8]) -> Self {
        // actorprefix<id>
        if key.starts_with(ACTOR_PREFIX) {
            return BedrockKey::Actor(key[ACTOR_PREFIX.len()..].to_vec());
        }
        // digp<x4><z4>  (always 12 bytes)
        if key.starts_with(DIGP_PREFIX) && key.len() == 12 {
            let x = i32::from_le_bytes(key[4..8].try_into().unwrap());
            let z = i32::from_le_bytes(key[8..12].try_into().unwrap());
            return BedrockKey::ActorDigp { x, z };
        }
        // Chunk key
        if let Some(ck) = parse_chunk_key(key) {
            return ck;
        }
        // Only treat as Named if every byte is printable ASCII (0x20–0x7E).
        // Keys like `actorprefix<binary_id>` have a non-printable suffix and must
        // be treated as Binary so we can reconstruct them exactly.
        if let Ok(s) = std::str::from_utf8(key)
            && s.bytes().all(|b| (0x20..0x7F).contains(&b))
        {
            return BedrockKey::Named(s.to_owned());
        }
        BedrockKey::Binary(key.to_vec())
    }

    pub fn to_raw_bytes(&self) -> Vec<u8> {
        match self {
            BedrockKey::Chunk {
                x,
                z,
                dim,
                tag,
                subchunk,
            } => {
                let mut out = Vec::with_capacity(14);
                out.extend_from_slice(&x.to_le_bytes());
                out.extend_from_slice(&z.to_le_bytes());
                if *dim != 0 {
                    out.extend_from_slice(&dim.to_le_bytes());
                }
                out.push(*tag);
                if let Some(y) = subchunk {
                    out.push(*y as u8);
                }
                out
            }
            BedrockKey::Actor(id) => {
                let mut out = ACTOR_PREFIX.to_vec();
                out.extend_from_slice(id);
                out
            }
            BedrockKey::ActorDigp { x, z } => {
                let mut out = DIGP_PREFIX.to_vec();
                out.extend_from_slice(&x.to_le_bytes());
                out.extend_from_slice(&z.to_le_bytes());
                out
            }
            BedrockKey::Named(s) => s.as_bytes().to_vec(),
            BedrockKey::Binary(b) => b.clone(),
        }
    }
}

fn parse_chunk_key(key: &[u8]) -> Option<BedrockKey> {
    let len = key.len();

    // Overworld: 9 bytes (no subchunk) or 10 bytes (with subchunk)
    if len == 9 || len == 10 {
        let x = i32::from_le_bytes(key[0..4].try_into().ok()?);
        let z = i32::from_le_bytes(key[4..8].try_into().ok()?);
        let tag = key[8];
        if !is_chunk_tag(tag) {
            return None;
        }
        let subchunk = if len == 10 {
            if tag != 47 {
                return None;
            }
            Some(key[9] as i8)
        } else {
            None
        };
        return Some(BedrockKey::Chunk {
            x,
            z,
            dim: 0,
            tag,
            subchunk,
        });
    }

    // Non-overworld: 13 bytes (no subchunk) or 14 bytes (with subchunk)
    if len == 13 || len == 14 {
        let x = i32::from_le_bytes(key[0..4].try_into().ok()?);
        let z = i32::from_le_bytes(key[4..8].try_into().ok()?);
        let dim = i32::from_le_bytes(key[8..12].try_into().ok()?);
        let tag = key[12];
        if !is_chunk_tag(tag) {
            return None;
        }
        let subchunk = if len == 14 {
            if tag != 47 {
                return None;
            }
            Some(key[13] as i8)
        } else {
            None
        };
        return Some(BedrockKey::Chunk {
            x,
            z,
            dim,
            tag,
            subchunk,
        });
    }

    None
}

fn is_chunk_tag(tag: u8) -> bool {
    (43..=65).contains(&tag) || tag == 118
}

// ---------------------------------------------------------------------------
// File path helpers
// ---------------------------------------------------------------------------

pub fn dim_dir(dim: i32) -> String {
    match dim {
        0 => "overworld".to_string(),
        1 => "nether".to_string(),
        2 => "the_end".to_string(),
        n => format!("dim_{n}"),
    }
}

/// Tags whose values are small integers stored as human-readable decimal text.
pub fn is_scalar_tag(tag: u8) -> bool {
    matches!(tag, 44 | 54 | 60 | 65)
}

/// Tags whose values are NBT and should be stored as JSON.
pub fn is_nbt_tag(tag: u8) -> bool {
    matches!(tag, 49..=51)
}

pub fn tag_name(tag: u8) -> &'static str {
    match tag {
        43 => "data3d",
        44 => "version",
        45 => "data2d",
        46 => "data2d_legacy",
        47 => "sub",
        48 => "terrain_legacy",
        49 => "block_entities",
        50 => "entities",
        51 => "pending_ticks",
        52 => "block_extra_data",
        53 => "biome_state",
        54 => "finalized",
        55 => "conversion_data",
        56 => "border_blocks",
        57 => "hardcoded_spawners",
        58 => "random_ticks",
        59 => "checksums",
        60 => "generation_seed",
        61 => "pre_caves_blending",
        62 => "blending_biome_height",
        63 => "metadata_hash",
        64 => "blending_data",
        65 => "actor_digest_version",
        118 => "chunk_metadata_dict",
        _ => "unknown",
    }
}
