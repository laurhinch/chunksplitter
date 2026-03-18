mod keys;
mod ldb;
mod merge;
mod nbt;
mod split;

pub use merge::merge;
pub use split::split;

pub enum ProgressEvent {
    /// Starting a new named phase (resets the current bar).
    Phase(&'static str),
    /// Total number of steps for the current phase (switches to a bar).
    Total(u64),
    /// Advance the current phase by N steps.
    Advance(u64),
}

// ---------------------------------------------------------------------------
// Round-trip verification
// ---------------------------------------------------------------------------

pub struct RoundTripReport {
    pub pairs_checked: usize,
}

/// Split `world` to a temp directory, merge it back, then verify that every
/// original LevelDB key-value pair is present and byte-identical in the
/// reconstructed database.  Returns `Err` on any mismatch.
pub fn verify_round_trip(
    world: &std::path::Path,
    cb: &mut dyn FnMut(ProgressEvent),
) -> anyhow::Result<RoundTripReport> {
    // Use the parent of the world directory so temp files land on the same
    // filesystem as the source data, avoiding /tmp quota limits.
    let temp_base = world.parent().unwrap_or(std::path::Path::new("."));
    let tmp = tempfile::Builder::new().tempdir_in(temp_base)?;
    let split_dir = tmp.path().join("split");
    let merged_dir = tmp.path().join("merged");

    split(world, &split_dir, cb)?;
    merge(&split_dir, &merged_dir, cb)?;

    let mut original_db = ldb::open_world_db(&world.join("db"))?;
    let original_pairs = ldb::read_all(&mut original_db)?;

    let mut reconstructed_db = ldb::open_world_db(&merged_dir.join("db"))?;
    let reconstructed_pairs = ldb::read_all(&mut reconstructed_db)?;

    let orig_map: std::collections::HashMap<Vec<u8>, Vec<u8>> =
        original_pairs.into_iter().collect();
    let reco_map: std::collections::HashMap<Vec<u8>, Vec<u8>> =
        reconstructed_pairs.into_iter().collect();

    anyhow::ensure!(
        orig_map.len() == reco_map.len(),
        "DB entry count mismatch: original={} reconstructed={}",
        orig_map.len(),
        reco_map.len(),
    );

    for (key, orig_val) in &orig_map {
        let reco_val = reco_map
            .get(key)
            .ok_or_else(|| anyhow::anyhow!("Key missing from reconstructed DB: {key:?}"))?;
        anyhow::ensure!(orig_val == reco_val, "Value mismatch for key {key:?}");
    }

    Ok(RoundTripReport { pairs_checked: orig_map.len() })
}
