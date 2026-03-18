use std::path::Path;

use chunksplitter::verify_round_trip;

const TEST_WORLDS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../test_worlds");

/// One test per subdirectory found in test_worlds/.
/// Each subdirectory must be a valid Bedrock world (contains a `db/` folder).
#[test]
fn round_trip_all_worlds() {
    let dir = Path::new(TEST_WORLDS_DIR);
    if !dir.exists() {
        eprintln!("test_worlds/ not found, skipping (must run from the repository)");
        return;
    }

    let mut worlds: Vec<_> = std::fs::read_dir(dir)
        .expect("read test_worlds/")
        .filter_map(|e| {
            let path = e.ok()?.path();
            if path.is_dir() && path.join("db").exists() {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    worlds.sort();
    assert!(!worlds.is_empty(), "no valid worlds found in test_worlds/");

    let mut failures = Vec::new();
    for world in &worlds {
        let name = world.file_name().unwrap().to_string_lossy();
        print!("  round-trip {name} ... ");
        match verify_round_trip(world, &mut |_| {}) {
            Ok(report) => println!("ok ({} pairs)", report.pairs_checked),
            Err(e) => {
                println!("FAILED: {e}");
                failures.push(format!("{name}: {e}"));
            }
        }
    }

    if !failures.is_empty() {
        panic!("Round-trip failures:\n{}", failures.join("\n"));
    }
}
