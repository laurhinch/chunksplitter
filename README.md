# ChunkSplitter

Split a Minecraft Bedrock Edition world into a version-control-friendly directory tree, then merge it back into a playable world.

## Why

Bedrock worlds are stored as a LevelDB database. Binary databases don't diff, don't merge, and can't be meaningfully reviewed in a pull request. ChunkSplitter explodes the database into one JSON file per chunk so that:

- Two people editing **different areas** produce changes in different files, no merge conflicts.
- World settings (`level.dat`) become a human-readable JSON file that diffs cleanly.
- Named global data (biomes, scoreboards, player data, etc.) become individual files under `misc/`.

## Output layout

```
world_split/
├── level.dat.json               # world settings (human-readable JSON)
├── levelname.txt
├── world_behavior_packs.json
├── world_resource_packs.json
└── db/
    ├── overworld/
    │   ├── 0_0.json             # one JSON file per chunk (x_z.json)
    │   ├── 0_1.json
    │   └── ...
    ├── nether/
    ├── the_end/
    └── misc/
        ├── empty_digp.json      # chunks with empty entity-digest entries
        ├── orphan_actors.json   # actors not linked to any chunk
        ├── scoreboard.json      # named global entries (scoreboard, player data, etc.)
        └── binary_keys.json     # unrecognised binary-keyed entries
```

## Installation

From crates.io:

```sh
cargo install chunksplitter-cli
```

Or build from source:

```sh
cargo install --path cli
```

Pre-built binaries for Linux (x86_64 / aarch64), macOS (Intel / Apple Silicon), and Windows are attached to each [GitHub release](https://github.com/laurhinch/chunksplitter/releases).

## Usage

```sh
# Split a world before committing to version control
chunksplitter split path/to/world path/to/output

# Reconstruct a world after checking out
chunksplitter merge path/to/output path/to/world

# Verify a world round-trips correctly (split → merge → compare)
chunksplitter test-world path/to/world
```

## Using the library

```toml
[dependencies]
chunksplitter = "0.1"
```

```rust
use std::path::Path;

// The third argument is a progress callback; use |_| {} to ignore it.
chunksplitter::split(Path::new("my_world"), Path::new("split_out"), &mut |_| {})?;
chunksplitter::merge(Path::new("split_out"), Path::new("my_world_restored"), &mut |_| {})?;

// Verify a round-trip (split → merge → byte-for-byte DB comparison)
let report = chunksplitter::verify_round_trip(Path::new("my_world"), &mut |_| {})?;
println!("{} key-value pairs verified", report.pairs_checked);
```

## License

Licensed under the [MIT](LICENSE.md) license.
