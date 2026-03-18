use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};

use chunksplitter::{ProgressEvent, verify_round_trip};

/// Split and merge Minecraft Bedrock worlds for version control.
#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Split a Bedrock world into a VCS-friendly directory tree.
    Split {
        /// Path to the Bedrock world directory.
        world: PathBuf,
        /// Destination directory for the split output.
        output: PathBuf,
    },
    /// Reconstruct a Bedrock world from a split directory.
    Merge {
        /// Path to the split directory produced by `split`.
        input: PathBuf,
        /// Destination directory for the reconstructed world.
        output: PathBuf,
    },
    /// Verify a world round-trips correctly (split → merge → compare).
    TestWorld {
        /// Path to the Bedrock world directory to test.
        world: PathBuf,
    },
}

// ---------------------------------------------------------------------------
// Progress rendering
// ---------------------------------------------------------------------------

struct Renderer {
    bar: Option<ProgressBar>,
    phase: &'static str,
    start: Instant,
}

impl Renderer {
    fn new() -> Self {
        Self { bar: None, phase: "", start: Instant::now() }
    }

    fn on_event(&mut self, event: ProgressEvent) {
        match event {
            ProgressEvent::Phase(msg) => {
                self.finish_phase();
                self.phase = msg;
                let pb = ProgressBar::new_spinner();
                pb.set_style(spinner_style());
                pb.set_message(msg);
                pb.enable_steady_tick(Duration::from_millis(80));
                self.bar = Some(pb);
            }
            ProgressEvent::Total(n) => {
                if let Some(pb) = &self.bar {
                    pb.set_length(n);
                    pb.set_style(bar_style());
                }
            }
            ProgressEvent::Advance(n) => {
                if let Some(pb) = &self.bar {
                    pb.inc(n);
                }
            }
        }
    }

    fn finish_phase(&mut self) {
        if let Some(pb) = self.bar.take() {
            let pos = pb.position();
            let len = pb.length();
            let count = match len {
                Some(total) if total > 0 => format!(" {pos}/{total}"),
                _ if pos > 0 => format!(" {pos}"),
                _ => String::new(),
            };
            pb.set_style(done_style());
            pb.finish_with_message(format!("{:<28}{}", self.phase, count));
        }
    }

    fn finish(mut self) -> Duration {
        self.finish_phase();
        self.start.elapsed()
    }
}

fn spinner_style() -> ProgressStyle {
    ProgressStyle::with_template("  {spinner:.cyan} {msg}")
        .unwrap()
        .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏", ""])
}

fn bar_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "  {spinner:.cyan} {msg:<28} [{bar:38.cyan/blue}] {pos}/{len}",
    )
    .unwrap()
    .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏", ""])
    .progress_chars("=> ")
}

fn done_style() -> ProgressStyle {
    ProgressStyle::with_template("  ✓  {msg}").unwrap()
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Split { world, output } => {
            eprintln!("Splitting {} → {}", world.display(), output.display());
            let mut r = Renderer::new();
            chunksplitter::split(&world, &output, &mut |e| r.on_event(e))?;
            let elapsed = r.finish();
            eprintln!("\nDone in {:.2}s", elapsed.as_secs_f64());
        }
        Command::Merge { input, output } => {
            eprintln!("Merging {} → {}", input.display(), output.display());
            let mut r = Renderer::new();
            chunksplitter::merge(&input, &output, &mut |e| r.on_event(e))?;
            let elapsed = r.finish();
            eprintln!("\nDone in {:.2}s", elapsed.as_secs_f64());
        }
        Command::TestWorld { world } => {
            eprintln!("Testing round-trip for {}", world.display());
            let mut r = Renderer::new();
            match verify_round_trip(&world, &mut |e| r.on_event(e)) {
                Ok(report) => {
                    let elapsed = r.finish();
                    eprintln!("\nPASS: {} key-value pairs verified in {:.2}s", report.pairs_checked, elapsed.as_secs_f64());
                }
                Err(e) => {
                    r.finish();
                    eprintln!("\nFAIL: {e}");
                    std::process::exit(1);
                }
            }
        }
    }
    Ok(())
}
