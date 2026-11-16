// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Read-only compaction planner for local validation.
//!
//! ```text
//! cargo run -p lakesoul-io --example compaction_plan -- \
//!   --dir /path/to/parquet-or-vortex/files --pk id
//! ```
//!
//! The tool never writes to the input files.  With `--execute` it writes the
//! planned output (whole files or a copied vortex file) into `--out`.

use std::path::{Path, PathBuf};

use lakesoul_io::compaction::{
    CompactionMode, CompactionOptions, FileAction, copy_files, copy_vortex_ranges,
    plan_compaction, summarize_files,
};

struct Args {
    dir: Option<String>,
    manifest: Option<String>,
    out: Option<String>,
    primary_keys: Vec<String>,
    require_single_file: bool,
    block_copy_min_ratio: f64,
    execute: bool,
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        dir: None,
        manifest: None,
        out: None,
        primary_keys: Vec::new(),
        require_single_file: false,
        block_copy_min_ratio: 0.5,
        execute: false,
    };
    let mut iter = std::env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--dir" => args.dir = iter.next(),
            "--manifest" => args.manifest = iter.next(),
            "--out" => args.out = iter.next(),
            "--pk" => {
                args.primary_keys = iter
                    .next()
                    .ok_or("--pk needs a value")?
                    .split(',')
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
                    .collect();
            }
            "--require-single-file" => args.require_single_file = true,
            "--block-copy-min-ratio" => {
                args.block_copy_min_ratio = iter
                    .next()
                    .ok_or("--block-copy-min-ratio needs a value")?
                    .parse()
                    .map_err(|_| "invalid --block-copy-min-ratio")?;
            }
            "--execute" => args.execute = true,
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    if args.primary_keys.is_empty() {
        return Err("--pk is required".to_string());
    }
    if args.dir.is_none() && args.manifest.is_none() {
        return Err("--dir or --manifest is required".to_string());
    }
    Ok(args)
}

fn collect_files(args: &Args) -> Result<Vec<String>, String> {
    let mut files = if let Some(manifest) = &args.manifest {
        std::fs::read_to_string(manifest)
            .map_err(|error| format!("read manifest {manifest}: {error}"))?
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(str::to_string)
            .collect::<Vec<_>>()
    } else {
        let dir = args.dir.as_ref().expect("dir checked");
        let mut entries = std::fs::read_dir(dir)
            .map_err(|error| format!("read dir {dir}: {error}"))?
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| {
                matches!(
                    path.extension().and_then(|value| value.to_str()),
                    Some("parquet") | Some("vortex")
                )
            })
            .collect::<Vec<PathBuf>>();
        // File order is version order.  A directory does not carry LakeSoul's
        // commit order, so approximate it with modification time for local
        // validation.  Production callers pass an explicit manifest.
        entries.sort_by_key(|path| {
            std::fs::metadata(path)
                .and_then(|metadata| metadata.modified())
                .ok()
        });
        entries
            .into_iter()
            .map(|path| path.to_string_lossy().into_owned())
            .collect()
    };
    files.retain(|file| Path::new(file).exists());
    if files.is_empty() {
        return Err("no input files found".to_string());
    }
    Ok(files)
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let args = parse_args()?;
    let files = collect_files(&args)?;
    let summary = summarize_files(&files, &args.primary_keys)
        .await
        .map_err(|error| format!("summarize files: {error}"))?;
    let options = CompactionOptions {
        primary_keys: args.primary_keys.clone(),
        block_copy_min_ratio: args.block_copy_min_ratio,
        require_single_file: args.require_single_file,
        metadata_only: false,
    };
    let plan = plan_compaction(&summary, &options)
        .await
        .map_err(|error| format!("plan compaction: {error}"))?;

    println!("mode: {:?}", plan.mode);
    println!("reason: {}", plan.reason);
    println!(
        "files={} rows={} winners={} duplicate={:.2}% copy={:.2}% keep={} discard={} rewrite={}",
        plan.stats.files,
        plan.stats.total_rows,
        plan.stats.winner_rows,
        plan.stats.duplicate_ratio * 100.0,
        plan.stats.copy_ratio * 100.0,
        plan.stats.keep_files,
        plan.stats.discard_files,
        plan.stats.rewrite_files,
    );
    println!(
        "ranges: nested={:.2}% disjoint={:.2}% overlap={:.2}% hash_like={} newest_contains_all={}",
        plan.stats.nested_ratio * 100.0,
        plan.stats.disjoint_ratio * 100.0,
        plan.stats.overlap_ratio * 100.0,
        plan.stats.hash_like,
        plan.stats.newest_contains_all,
    );
    for file in &plan.files {
        if file.action != FileAction::Keep || plan.stats.files <= 12 {
            println!(
                "  {:?} winners={} {}",
                file.action, file.winner_rows, file.path
            );
        }
    }

    if args.execute {
        let out = args.out.as_deref().ok_or("--execute requires --out")?;
        match plan.mode {
            CompactionMode::BlockLevel => {
                let block_plan = plan
                    .block_plan
                    .as_ref()
                    .ok_or("block-level plan is missing block_plan")?;
                let all_files = plan
                    .files
                    .iter()
                    .map(|file| file.path.clone())
                    .collect::<Vec<_>>();
                let output = Path::new(out).join("compacted.vortex");
                std::fs::create_dir_all(out)
                    .map_err(|error| format!("create {out}: {error}"))?;
                let size = copy_vortex_ranges(
                    &all_files,
                    &block_plan.ranges,
                    &output.to_string_lossy(),
                )
                .await
                .map_err(|error| format!("copy vortex: {error}"))?;
                println!("wrote {} ({} bytes)", output.display(), size);
            }
            CompactionMode::FileLevel | CompactionMode::KeepOnly => {
                let selected = plan
                    .files
                    .iter()
                    .filter(|file| file.action != FileAction::Discard)
                    .map(|file| file.path.clone())
                    .collect::<Vec<_>>();
                let outputs = copy_files(&selected, out)
                    .map_err(|error| format!("copy files: {error}"))?;
                println!("kept {} files in {out}", outputs.len());
            }
            CompactionMode::Fallback => {
                println!("fallback selected; no output written by this planner");
            }
        }
    }
    Ok(())
}
