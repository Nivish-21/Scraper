#!/usr/bin/env python3
"""
Single-entry runner for the image scraping pipeline.

This script:
  1. runs the bulk scraper
  2. checks whether failures are recoverable
  3. runs retry_failed.py logic automatically
  4. resumes the scraper for a configurable number of retry rounds
"""

import argparse
import os
import sys

from scraper import main as run_bulk_scraper
from retry_failed import repair_failed_rows


def derive_output_dir(dataset_path, explicit_output_dir=None):
    if explicit_output_dir:
        return explicit_output_dir
    stem = os.path.splitext(os.path.basename(dataset_path))[0]
    return f"{stem}_images_output"


def print_final_summary(dataset_path, output_dir, result, retry_rounds_used):
    dataset_name = os.path.basename(dataset_path)
    output_name = os.path.basename(output_dir.rstrip(os.sep))
    print("\n" + "=" * 64)
    print("Pipeline Summary")
    print(f"Dataset file       : {os.path.abspath(dataset_path)}")
    print(f"Derived folder name: {output_name}/")
    print(f"Final output path  : {os.path.abspath(output_dir)}/")
    print(f"Successful rows    : {result['ok']}")
    print(f"Failed rows        : {result['failed']}")
    print(f"Skipped rows       : {result['skipped']}")
    print(f"Retry rounds used  : {retry_rounds_used}")
    print(f"Log CSV            : {result['log_path']}")
    print(f"Checkpoint         : {result['checkpoint_path']}")
    print("=" * 64)


def build_bulk_args(args, resume=False):
    bulk_args = [args.file, "--workers", str(args.workers), "--delay", str(args.delay)]
    if args.rows is not None:
        bulk_args += ["--rows", str(args.rows)]
    bulk_args += ["--output-dir", derive_output_dir(args.file, args.output_dir)]
    if args.url_col:
        bulk_args += ["--url-col", args.url_col]
    if args.name_col:
        bulk_args += ["--name-col", args.name_col]
    if args.id_col:
        bulk_args += ["--id-col", args.id_col]
    if args.image_col:
        bulk_args += ["--image-col", args.image_col]
    if resume:
        bulk_args.append("--resume")
    return bulk_args


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Run the full image scraping pipeline with automatic retry rounds."
    )
    parser.add_argument("file", help="Path to the CSV or Excel dataset")
    parser.add_argument("--rows", type=int, default=None, help="Limit rows to process")
    parser.add_argument("--workers", type=int, default=4, help="Parallel workers (default: 4)")
    parser.add_argument("--delay", type=float, default=1.0, help="Base delay between requests")
    parser.add_argument("--output-dir", default=None, help="Output folder override")
    parser.add_argument("--url-col", default=None, help="Page URL column override")
    parser.add_argument("--name-col", default=None, help="Name/title column override")
    parser.add_argument("--id-col", default=None, help="ID column override")
    parser.add_argument("--image-col", default=None, help="Direct image URL column override")
    parser.add_argument("--max-retry-rounds", type=int, default=2, help="Automatic retry rounds after the first pass")
    args = parser.parse_args(argv)
    resolved_output_dir = derive_output_dir(args.file, args.output_dir)
    retry_rounds_used = 0

    print(f"[*] Dataset: {os.path.abspath(args.file)}")
    print(f"[*] Output : {os.path.abspath(resolved_output_dir)}")
    first_pass = run_bulk_scraper(build_bulk_args(args, resume=False))
    if first_pass["exit_code"] != 0:
        print("[X] Pipeline stopped during the initial scrape.")
        print(f"[X] Output folder was: {os.path.abspath(resolved_output_dir)}")
        return first_pass["exit_code"]

    output_target = resolved_output_dir
    last_result = first_pass

    for round_no in range(1, args.max_retry_rounds + 1):
        if last_result["failed"] <= 0:
            print("[*] No failed rows remain. Pipeline complete.")
            print_final_summary(args.file, resolved_output_dir, last_result, retry_rounds_used)
            return 0

        print(f"\n[*] Automatic recovery round {round_no}/{args.max_retry_rounds}")
        repair = repair_failed_rows(output_target)
        if repair["exit_code"] != 0:
            print("[X] Could not prepare failed rows for retry.")
            print(f"[X] Check the output folder manually: {os.path.abspath(resolved_output_dir)}")
            return repair["exit_code"]
        if repair["failed_count"] == 0:
            print("[*] No retryable failed rows remain. Pipeline complete.")
            print_final_summary(args.file, resolved_output_dir, last_result, retry_rounds_used)
            return 0

        resumed = run_bulk_scraper(build_bulk_args(args, resume=True))
        retry_rounds_used += 1
        if resumed["exit_code"] != 0:
            print("[X] Pipeline stopped during an automatic retry pass.")
            print(f"[X] Check the output folder manually: {os.path.abspath(resolved_output_dir)}")
            return resumed["exit_code"]
        if resumed["failed"] >= last_result["failed"]:
            print("[X] Automatic recovery stalled: failed row count did not improve on the last retry pass.")
            print("[X] This usually means the remaining rows are blocked, malformed, or the site is exposing the wrong asset.")
            print_final_summary(args.file, resolved_output_dir, resumed, retry_rounds_used)
            return 1
        last_result = resumed

    if last_result["failed"] > 0:
        print(
            f"[X] Pipeline finished with {last_result['failed']} unresolved row(s) after "
            f"{args.max_retry_rounds} automatic retry round(s)."
        )
        print("[X] Check the log CSV and _invalid.xlsx in the output folder for impossible or blocked cases.")
        print_final_summary(args.file, resolved_output_dir, last_result, retry_rounds_used)
        return 1

    print("[*] Pipeline complete.")
    print_final_summary(args.file, resolved_output_dir, last_result, retry_rounds_used)
    return 0


if __name__ == "__main__":
    sys.exit(main())
