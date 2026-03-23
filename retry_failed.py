#!/usr/bin/env python3
"""
retry_failed.py
================
Reads the scraper log, removes all failed rows (no_image + error) from the
checkpoint so they get re-processed on the next --resume run.
Also writes an _invalid.xlsx report listing every failed row for easy review.

Works automatically for all 5 datasets — no flags needed.

Usage:
  python retry_failed.py <output_folder_or_dataset_filename>

  python retry_failed.py IndianFoodDatasetXLSFinal
  python retry_failed.py IndianFoodDatasetXLSFinal.xlsx
  python retry_failed.py Indonesian_Food_Recipes.csv
  python retry_failed.py data.csv
  python retry_failed.py food_recipes.csv
  python retry_failed.py "INDIAN FOOD RECIPE.csv"
"""

import os
import sys
import csv
import glob
import argparse

import pandas as pd


# Statuses that should be retried — no_image and anything starting with "error"
def is_failed(status):
    s = status.strip().lower()
    return s == 'no_image' or s.startswith('error')


def resolve_folder(arg):
    """Accept a folder name or a dataset filename — strip extension if needed."""
    if os.path.isdir(arg):
        return arg
    folder = os.path.splitext(arg)[0]
    if os.path.isdir(folder):
        return folder
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Remove failed rows from checkpoint and export them to _invalid.xlsx.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python retry_failed.py IndianFoodDatasetXLSFinal
  python retry_failed.py Indonesian_Food_Recipes.csv
  python retry_failed.py data.csv
        """
    )
    parser.add_argument("target", help="Output folder name or dataset filename")
    args = parser.parse_args()

    # ── Resolve folder ─────────────────────────────────────────────────────────
    folder = resolve_folder(args.target)
    if not folder:
        print(f"[X] Could not find output folder for '{args.target}'")
        print(f"    Make sure the scraper has run first to create the output folder.")
        sys.exit(1)

    print(f"[*] Output folder : {folder}/")

    # ── Find log CSV ───────────────────────────────────────────────────────────
    logs = glob.glob(os.path.join(folder, "*_log.csv"))
    if not logs:
        print(f"[X] No log CSV found in '{folder}/'")
        sys.exit(1)
    log_path = logs[0]
    print(f"[*] Log file      : {log_path}")

    # ── Find checkpoint ────────────────────────────────────────────────────────
    checkpoint_path = os.path.join(folder, "_checkpoint.txt")
    if not os.path.exists(checkpoint_path):
        print(f"[X] No checkpoint file at '{checkpoint_path}'")
        sys.exit(1)

    # ── Load checkpoint into a set — O(1) lookups ──────────────────────────────
    with open(checkpoint_path, 'r') as f:
        checkpoint_ids = set(line.strip() for line in f if line.strip())
    print(f"[*] Checkpoint    : {len(checkpoint_ids)} completed IDs")

    # ── Read log into a dict keyed by ID — O(1) access per row ────────────────
    # Building a dict avoids linear search when cross-referencing IDs later.
    # Key: row ID (str), Value: row dict
    all_rows   = {}   # id -> row dict (preserves all rows for log rewrite)
    failed_ids = set()
    status_counts = {}
    fieldnames = None

    with open(log_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames)
        for row in reader:
            row_id = str(row.get('id', '')).strip()
            status = row.get('status', '').strip()
            status_counts[status] = status_counts.get(status, 0) + 1
            all_rows[row_id] = row          # O(1) insert into dict
            if row_id and is_failed(status):
                failed_ids.add(row_id)      # O(1) set insert

    # ── Status summary ─────────────────────────────────────────────────────────
    print(f"\n[*] Log status breakdown:")
    for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
        tag = "  <-- will retry" if is_failed(status) else ""
        print(f"    {status:35s}: {count:6d}{tag}")

    if not failed_ids:
        print(f"\n[*] No failed rows found. Nothing to retry.")
        sys.exit(0)

    print(f"\n[*] Failed rows   : {len(failed_ids)}")

    # ── Separate failed and successful rows using O(1) dict lookup ─────────────
    # failed_rows: rows that will be retried (removed from checkpoint + log)
    # keep_rows  : rows that stay in the log (ok + skip)
    failed_rows = []
    keep_rows   = []

    for row_id, row in all_rows.items():
        if row_id in failed_ids:          # O(1) set lookup
            failed_rows.append(row)
        else:
            keep_rows.append(row)

    # ── Remove failed IDs from checkpoint — O(1) set difference ───────────────
    new_checkpoint_ids = checkpoint_ids - failed_ids   # O(n), not O(n^2)

    with open(checkpoint_path, 'w') as f:
        for rid in sorted(new_checkpoint_ids, key=lambda x: int(x) if x.isdigit() else x):
            f.write(rid + '\n')
    print(f"[✓] Checkpoint    : {len(new_checkpoint_ids)} IDs remain ({len(failed_ids)} removed)")

    # ── Rewrite log with only successful rows ─────────────────────────────────
    with open(log_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(keep_rows)
    print(f"[✓] Log cleaned   : {len(keep_rows)} rows remain ({len(failed_rows)} removed)")

    # ── Write _invalid.xlsx with failed rows ───────────────────────────────────
    # Sheet 1: All failed rows with full details
    # Sheet 2: Just the page URLs — easy to open and check manually
    invalid_path = os.path.join(folder, "_invalid.xlsx")

    failed_df = pd.DataFrame(failed_rows, columns=fieldnames)

    # Clean URL sheet — just name + page_url + status for quick review
    url_cols = [c for c in ['id', 'name', 'page_url', 'status', 'timestamp'] if c in fieldnames]
    url_df   = failed_df[url_cols].reset_index(drop=True)

    with pd.ExcelWriter(invalid_path, engine='openpyxl') as writer:
        failed_df.to_excel(writer, sheet_name="Failed Rows",  index=False)
        url_df.to_excel(   writer, sheet_name="URLs to Check", index=False)

    print(f"[✓] Invalid report: {invalid_path}")
    print(f"    Sheet 1 — Failed Rows   : {len(failed_df)} rows (full details)")
    print(f"    Sheet 2 — URLs to Check : {len(url_df)} rows (page URLs for manual review)")

    # ── Print the failed URLs for quick visibility ─────────────────────────────
    print(f"\n[*] Failed URLs:")
    for row in failed_rows[:30]:
        print(f"    [{row.get('status','')}]  {row.get('name','')[:40]:40s}  {row.get('page_url','')[:70]}")
    if len(failed_rows) > 30:
        print(f"    ... and {len(failed_rows) - 30} more (see _invalid.xlsx)")

    # ── Infer dataset filename for the next-step hint ──────────────────────────
    dataset_name = os.path.basename(folder)
    candidates   = [f"{dataset_name}.csv", f"{dataset_name}.xlsx", f"{dataset_name}.xls"]
    dataset_file = next((c for c in candidates if os.path.exists(c)), f"{dataset_name}.csv")

    print(f"""
[*] Ready. Now run:

    python bulk_scraper_pro.py "{dataset_file}" --workers 5 --resume

    {len(failed_ids)} failed rows will be re-processed.
    {len(new_checkpoint_ids)} completed rows will be skipped instantly.
""")


if __name__ == "__main__":
    main()
