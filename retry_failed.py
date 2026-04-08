#!/usr/bin/env python3
"""
retry_failed.py
================
Reads the scraper log, removes all failed rows (no_image + error) from the
checkpoint so they get re-processed on the next --resume run.
Also writes an _invalid.xlsx report listing every failed row for easy review.
"""

import os
import sys
import csv
import glob
import argparse

import pandas as pd


def is_failed(status):
    s = status.strip().lower()
    return s == "no_image" or s.startswith("error")


def resolve_folder(arg):
    if os.path.isdir(arg):
        return arg
    folder = os.path.splitext(arg)[0]
    if os.path.isdir(folder):
        return folder
    return None


def repair_failed_rows(target):
    folder = resolve_folder(target)
    if not folder:
        print(f"[X] Could not find output folder for '{target}'")
        print("    Make sure the scraper has run first to create the output folder.")
        return {"exit_code": 1, "folder": None, "failed_count": None, "dataset_file": None}

    print(f"[*] Output folder : {folder}/")

    logs = glob.glob(os.path.join(folder, "*_log.csv"))
    if not logs:
        print(f"[X] No log CSV found in '{folder}/'")
        return {"exit_code": 1, "folder": folder, "failed_count": None, "dataset_file": None}
    log_path = logs[0]
    print(f"[*] Log file      : {log_path}")

    checkpoint_path = os.path.join(folder, "_checkpoint.txt")
    if not os.path.exists(checkpoint_path):
        print(f"[X] No checkpoint file at '{checkpoint_path}'")
        return {"exit_code": 1, "folder": folder, "failed_count": None, "dataset_file": None}

    with open(checkpoint_path, "r") as f:
        checkpoint_ids = set(line.strip() for line in f if line.strip())
    print(f"[*] Checkpoint    : {len(checkpoint_ids)} completed IDs")

    all_rows = {}
    failed_ids = set()
    status_counts = {}
    fieldnames = None

    with open(log_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames)
        for row in reader:
            row_id = str(row.get("id", "")).strip()
            status = row.get("status", "").strip()
            status_counts[status] = status_counts.get(status, 0) + 1
            all_rows[row_id] = row
            if row_id and is_failed(status):
                failed_ids.add(row_id)

    print("\n[*] Log status breakdown:")
    for status, count in sorted(status_counts.items(), key=lambda x: -x[1]):
        tag = "  <-- will retry" if is_failed(status) else ""
        print(f"    {status:35s}: {count:6d}{tag}")

    dataset_name = os.path.basename(folder)
    candidates = [f"{dataset_name}.csv", f"{dataset_name}.xlsx", f"{dataset_name}.xls"]
    dataset_file = next((c for c in candidates if os.path.exists(c)), f"{dataset_name}.csv")

    if not failed_ids:
        print("\n[*] No failed rows found. Nothing to retry.")
        return {"exit_code": 0, "folder": folder, "failed_count": 0, "dataset_file": dataset_file}

    print(f"\n[*] Failed rows   : {len(failed_ids)}")

    failed_rows = []
    keep_rows = []
    for row_id, row in all_rows.items():
        if row_id in failed_ids:
            failed_rows.append(row)
        else:
            keep_rows.append(row)

    new_checkpoint_ids = checkpoint_ids - failed_ids

    with open(checkpoint_path, "w") as f:
        for rid in sorted(new_checkpoint_ids, key=lambda x: int(x) if x.isdigit() else x):
            f.write(rid + "\n")
    print(f"[✓] Checkpoint    : {len(new_checkpoint_ids)} IDs remain ({len(failed_ids)} removed)")

    with open(log_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(keep_rows)
    print(f"[✓] Log cleaned   : {len(keep_rows)} rows remain ({len(failed_rows)} removed)")

    invalid_path = os.path.join(folder, "_invalid.xlsx")
    failed_df = pd.DataFrame(failed_rows, columns=fieldnames)
    url_cols = [c for c in ["id", "name", "page_url", "status", "timestamp"] if c in fieldnames]
    url_df = failed_df[url_cols].reset_index(drop=True)

    with pd.ExcelWriter(invalid_path, engine="openpyxl") as writer:
        failed_df.to_excel(writer, sheet_name="Failed Rows", index=False)
        url_df.to_excel(writer, sheet_name="URLs to Check", index=False)

    print(f"[✓] Invalid report: {invalid_path}")
    print(f"    Sheet 1 — Failed Rows   : {len(failed_df)} rows (full details)")
    print(f"    Sheet 2 — URLs to Check : {len(url_df)} rows (page URLs for manual review)")

    print("\n[*] Failed URLs:")
    for row in failed_rows[:30]:
        print(f"    [{row.get('status','')}]  {row.get('name','')[:40]:40s}  {row.get('page_url','')[:70]}")
    if len(failed_rows) > 30:
        print(f"    ... and {len(failed_rows) - 30} more (see _invalid.xlsx)")

    print(f"""
[*] Ready. Now run:

    python scraper.py "{dataset_file}" --workers 4 --resume

    {len(failed_ids)} failed rows will be re-processed.
    {len(new_checkpoint_ids)} completed rows will be skipped instantly.
""")

    return {
        "exit_code": 0,
        "folder": folder,
        "failed_count": len(failed_ids),
        "dataset_file": dataset_file,
        "invalid_path": invalid_path,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Remove failed rows from checkpoint and export them to _invalid.xlsx.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python retry_failed.py IndianFoodDatasetXLSFinal
  python retry_failed.py Indonesian_Food_Recipes.csv
  python retry_failed.py data.csv
        """,
    )
    parser.add_argument("target", help="Output folder name or dataset filename")
    args = parser.parse_args(argv)
    result = repair_failed_rows(args.target)
    return result["exit_code"]


if __name__ == "__main__":
    sys.exit(main())
