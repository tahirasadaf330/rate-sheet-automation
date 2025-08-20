
""" 
Step 1: run the email verification script to fetch all the new valid files which will be stored in the attachments folder
Step 2: then run the jerasoft script to fetch all the relevant tables for comparision
Step 3: run the preprocess script on all the files 
Step 4: run the ratesheet comparision script to generate the comparision report
Step 5: run the database script to push the comparision results to the database 

"""

"""
while saving the files from email also create a meta data file for that directory that should include subject, sender, date, time
path to the directory.

then read that meta data file and create all the comparision files using jerasoft and save in the same directory.

then preprocess all the files 

"""

from jerasoft import export_rates_by_query
from ratesheet_comparision_engine import read_table, compare, write_excel
from email_verification import verify_fetch_emails
import os
import json
from pathlib import Path
from preprocess_data import load_clean_rates
from typing import Iterable, Tuple, Optional
from datetime import datetime, timezone

#_____________ Email Verification Script_____________

after = "2025-08-19"              # only include emails on/after this date (YYYY-MM-DD) or None
before = "2025-08-19"             # only include emails on/before this date (YYYY-MM-DD) or None
unread_only = False    
verify_fetch_emails(after, before, unread_only)

#_______________ Jerasoft Script _____________

def process_all_directories(attachments_base="attachments"):
    """
    Walk through all directories inside `attachments/`,
    check metadata.json, update preprocessed flag,
    and call export_rates_by_query with correct parameters.
    """
    for subdir in Path(attachments_base).iterdir():
        if not subdir.is_dir():
            continue

        meta_file = subdir / "metadata.json"
        if not meta_file.exists():
            print(f"[SKIP] No metadata.json in {subdir}")
            continue

        # Load metadata
        try:
            with open(meta_file, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except Exception as e:
            print(f"[ERROR] Failed to read {meta_file}: {e}")
            continue

        # Skip if already preprocessed
        if meta.get("preprocessed") is True:
            print(f"[SKIP] Already preprocessed: {subdir}")
            continue

        # Mark preprocessed
        meta["preprocessed"] = True
        with open(meta_file, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)
        print(f"[INFO] Updated metadata with preprocessed flag: {meta_file}")

        # Extract subject
        subject = meta.get("subject")
        if not subject:
            print(f"[WARN] No subject found in {meta_file}, skipping...")
            continue

        # Extract directory and attachments
        dir_path = meta.get("directory")
        attachments = meta.get("attachments", [])

        if not dir_path or not attachments:
            print(f"[WARN] Missing directory/attachments info in {meta_file}, skipping...")
            continue

        # Decide output filename
        if len(attachments) == 1:
            base_name = Path(attachments[0]).stem
            output_file = f"{base_name}_jerasoft_comparison.xlsx"
        else:
            output_file = "jerasoft_comparison_all.xlsx"

        output_path = str(Path(dir_path) / output_file)

        # Call your export function
        try:
            info = export_rates_by_query(
                target_query=subject,
                output_path=output_path,
            )
            print(f"[SUCCESS] Exported for {subject} -> {output_path}")
        except Exception as e:
            print(f"[ERROR] Failed to export for {subject}: {e}")


process_all_directories()

#_______________ Preprocess Script _____________


ALLOWED_EXTS = {".xlsx", ".xls", ".csv"}

def iter_preprocessed_dirs(attachments_root: Path):
    """
    Yield directories under attachments_root whose metadata.json has "preprocessed": true.
    """
    for child in sorted(attachments_root.iterdir()):
        if not child.is_dir():
            continue
        meta = child / "metadata.json"
        if not meta.exists():
            continue
        try:
            with meta.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            # If metadata is unreadable, skip silently (or print a warning if you want)
            continue
        # AFTER
        if bool(data.get("preprocessed")) is True and "preprocessed_results" not in data:
            yield child

def files_to_clean(folder: Path):
    """
    Return all cleanable files in folder (CSV/XLS/XLSX/XLSM), excluding metadata.json.
    """
    for f in sorted(folder.iterdir()):
        if not f.is_file():
            continue
        if f.name.lower() == "metadata.json":
            continue
        if f.suffix.lower() in ALLOWED_EXTS:
            yield f

def clean_preprocessed_folders(attachments_dir: str | Path):
    """
    For each preprocessed folder:
      - run load_clean_rates(file, file, 0) for every allowed file inside.
    Updates metadata.json with success/failure for each file.
    Returns (folders_processed, files_cleaned).
    """
    root = Path(attachments_dir).expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Attachments directory not found: {root}")

    folders_done = 0
    files_done = 0

    print("\n=== Cleaning pass over preprocessed folders ===")
    for folder in iter_preprocessed_dirs(root):
        folders_done += 1
        print(f"\n[FOLDER] {folder}")

        # Load metadata to track file results
        metadata_path = folder / "metadata.json"
        try:
            with metadata_path.open("r", encoding="utf-8") as f:
                metadata = json.load(f)
        except Exception as e:
            print(f"  ✖ Failed to load metadata: {e}")
            continue

        # Initialize preprocessed_results in metadata if not present
        if "preprocessed_results" not in metadata:
            metadata["preprocessed_results"] = {}

        any_files = False
        for file_path in files_to_clean(folder):
            any_files = True
            try:
                in_path = str(file_path)
                out_path = str(file_path)   # same path -> overwrite in place
                print(f"  - Cleaning: {file_path.name}")
                load_clean_rates(in_path, out_path, 0)
                files_done += 1
                print(f"    ✔ cleaned -> {file_path}")
                # Update metadata with successful cleaning result
                metadata["preprocessed_results"][file_path.name] = True
            except Exception as e:
                print(f"    ✖ failed cleaning {file_path.name}: {e}")
                # Update metadata with failure result
                metadata["preprocessed_results"][file_path.name] = False

        # Save the updated metadata back to the file
        try:
            with metadata_path.open("w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"  ✖ Failed to update metadata: {e}")

        if not any_files:
            print("  (no CSV/Excel files found)")

    print("\n=== Cleaning summary ===")
    print(f"Folders processed: {folders_done}")
    print(f"Files cleaned:     {files_done}")
    return folders_done, files_done


clean_preprocessed_folders("attachments")

#________________ Ratesheet Comparision Script _____________

ALLOWED_EXTS = {".xlsx", ".xls", ".csv"}


def iter_preprocessed_dirs(attachments_root: Path) -> Iterable[Path]:
    """
    Yield preprocessed directories that have NOT yet been compared
    (metadata.json has preprocessed=True and NO 'comparision_result' key).
    """
    for child in sorted(attachments_root.iterdir()):
        if not child.is_dir():
            continue
        meta = child / "metadata.json"
        if not meta.exists():
            continue
        try:
            with meta.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue

        if bool(data.get("preprocessed")) is True and "comparision_result" not in data:
            yield child


def find_jerasoft_file(folder: Path) -> Optional[Path]:
    """Prefer jerasoft_comparison_all.xlsx, else first *_jerasoft_comparison.xlsx."""
    prime = folder / "jerasoft_comparison_all.xlsx"
    if prime.exists():
        return prime
    candidates = sorted(
        p for p in folder.iterdir()
        if p.is_file()
        and p.suffix.lower() in (".xlsx", ".xls")
        and p.name.lower().endswith("_jerasoft_comparison.xlsx")
    )
    return candidates[0] if candidates else None


def vendor_files(folder: Path) -> list[Path]:
    """All candidate files except metadata.json and JeraSoft comparison outputs."""
    out: list[Path] = []
    for f in sorted(folder.iterdir()):
        if not f.is_file():
            continue
        if f.name.lower() == "metadata.json":
            continue
        ext = f.suffix.lower()
        if ext not in ALLOWED_EXTS:
            continue
        name = f.name.lower()
        if name == "jerasoft_comparison_all.xlsx" or name.endswith("_jerasoft_comparison.xlsx"):
            continue  # baseline, not a vendor file
        out.append(f)
    return out


def as_of_from_metadata(folder: Path) -> str:
    """Use metadata.date_utc if available, else today (UTC, YYYY-MM-DD)."""
    meta = folder / "metadata.json"
    if meta.exists():
        try:
            with meta.open("r", encoding="utf-8") as f:
                data = json.load(f)
            d = (data.get("date_utc") or "").strip()
            if len(d) == 10 and d[4] == "-" and d[7] == "-":
                return d
        except Exception:
            pass
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _read_metadata(folder: Path) -> dict:
    with (folder / "metadata.json").open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_metadata(folder: Path, data: dict) -> None:
    with (folder / "metadata.json").open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def compare_preprocessed_folders(
    attachments_dir: str | Path,
    *,
    notice_days: int = 7,
    rate_tol: float = 0.0,
    sheet_left=None,
    sheet_right=None,
) -> Tuple[int, int]:
    """
    Folder-level logic:
      1) Require 'preprocessed' True and absence of 'comparision_result'.
      2) Confirm comparison/baseline file exists AND was preprocessed True.
         - If its preprocessed flag is False (or missing), skip folder and write:
           comparision_result: {"result": "...skipped because comparison file failed..."}
      3) Otherwise, for each vendor file:
         - If vendor file's preprocessed flag is False/missing -> comparision_result[filename] = False
         - Else run compare; success -> True, failure -> False
      4) Write comparision_result to metadata once per folder.
    """
    root = Path(attachments_dir).expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Attachments directory not found: {root}")

    folders_done = 0
    writes = 0

    print("\n=== Comparison pass over preprocessed folders ===")
    for folder in iter_preprocessed_dirs(root):
        folders_done += 1
        print(f"\n[FOLDER] {folder}")

        # load metadata once
        try:
            meta = _read_metadata(folder)
        except Exception as e:
            print(f"  ✖ cannot read metadata.json: {e}")
            # can't record anything; just skip this folder
            continue

        preproc_map: dict = meta.get("preprocessed_results", {}) or {}

        # 1) find baseline file
        left_path = find_jerasoft_file(folder)
        if not left_path:
            print("  ✖ No JeraSoft baseline found (jerasoft_comparison_all.xlsx or *_jerasoft_comparison.xlsx). Skipping.")
            # record reason
            meta["comparision_result"] = {"result": "comparison skipped: no baseline file found"}
            _write_metadata(folder, meta)
            continue

        # 2) check baseline preprocessed flag
        baseline_name = left_path.name
        baseline_ok = bool(preproc_map.get(baseline_name))
        if not baseline_ok:
            print("  ✖ Baseline comparison file exists but was not successfully preprocessed. Skipping folder.")
            meta["comparision_result"] = {
                "result": "comparison skipped: comparison file failed preprocessing"
            }
            _write_metadata(folder, meta)
            continue

        # 3) gather vendor files
        vfiles = vendor_files(folder)
        if not vfiles:
            print("  (no vendor files to compare)")
            # still write an empty result so this folder won't be processed again
            meta["comparision_result"] = {"result": "no vendor files to compare"}
            _write_metadata(folder, meta)
            continue

        as_of_date = as_of_from_metadata(folder)
        print(f"  AS_OF_DATE: {as_of_date} | NOTICE_DAYS: {notice_days} | RATE_TOL: {rate_tol}")

        # 4) read baseline table
        try:
            left_df = read_table(str(left_path), sheet_left)
        except Exception as e:
            print(f"  ✖ Failed reading LEFT (JeraSoft) {left_path.name}: {e}")
            meta["comparision_result"] = {"result": f"comparison skipped: failed to read baseline ({e})"}
            _write_metadata(folder, meta)
            continue

        # 5) compare each vendor; build comparision_result as requested
        comp_result: dict[str, bool] = {}
        for idx, v in enumerate(vfiles):
            vname = v.name
            print(f"  - Compare against vendor: {vname}")

            # gate on vendor preprocessed flag
            if not preproc_map.get(vname):
                print("    ↳ skipped: vendor file not successfully preprocessed")
                comp_result[vname] = False
                continue

            try:
                right_df = read_table(str(v), sheet_right)
                result = compare(left_df, right_df, as_of_date, notice_days, rate_tol)

                # NEW: always name by vendor file + "_comparision_difference.xlsx"
                out_path = folder / f"{v.stem}_comparision_difference.xlsx"


                write_excel(result, str(out_path))
                writes += 1
                comp_result[vname] = True
                print(f"    ✔ wrote {out_path} ({len(result)} rows)")
            except Exception as e:
                comp_result[vname] = False
                print(f"    ✖ failed comparison for {vname}: {e}")

        # 6) persist comparision_result
        # if at least one vendor processed, include a simple outcome line
        if comp_result:
            success_any = any(comp_result.values())
            if success_any:
                meta["comparision_result"] = {"result": "ok", **comp_result}
            else:
                meta["comparision_result"] = {"result": "no comparisons succeeded", **comp_result}
        else:
            meta["comparision_result"] = {"result": "no eligible vendor files"}

        try:
            _write_metadata(folder, meta)
        except Exception as e:
            print(f"  ⚠ failed updating metadata: {e}")

    print("\n=== Comparison summary ===")
    print(f"Folders processed:     {folders_done}")
    print(f"Comparison files made: {writes}")
    return folders_done, writes

compare_preprocessed_folders("attachments", notice_days=7, rate_tol=0.0)

#________________ Database Script _____________
