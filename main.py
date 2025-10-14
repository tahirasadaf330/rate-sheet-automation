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
from preprocess_data import load_clean_rates, _raw_from_excel_pandas
from typing import Iterable, Tuple, Optional, Dict, Any, List, Mapping
from datetime import datetime, timezone
from database import insert_rate_upload, bulk_insert_rate_upload_details, push_failed_emails_json_to_db, fetch_approved_unprocessed_paths_map, insert_rejected_email_row, insert_or_update_ingest_file, mark_ingest_processed
import pandas as pd
from datetime import datetime
import re


FAILED_EMAILS_PATH = Path(__file__).with_name("failed_emails.json")


#_____________ Email Verification Script_____________

# after = "2025-09-29"              # only include emails on/after this date (YYYY-MM-DD) or None     "2025-08-29"
after = datetime.now().strftime("%Y-%m-%d")
before = None       # only include emails on/before this date (YYYY-MM-DD) or None
unread_only = False    
ATTEMPTS = 2

#________________ Manual Date Verification _____________________

MAX_PREVIEW_ROWS = 200
EXCEL_EXTS = ('.xlsx', '.xlsm', '.xls')

def _parse_iso_utc_dt(s: Optional[str]) -> Optional[datetime]:
    if not s or not isinstance(s, str):
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def _df_preview_records(df: pd.DataFrame, limit: int = MAX_PREVIEW_ROWS) -> list[dict]:
    """Rename columns to col1..colN and return first N rows as list of dicts (strings, blanks for NaN)."""
    if df is None or df.empty:
        return []
    df = df.head(limit).reset_index(drop=True)
    # ensure all values are strings; keep blanks for NaN
    try:
        df = df.astype("string")
    except Exception:
        df = df.astype(str)
    ncols = int(df.shape[1])
    df.columns = [f"col{i+1}" for i in range(ncols)]
    # fillna("") for string dtype keeps <NA> out of JSON
    try:
        df = df.fillna("")
    except Exception:
        pass
    return df.to_dict(orient="records")

def _first_attachment_path(meta: dict) -> Optional[Path]:
    """Return absolute Path to the first attachment (join with directory if needed)."""
    attachments = meta.get("attachments") or []
    if not isinstance(attachments, list) or len(attachments) == 0:
        return None

    first_item = str(attachments[0]).strip()
    if not first_item:
        return None

    p = Path(first_item)
    if p.is_absolute():
        return p

    # directory contains the folder path (Linux server)
    base_dir = str(meta.get("directory") or "").strip()
    if not base_dir:
        return None
    return Path(base_dir) / first_item

def ingest_files_for_manual_date(attachments_root: str | Path = "attachments") -> tuple[int, int, int]:
    """
    Walk attachments/* folders, and for each folder whose metadata.json has
    no 'date_verification_ingestion' (or it's False):
      - pick the first attachment
      - read first 200 rows (csv or excel via _raw_from_excel_pandas)
      - build preview_cache (list[dict] -> col1..colN)
      - insert into ingest_files (email_address, subject, received_at, processed_at, file_path, preview_cache, error_message)
      - set metadata['date_verification_ingestion'] = True

    Returns: (scanned_folders, inserted_rows, skipped_folders)
    """
    root = Path(attachments_root).expanduser().resolve()
    if not root.exists():
        print(f"[INGEST] attachments root not found: {root}")
        return (0, 0, 0)

    scanned = 0
    inserted = 0
    skipped = 0

    for folder in sorted(root.iterdir()):
        if not folder.is_dir():
            continue
        scanned += 1

        meta_path = folder / "metadata.json"
        if not meta_path.exists():
            print(f"[INGEST][SKIP] {folder.name}: no metadata.json")
            skipped += 1
            continue

        try:
            with meta_path.open("r", encoding="utf-8") as f:
                meta = json.load(f) or {}
        except Exception as e:
            print(f"[INGEST][SKIP] {folder.name}: failed reading metadata.json: {e}")
            skipped += 1
            continue

        # Skip if already ingested for manual date verification
        if bool(meta.get("date_verification_ingestion")):
            # already processed this folder for manual date step
            continue

        # Step 2: find first attachment
        fpath = _first_attachment_path(meta)
        if not fpath:
            print(f"[INGEST][SKIP] {folder.name}: metadata.attachments missing/empty")
            skipped += 1
            continue

        # Build the preview_cache
        preview_cache: list[dict] = []
        error_message: Optional[str] = None

        try:
            ext = fpath.suffix.lower()
            if ext == ".csv":
                # treat as raw grid; include header row as data by using header=None
                df = pd.read_csv(str(fpath), header=None, nrows=MAX_PREVIEW_ROWS, dtype=str, on_bad_lines="skip")
                preview_cache = _df_preview_records(df, MAX_PREVIEW_ROWS)
            elif ext in EXCEL_EXTS:
                df = _raw_from_excel_pandas(str(fpath), sheet=0)  # your robust multi-engine reader
                preview_cache = _df_preview_records(df, MAX_PREVIEW_ROWS)
            else:
                # optional: best-effort text read as CSV
                try:
                    df = pd.read_csv(str(fpath), header=None, nrows=MAX_PREVIEW_ROWS, dtype=str, engine="python")
                    preview_cache = _df_preview_records(df, MAX_PREVIEW_ROWS)
                except Exception as e2:
                    raise RuntimeError(f"Unsupported file type {ext} and CSV fallback failed: {e2}") from e2
        except Exception as e:
            error_message = f"preview build failed: {e}"
            preview_cache = []  # still ingest a row with the error

        # Collect DB fields
        email_address = (meta.get("sender") or "").strip() or None
        subject = (meta.get("subject") or "").strip() or None
        received_at = _parse_iso_utc_dt(meta.get("receivedDateTime_raw"))
        processed_at = _parse_iso_utc_dt(meta.get("processed_at_utc"))
        file_path = str(fpath)

        try:
            _id = insert_or_update_ingest_file(
                email_address=email_address,
                subject=subject,
                received_at=received_at,
                processed_at=processed_at,
                file_path=file_path,
                preview_cache=preview_cache,
                error_message=error_message,
            )
            inserted += 1
            print(f"[INGEST][OK] id={_id} -> {folder.name} :: {fpath.name}")

            # Mark folder as done for this step
            meta["date_verification_ingestion"] = True
            with meta_path.open("w", encoding="utf-8") as f:
                json.dump(meta, f, ensure_ascii=False, indent=2)
        except Exception as db_e:
            print(f"[INGEST][ERR] DB insert failed for {folder.name}: {db_e}")
            # do NOT set the flag so we can retry later

    print(f"[INGEST] summary: scanned={scanned}, inserted={inserted}, skipped={skipped}")
    return (scanned, inserted, skipped)

def mark_date_verification_ingestion(path_to_format: Mapping[str, Optional[str]]) -> Tuple[int, int, int]:
    """
    For each file path like:
      /.../attachments/<dir>/<filename.xlsx>
    find its parent directory <dir>, open <dir>/metadata.json, and set:
      date_verification_ingestion_status = True
      date_format_identified = <value from path_to_format[file_path]>

    - Input: dict mapping file_path -> date_format
    - Does NOT modify the input.
    - Deduplicates parent directories (first occurrence wins).
    - Returns (dirs_seen, updated, missing_meta).
    """
    # Collect unique parent directories in order, tracking chosen date_format per dir
    parents = []
    seen = set()
    dir_fmt = {}
    for p, fmt in path_to_format.items():
        try:
            d = Path(p).parent.resolve()
        except Exception:
            continue
        if d not in seen:
            seen.add(d)
            parents.append(d)
            dir_fmt[d] = fmt  # remember the first date_format seen for this dir

    dirs_seen = len(parents)
    updated = 0
    missing_meta = 0

    for d in parents:
        meta_path = d / "metadata.json"
        if not meta_path.exists():
            print(f"[INGEST][SKIP] No metadata.json in {d}")
            missing_meta += 1
            continue

        try:
            with meta_path.open("r", encoding="utf-8") as f:
                meta = json.load(f)
        except Exception as e:
            print(f"[INGEST][WARN] Failed to read {meta_path}: {e}")
            missing_meta += 1
            continue

        # Keep original behavior: if already true, do not modify file
        if meta.get("date_verification_ingestion_status") is True:
            print(f"[INGEST] Already marked true: {d}")
            continue

        meta["date_verification_ingestion_status"] = True
        meta["date_format_identified"] = dir_fmt.get(d)

        # Atomic-ish write
        tmp = meta_path.with_suffix(meta_path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        os.replace(tmp, meta_path)

        updated += 1
        print(f"[INGEST] Marked date_verification_ingestion_status=true (date_format_identified={dir_fmt.get(d)!r}) → {meta_path}")

    print(f"[INGEST] summary: dirs_seen={dirs_seen}, updated={updated}, missing_meta={missing_meta}")
    return dirs_seen, updated, missing_meta

#_______________ Jerasoft Script _____________

def process_all_directories(attachments_base="attachments"):
    """
    Walk through all directories inside `attachments/`,
    check metadata.json, update jerasoft_preprocessed flag,
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

        # Skip if already jerasoft_preprocessed
        if meta.get("jerasoft_preprocessed") is True:
            print(f"[SKIP] Already jerasoft_preprocessed: {subdir}")
            continue

        # NEW: require manual date verification approval
        if not bool(meta.get("date_verification_ingestion_status")):
            print(f"[SKIP] Awaiting date verification approval: {subdir}")
            continue

        # Extract subject
        company = meta.get("company")
        subject = meta.get("subject")
        prefix  = meta.get("prefix")  # may be int or str

        missing_company = not str(company or "").strip()
        has_subject     = bool(str(subject or "").strip())
        has_prefix      = prefix is not None and str(prefix).strip() != ""

        if missing_company and has_subject and has_prefix:
            print(f"[WARN] No company found in {meta_file}, skipping...")
            continue

        print('DEBUG: Running JeraSoft export for:', company, '| subject:', subject, '| prefix:', prefix)

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

        info = None
        try:
            print("DEBUG: Calling export_rates_by_query...")
            info = export_rates_by_query(company, output_path, subject, prefix_code=prefix)
            print(info)

            if isinstance(info, str):
                # Export failed with a message returned by export_rates_by_query
                print(f"[ERROR] Export failed for {company}: {info}")
                meta["keyword_error"] = info
                with open(meta_file, "w", encoding="utf-8") as f:
                    json.dump(meta, f, indent=2)
                print(f"[INFO] Updated metadata with keyword_error: {meta_file}")

            else:
                # Export succeeded; update metadata and also check row count of the saved file
                print(f"[INFO] Export succeeded for {company}, updating metadata.")

                if info:
                    meta["best_table_name"] = info.get("best_table_name")

                # --- Count rows in the saved JeraSoft file and set human-eval flags ---
                rows_js = 0
                try:
                    out_ext = Path(output_path).suffix.lower()
                    if out_ext in (".xlsx", ".xls"):
                        df_js = pd.read_excel(output_path)
                        rows_js = int(df_js.shape[0])
                    elif out_ext == ".csv":
                        df_js = pd.read_csv(output_path)
                        rows_js = int(df_js.shape[0])
                    else:
                        print(f"[WARN] Unrecognized JeraSoft output extension: {out_ext}")
                except Exception as e2:
                    print(f"[WARN] Could not read exported JeraSoft file for row count: {e2}")

                # Record details and sticky flag
                meta["human_eval_details_jerasoft"] = {
                    "file": Path(output_path).name,
                    "rows": rows_js,
                }
                meta["need_human_eval_jerasoft"] = bool(meta.get("need_human_eval_jerasoft")) or (rows_js < 100)
                # ---------------------------------------------------------------------

                # Only mark true if the export call didn’t blow up
                meta["jerasoft_preprocessed"] = True
                with open(meta_file, "w", encoding="utf-8") as f:
                    json.dump(meta, f, indent=2)
                print(f"[INFO] Updated metadata with jerasoft_preprocessed flag/best_table_name: {meta_file}")

                print(f"[SUCCESS] Exported for {company} -> {output_path}")
        except Exception as e:
            print(f"[ERROR] Export failed for {company}: {e}")

        if info is None:
            print(f"[SKIP] {company} not exported; moving on.")

#_______________ Preprocess Script _____________

ALLOWED_EXTS = {".xlsx", ".xls", ".csv"}

def iter_preprocessed_dirs_(attachments_root: Path):
    """
    Yield directories under attachments_root whose metadata.json has "jerasoft_preprocessed": true.
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
            print('  ✖ Failed to load metadata')
            continue

        # NEW: require manual date verification approval
        if not bool(data.get("date_verification_ingestion_status")):
            print(f"[SKIP] Awaiting date verification approval: {child}")
            continue

        if bool(data.get("jerasoft_preprocessed")) is True:
            results = data.get("preprocessed_results", {})
            if not results or any(v is False for v in results.values()):
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
        if f.name.startswith("~$") or f.name.startswith("."):
             continue
        if f.suffix.lower() in ALLOWED_EXTS:
            yield f

def clean_preprocessed_folders(attachments_dir: str | Path):
    """
    For each jerasoft_preprocessed folder:
      - run load_clean_rates(file, file, 0) for every allowed file inside.
    Updates metadata.json with success/failure for each file.
    Returns (folders_processed, files_cleaned).
    """
    root = Path(attachments_dir).expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Attachments directory not found: {root}")

    folders_done = 0
    files_done = 0

    print("\n=== Cleaning pass over jerasoft_preprocessed folders ===")
    for folder in iter_preprocessed_dirs_(root):
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
                print(f"  - Cleaning: {file_path}")
                
                date_fmt = (metadata.get("date_format_identified") or "").strip() or None
                fname = file_path.name.lower()

                # If this is a JeraSoft output, force ISO dates
                if "jerasoft_comparison_all" in fname or "jerasoft_comparison" in fname:
                    date_fmt_to_use = "YYYY-MM-DD"
                else:
                    date_fmt_to_use = date_fmt

                print(f"DEBUG: Date format for this file: {date_fmt_to_use!r}")

                cleaned_df = load_clean_rates(in_path, out_path, 0, date_format_email=date_fmt_to_use)

                files_done += 1
                print(f"    ✔ cleaned -> {file_path}")
                # Update metadata with successful cleaning result
                metadata["preprocessed_results"][file_path.name] = True

                # NEW: flag tiny outputs for human review
                try:
                    row_count = int(cleaned_df.shape[0])
                except Exception:
                    row_count = 0  # be safe if something odd is returned

                # keep a per-file detail (handy for debugging)
                metadata.setdefault("human_eval_details_pre", {})[file_path.name] = {"rows": row_count}

                # set a top-level flag; never turn a prior True back to False
                metadata["need_human_eval_pre"] = bool(metadata.get("need_human_eval_pre")) or (row_count < 100)

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

#________________ Ratesheet Comparision Script _____________

ALLOWED_EXTS = {".xlsx", ".xls", ".csv"}

def iter_preprocessed_dirs(attachments_root: Path) -> Iterable[Path]:
    """
    Yield folders that finished JeraSoft and still need comparison:
    - no comparision_result yet, or
    - comparision_result exists but at least one vendor flag is not True.
    """
    for child in sorted(attachments_root.iterdir()):
        if not child.is_dir():
            continue
        meta_path = child / "metadata.json"
        if not meta_path.exists():
            continue
        try:
            with meta_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue

        # NEW: require manual date verification approval
        if not bool(data.get("date_verification_ingestion_status")):
            print(f"[SKIP] Awaiting date verification approval: {child}")
            continue

        if data.get("jerasoft_preprocessed") is True:
            comp = data.get("comparision_result") or {}
            per_vendor = {k: v for k, v in comp.items() if k != "result"}
            # run if no result yet, or any vendor isn’t exactly True
            if not comp or any(v is not True for v in per_vendor.values()):
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
      1) Require 'jerasoft_preprocessed' True and absence of 'comparision_result'.
      2) Confirm comparison/baseline file exists AND was jerasoft_preprocessed True.
         - If its jerasoft_preprocessed flag is False (or missing), skip folder and write:
           comparision_result: {"result": "...skipped because comparison file failed..."}
      3) Otherwise, for each vendor file:
         - If vendor file's jerasoft_preprocessed flag is False/missing -> comparision_result[filename] = False
         - Else run compare; success -> True, failure -> False
      4) Write comparision_result to metadata once per folder.
    """
    root = Path(attachments_dir).expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Attachments directory not found: {root}")

    folders_done = 0
    writes = 0

    print("\n=== Comparison pass over jerasoft_preprocessed folders ===")
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

        # 2) check baseline jerasoft_preprocessed flag
        baseline_name = left_path.name
        baseline_ok = bool(preproc_map.get(baseline_name))
        if not baseline_ok:
            print("  ✖ Baseline comparison file exists but was not successfully jerasoft_preprocessed. Skipping folder.")
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

            # gate on vendor jerasoft_preprocessed flag
            if not preproc_map.get(vname):
                print("    ↳ skipped: vendor file not successfully jerasoft_preprocessed")
                comp_result[vname] = False
                continue

            try:
            
                right_df = read_table(str(v), sheet_right)
                result, stats = compare(left_df, right_df, as_of_date, notice_days, rate_tol)

                out_path = folder / f"{v.stem}_comparision_result.xlsx"
                write_excel(result, str(out_path))
                writes += 1
                comp_result[vname] = True
                print(f"    ✔ wrote {out_path} ({len(result)} rows)")

                # ⬇️ persist per-attachment stats into metadata
                try:
                    meta.setdefault("attachment_stats", {})
                    stats_key = f"{v.name}"  # e.g. VendorA.xlsx_stats
                    meta["attachment_stats"][stats_key] = {
                        **stats,
                        "source_attachment": v.name,                    # the vendor file we compared
                        "result_file": out_path.name,                   # the Excel we just wrote
                        "generated_at_utc": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                    }
                    _write_metadata(folder, meta)
                except Exception as e:
                    print(f"    ⚠ failed to update attachment_stats in metadata: {e}")

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

            meta["processed_at_utc"] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            meta["comparision_result"] = {"result": "no eligible vendor files"}
            meta["processed_at_utc"] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

        try:
            _write_metadata(folder, meta)
        except Exception as e:
            print(f"  ⚠ failed updating metadata: {e}")

    print("\n=== Comparison summary ===")
    print(f"Folders processed:     {folders_done}")
    print(f"Comparison files made: {writes}")
    return folders_done, writes

##############################################

def push_rejections_from_metadata(attachments_root: str | Path) -> tuple[int, int]:
    """
    Scan each attachments/<folder>/metadata.json and, if not already_pushed,
    insert one row into rejected_emails based on the first applicable condition:
      1) jerasoft_preprocessed == False  -> 'jerasoft_error'
      2) need_human_eval_jerasoft == True -> 'need_human_eval_jerasoft'
      3) any preprocessed_results entry == False -> 'preprocessing_error'
      4) need_human_eval_pre == True -> 'need_human_eval_preprocessing'

    After a successful insert, mark metadata['already_pushed'] = True.

    Returns: (folders_scanned, rows_inserted)
    """
    print("\n=== Pushing rejected emails from metadata.json files ===")
    root = Path(attachments_root).expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Attachments directory not found: {root}")

    scanned = 0
    inserted = 0

    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue
        meta_path = child / "metadata.json"
        if not meta_path.exists():
            continue

        scanned += 1

        try:
            with meta_path.open("r", encoding="utf-8") as f:
                meta = json.load(f)
        except Exception as e:
            print(f"(warn) cannot read {meta_path}: {e}")
            continue

        # skip if already pushed
        if bool(meta.get("already_pushed")) is True:
            continue

        sender = (meta.get("sender") or "").strip() or None
        subject = (meta.get("subject") or "").strip() or None
        received_at = _parse_iso_utc_safe(meta.get("receivedDateTime_raw"))
        processed_at = _parse_iso_utc_safe(meta.get("processed_at_utc"))

        # Decide category & notes (first match wins)
        category = None
        notes = None

        # 1) JeraSoft export failed/not done
        jp = meta.get("jerasoft_preprocessed")
        if jp is False or jp is None:
            category = "jerasoft_error"
            ke = meta.get("keyword_error")
            best = meta.get("best_table_name")
            bits = ["JeraSoft export did not complete (jerasoft_preprocessed is false/missing)."]
            if ke:
                bits.append(f"keyword_error: {ke}")
            if best:
                bits.append(f"best_table_name (last known): {best}")
            notes = " ".join(bits)

        # 2) JeraSoft needs human review: row count < 100
        if not category and bool(meta.get("need_human_eval_jerasoft")):
            category = "need_human_eval_jerasoft"
            det = meta.get("human_eval_details_jerasoft") or {}
            file_name = det.get("file")
            rows = det.get("rows")
            notes = f"JeraSoft export appears too small (<100 rows). File={file_name or 'unknown'}, rows={rows if rows is not None else 'unknown'}."

        # 3) Preprocessing had failures
        if not category:
            pr = meta.get("preprocessed_results") or {}
            failed = [name for name, ok in pr.items() if ok is False]
            if failed:
                category = "preprocessing_error"
                notes = "Preprocessing failed for: " + ", ".join(failed)
        
        # 3.5) Comparison step failed or skipped (accepts either 'comparision_result' or 'comparison_result')
        if not category:
            comp = meta.get("comparision_result") or meta.get("comparison_result")
            if comp is not None:
                if isinstance(comp, dict):
                    result_val = (comp.get("result") or "").strip()
                    extra = comp.get("message") or comp.get("detail") or None
                else:
                    result_val = str(comp).strip()
                    extra = None

                # treat anything other than "ok" (case-insensitive) as a failure
                if result_val.lower() != "ok":
                    category = "comparison_failed"
                    note_parts = [f"Comparison result: {result_val or 'unknown'}."]
                    if extra:
                        note_parts.append(str(extra))
                    notes = " ".join(note_parts)


        # 4) Vendor files need human review: small outputs
        if not category and bool(meta.get("need_human_eval_pre")):
            category = "need_human_eval_preprocessing"
            det = meta.get("human_eval_details_pre") or {}
            # det looks like { "fileA.xlsx": {"rows": n}, ... }
            parts = []
            for k, v in det.items():
                try:
                    parts.append(f"{k}={int((v or {}).get('rows', 0))} rows")
                except Exception:
                    parts.append(f"{k}=unknown rows")
            joined = ", ".join(parts) if parts else "no detail"
            notes = f"Preprocessing produced small outputs (<100 rows): {joined}."
        
        # 5) Handle 'keyword_error' flag for company name issues
        if not category and meta.get("keyword_error"):
            category = "company_name_error"
            keyword_error_msg = meta.get("keyword_error")
            notes = f"Company name error: {keyword_error_msg}"

        # If nothing to report, skip
        if not category:
            continue

        # Insert into DB
        try:
            new_id = insert_rejected_email_row(
                sender_email=sender,
                subject=subject,
                category=category,
                notes=notes,
                received_at=received_at,
                processed_at=processed_at,
            )
            print(f"(ok) rejected_emails id={new_id} ← {child.name} [{category}]")
            inserted += 1
        except Exception as e:
            print(f"(warn) failed inserting rejected_emails for {child.name}: {e}")
            continue

        # Mark as already pushed and persist
        try:
            meta["already_pushed"] = True
            _atomic_write_json(meta_path, meta)
        except Exception as e:
            print(f"(warn) failed to mark already_pushed in {meta_path}: {e}")

    print(f"\n=== Rejections from metadata ===\nFolders scanned: {scanned}\nRows inserted: {inserted}\n")
    return scanned, inserted

# ________________ Database Script _____________

ATTACHMENTS_ROOT = "attachments"
_ALLOWED_EXTS = {".xlsx", ".xls", ".csv"}

EXPECTED_COLS = ["Code", "Old Rate", "New Rate", "Effective Date", "Status", "Change Type", "Notes"]

# ------------ metadata helpers ------------

def _parse_iso_utc_safe(s: Optional[str]) -> Optional[datetime]:
    if not s or not isinstance(s, str):
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def _atomic_write_json(path: Path, data: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def load_metadata(folder: Path) -> Optional[Dict[str, Any]]:
    meta = folder / "metadata.json"
    if not meta.exists():
        return None
    try:
        with meta.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def save_metadata(folder: Path, data: Dict[str, Any]) -> None:
    # atomic write to avoid corrupting metadata.json
    path = folder / "metadata.json"
    import tempfile, os
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", dir=folder) as tmp:
        json.dump(data, tmp, ensure_ascii=False, indent=2)
        tmp.flush(); os.fsync(tmp.fileno())
        tmpname = tmp.name
    os.replace(tmpname, path)

def comparison_result_ok(meta: Dict[str, Any]) -> bool:
    """
    True iff metadata has a result 'ok' under either:
      meta['comparison_result']['result']  or  meta['comparision_result']['result']
    """
    d = meta.get("comparison_result") or meta.get("comparision_result")
    if not isinstance(d, dict):
        return False
    return str(d.get("result", "")).strip().lower() == "ok"

def parse_received_at(meta: Dict[str, Any]) -> Optional[datetime]:
    raw = meta.get("receivedDateTime_raw")
    if isinstance(raw, str) and raw.strip():
        try:
            s = raw.strip().replace("Z", "+00:00")
            return datetime.fromisoformat(s).astimezone(timezone.utc)
        except Exception:
            pass
    date_s = meta.get("date_utc")
    time_s = meta.get("time_utc")
    if isinstance(date_s, str) and date_s.strip():
        try:
            ts = f"{date_s.strip()}T{(time_s or '00:00:00').strip()}"
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt
        except Exception:
            return None
    return None

def mark_results_pushed(folder: Path, filename: str, status: Any) -> None:
    meta = load_metadata(folder) or {}
    rp = meta.get("results_pushed")
    if not isinstance(rp, dict):
        rp = {}
    rp[filename] = status
    meta["results_pushed"] = rp
    save_metadata(folder, meta)

# ------------ file discovery ------------

def find_result_files(folder: Path) -> List[Path]:
    """
    Return files whose stem **ends with** '_comparision_result' (case-insensitive)
    and have an allowed extension.
    """
    out: List[Path] = []
    for f in sorted(folder.iterdir()):
        if not f.is_file():
            continue
        if f.suffix.lower() not in _ALLOWED_EXTS:
            continue
        if f.stem.lower().endswith("_comparision_result"):
            out.append(f)
    return out

# ------------ reading & transform ------------

def read_comparison_table(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(path)
    elif ext == ".csv":
        df = pd.read_csv(path)
    else:
        raise ValueError(f"Unsupported file type: {path.suffix}")

    # robust column rename
    rename_map: Dict[str, str] = {}
    cols_norm = {c: " ".join(str(c).strip().split()).lower() for c in df.columns}
    for c, n in cols_norm.items():
        if n == "code": rename_map[c] = "Code"
        elif n == "old rate": rename_map[c] = "Old Rate"
        elif n == "new rate": rename_map[c] = "New Rate"
        elif n == "effective date": rename_map[c] = "Effective Date"
        elif n == "status": rename_map[c] = "Status"
        elif n == "change type": rename_map[c] = "Change Type"
        elif n == "notes": rename_map[c] = "Notes"
        elif n == "old billing increment": rename_map[c] = "Old Billing Increment"
        elif n == "new billing increment": rename_map[c] = "New Billing Increment"
        elif n == "code name": rename_map[c] = "Code Name"
    df = df.rename(columns=rename_map)

    # ensure required columns exist
    missing = [c for c in EXPECTED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"{path.name}: missing expected columns {missing}. Found: {list(df.columns)}")

    # Clean types but DO NOT drop optional columns
    df["Code"] = df["Code"].astype(str).str.strip()
    df = df[df["Code"].ne("")]  # drop empty codes
    df["Old Rate"] = pd.to_numeric(df["Old Rate"], errors="coerce")
    df["New Rate"] = pd.to_numeric(df["New Rate"], errors="coerce")
    df["Effective Date"] = pd.to_datetime(df["Effective Date"], errors="coerce", utc=True)
    df.dropna(how="all", inplace=True)
    return df

def df_to_detail_dicts(df: pd.DataFrame) -> List[Dict[str, Any]]:
    details: List[Dict[str, Any]] = []

    has_old_bi   = "Old Billing Increment" in df.columns
    has_new_bi   = "New Billing Increment" in df.columns
    has_code_name = "Dst Code Name" in df.columns

    for _, r in df.iterrows():
        eff = r["Effective Date"]
        eff_py = None if pd.isna(eff) else eff.to_pydatetime()  # tz-aware UTC

        item: Dict[str, Any] = {
            "dst_code": None if pd.isna(r["Code"]) else str(r["Code"]).strip(),
            "rate_existing": None if pd.isna(r["Old Rate"]) else float(r["Old Rate"]),
            "rate_new": None if pd.isna(r["New Rate"]) else float(r["New Rate"]),
            "effective_date": eff_py,
            "change_type": None if pd.isna(r["Change Type"]) else str(r["Change Type"]).strip(),
            "status": None if pd.isna(r["Status"]) else str(r["Status"]).strip(),
            "notes": None if pd.isna(r["Notes"]) else str(r["Notes"]).strip(),
        }

        # Optional extras (only if present)
        if has_old_bi:
            v = r.get("Old Billing Increment")
            item["old_billing_increment"] = None if pd.isna(v) else str(v).strip()
        if has_new_bi:
            v = r.get("New Billing Increment")
            item["new_billing_increment"] = None if pd.isna(v) else str(v).strip()
        if has_code_name:
            v = r.get("Dst Code Name")
            item["code_name"] = None if pd.isna(v) else str(v).strip()

        details.append(item)

    return details

BOUND = r"(?:(?<=^)|(?<=,))\s*{label}\s*(?:(?=,)|(?=$))"  # comma-boundary regex

def _has_ct(df: pd.DataFrame, label: str) -> pd.Series:
    pat = re.compile(BOUND.format(label=re.escape(label)), flags=re.IGNORECASE)
    ct = df.get("Change Type", pd.Series([], dtype="object")).astype(str)
    return ct.str.contains(pat, na=False)

def compute_upload_stats(dfs: List[pd.DataFrame]) -> Dict[str, int]:
    if not dfs:
        return {
            "total_rows": 0,
            "new": 0, "increase": 0, "decrease": 0, "unchanged": 0, "closed": 0,
            "backdated_increase": 0, "backdated_decrease": 0,
            "billing_increment_changes": 0,
        }

    df = pd.concat(dfs, ignore_index=True)

    # Membership by Change Type (supports multi-label like "Billing ... Changes,Backdated Increase")
    is_new      = _has_ct(df, "New")
    is_closed   = _has_ct(df, "Closed")
    is_unchanged= _has_ct(df, "Unchanged")

    is_back_inc = _has_ct(df, "Backdated Increase")
    is_back_dec = _has_ct(df, "Backdated Decrease")

    # Normal inc/dec exclude backdated so totals don’t double-count
    is_inc = _has_ct(df, "Increase") & ~is_back_inc
    is_dec = _has_ct(df, "Decrease") & ~is_back_dec

    # Billing increment changes: prefer ground truth from columns if present;
    # otherwise fall back to label membership.
    bic = 0
    obi = df.get("Old Billing Increment")
    nbi = df.get("New Billing Increment")
    if obi is not None and nbi is not None:
        # Compare with nulls treated as equal and types normalized
        o = pd.Series(obi, dtype="string").fillna("")
        n = pd.Series(nbi, dtype="string").fillna("")
        bic = int((o != n).sum())
    else:
        bic = int(_has_ct(df, "Billing Increments Changes").sum())

    return {
        "total_rows": int(len(df)),
        "new":               int(is_new.sum()),
        "increase":          int(is_inc.sum()),
        "decrease":          int(is_dec.sum()),
        "unchanged":         int(is_unchanged.sum()),
        "closed":            int(is_closed.sum()),
        "backdated_increase":int(is_back_inc.sum()),
        "backdated_decrease":int(is_back_dec.sum()),
        "billing_increment_changes": bic,
    }

def push_all_ok_results(attachments_root: str | Path) -> Tuple[int, int, int]:
    """
    Idempotent push:
      - Only create a rate_uploads row if there is at least ONE result file that
        has not been pushed yet AND has at least one row to insert.
      - Process only files not previously marked as pushed in metadata.json.
      - Mark results_pushed[filename] per file with True/False (or a string reason).

    Returns: (folders_processed, files_processed, rows_inserted_total)
    """
    root = Path(attachments_root).expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"Attachments directory not found: {root}")

    folders_done = 0
    files_done = 0
    rows_total = 0

    print("\n=== DB push over OK comparison-result folders ===")
    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue

        meta = load_metadata(child)
        if not meta or not comparison_result_ok(meta):
            continue

        # Discover result files in this folder
        result_files = find_result_files(child)
        if not result_files:
            continue

        # Determine which files still need to be pushed (strict idempotency gate)
        rp = meta.get("results_pushed") or {}
        to_push = [f for f in result_files if rp.get(f.name) is not True]
        if not to_push:
            # Nothing left to do in this folder
            continue

        # Read only the files we plan to push (stats + pre-check for empties)
        dfs_to_push: List[pd.DataFrame] = []
        per_file_df: Dict[str, pd.DataFrame] = {}
        for f in to_push:
            try:
                df_tmp = read_comparison_table(f)
                # Remember the DF even if empty so we can mark status later
                per_file_df[f.name] = df_tmp
                if not df_tmp.empty:
                    dfs_to_push.append(df_tmp)
            except Exception as e:
                print(f"    ⚠ failed reading {f.name} for stats aggregation: {e}")
                per_file_df[f.name] = pd.DataFrame()  # treat as empty so it won't insert

        # If every to_push DF is empty, don't create a parent record; just mark files
        if not any((not d.empty) for d in per_file_df.values()):
            for f in to_push:
                mark_results_pushed(child, f.name, "empty file no results to push to the database")
            # Nothing inserted, but we did meaningful work → count folder processed
            folders_done += 1
            print(f"\n[FOLDER] {child}")
            print("  (All to-push files empty → no upload created)")
            continue

        # Aggregate stats from only the DFs that have rows
        stats_totals = compute_upload_stats(dfs_to_push)

        # Ready to create the parent upload row now (idempotent: only when there's work)
        folders_done += 1
        print(f"\n[FOLDER] {child}")

        sender = str(meta.get("sender") or "").strip()
        subject = (meta.get("subject") or "").strip() or None
        received_at = parse_received_at(meta)
        processed_at = _parse_iso_utc_safe(meta.get("processed_at_utc"))

        try:
            upload_id = insert_rate_upload(
                sender_email=sender or None,
                subject=subject,
                received_at=received_at,
                processed_at=processed_at,
                totals=stats_totals,
            )
        except Exception as e:
            # Parent failed → mark all to_push files as failed so we don't spin forever
            print(f"  ✖ Failed to create rate_upload row: {e}")
            for f in to_push:
                mark_results_pushed(child, f.name, False)
            continue

        # Process only files that still need pushing
        for f in to_push:
            print(f"  - Processing {f.name}")
            try:
                df = per_file_df.get(f.name)
                if df is None:
                    # Safety: read again if it wasn't cached
                    df = read_comparison_table(f)

                if df.empty:
                    print("    ⚠ empty comparison file; nothing to push")
                    mark_results_pushed(child, f.name, "empty file no results to push to the database")
                    continue

                details = df_to_detail_dicts(df)

                # If you have a UNIQUE constraint on details, turn this into an UPSERT there.
                inserted = bulk_insert_rate_upload_details(upload_id, details)
                rows_total += inserted
                files_done += 1
                print(f"    ✔ inserted {inserted} rows")
                mark_results_pushed(child, f.name, True)

            except Exception as e:
                print(f"    ✖ failed to insert from {f.name}: {e}")
                mark_results_pushed(child, f.name, False)

    print("\n=== DB push summary ===")
    print(f"Folders processed: {folders_done}")
    print(f"Files processed:   {files_done}")
    print(f"Rows inserted:     {rows_total}")
    return folders_done, files_done, rows_total

def finalize_processed_flags(paths_map: Dict[str, Optional[str]]) -> Tuple[int, int, int]:
    """
    Given {file_path: date_format or None}, look up each file's parent folder.
    If that folder's metadata.json has results_pushed with all values strictly True,
    mark that specific file_path row as is_processed = TRUE in the DB.

    Returns: (dirs_scanned, eligible_dirs, rows_marked)
    """
    # Deduplicate by directory, but keep a mapping back to at least one file_path per dir
    dir_to_any_fp: Dict[Path, str] = {}
    for fp in paths_map.keys():
        try:
            d = Path(fp).parent.resolve()
        except Exception:
            continue
        # keep the first seen file_path for this dir
        dir_to_any_fp.setdefault(d, fp)

    dirs_scanned = 0
    eligible_dirs = 0
    to_mark: list[str] = []

    for d, fp_for_dir in dir_to_any_fp.items():
        dirs_scanned += 1
        meta_path = d / "metadata.json"
        if not meta_path.exists():
            print(f"[INGEST][SKIP] No metadata.json in {d}")
            continue

        try:
            with meta_path.open("r", encoding="utf-8") as f:
                meta = json.load(f)
        except Exception as e:
            print(f"[INGEST][WARN] Failed to read {meta_path}: {e}")
            continue

        rp = meta.get("results_pushed")
        if not isinstance(rp, dict) or not rp:
            print(f"[INGEST][SKIP] No results_pushed or empty in {meta_path}")
            continue

        # All entries must be strictly boolean True
        all_true = all(v is True for v in rp.values())
        if not all_true:
            print(f"[INGEST][WAIT] Not all results pushed in {meta_path} -> {rp}")
            continue

        # This directory is eligible -> mark its chosen file_path
        eligible_dirs += 1
        to_mark.append(fp_for_dir)

    rows_marked = 0
    if to_mark:
        try:
            rows_marked = mark_ingest_processed(to_mark, processed=True)
            print(f"[INGEST] Marked is_processed=TRUE for {rows_marked} rows")
        except Exception as e:
            print(f"[INGEST][ERR] Failed to update is_processed flags: {e}")

    print(f"[INGEST] finalize_processed_flags summary: dirs_scanned={dirs_scanned}, "
          f"eligible_dirs={eligible_dirs}, rows_marked={rows_marked}")
    return dirs_scanned, eligible_dirs, rows_marked

#______________________________________________________________________________


if __name__ == "__main__":
    # scrap all the valid emails
    verify_fetch_emails(after, before, unread_only)

    #________________________________________________
    # run the ingestion pass before any further processing
    ingest_files_for_manual_date("attachments")

    valid_paths = fetch_approved_unprocessed_paths_map()

    mark_date_verification_ingestion(valid_paths)
    #________________________________________________


    # fetch jerasoft rates
    process_all_directories()

    # preprocessing the files
    clean_preprocessed_folders("attachments")

    # running comparision engine on all the files
    compare_preprocessed_folders("attachments", notice_days=7, rate_tol=0.0001)  #check the difference upto 4 decimal places.

    # pushing all the relevant details to the data base
    push_rejections_from_metadata("attachments")

    push_all_ok_results(ATTACHMENTS_ROOT)

    push_failed_emails_json_to_db("failed_emails.json")  


    #___________________________________________________________
    # Setting the is_processed status of the processed files to true in the db
    # Try to mark is_processed for those whose folders have fully-pushed results
    finalize_processed_flags(valid_paths)