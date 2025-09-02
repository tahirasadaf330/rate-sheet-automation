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
from typing import Iterable, Tuple, Optional, Dict, Any, List
from datetime import datetime, timezone
from database import insert_rate_upload, bulk_insert_rate_upload_details
import pandas as pd
from typing import Any 
#_____________ Email Verification Script_____________

after = "2025-09-1"              # only include emails on/after this date (YYYY-MM-DD) or None
before = None           # only include emails on/before this date (YYYY-MM-DD) or None
unread_only = False    
ATTEMPTS = 2
verify_fetch_emails(after, before, unread_only)

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

        # Extract subject
        company = meta.get("company")
        subject = meta.get("subject")
        if not company and subject:
            print(f"[WARN] No company found in {meta_file}, skipping...")
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

        info = None
        try:
            info = export_rates_by_query(company, output_path, subject)
            print(info)

            if isinstance(info, str):
                print(f"[ERROR] Export failed for {company}: {info}")
                meta["keyword_error"] = info
                with open(meta_file, "w", encoding="utf-8") as f:
                    json.dump(meta, f, indent=2)
                print(f"[INFO] Updated metadata with keyword_error: {meta_file}")

            else:
                print(f"[INFO] Export succeeded for {company}, updating metadata.")

                if info:
                    meta["best_table_name"] = info.get("best_table_name")

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

process_all_directories()

#_______________ Preprocess Script _____________

ALLOWED_EXTS = {".xlsx", ".xls", ".csv"}

def iter_preprocessed_dirs(attachments_root: Path):
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
                result = compare(left_df, right_df, as_of_date, notice_days, rate_tol)

                # NEW: always name by vendor file + "_comparision_difference.xlsx"
                out_path = folder / f"{v.stem}_comparision_result.xlsx"

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

compare_preprocessed_folders("attachments", notice_days=7, rate_tol=0.0001)  #check the difference upto 4 decimal places.

#________________ Database Script _____________

ATTACHMENTS_ROOT = "attachments"
_ALLOWED_EXTS = {".xlsx", ".xls", ".csv"}

EXPECTED_COLS = ["Code", "Old Rate", "New Rate", "Effective Date", "Status", "Change Type", "Notes"]

# ------------ metadata helpers ------------

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
    df = df.rename(columns=rename_map)

    missing = [c for c in EXPECTED_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"{path.name}: missing expected columns {missing}. Found: {list(df.columns)}")

    df = df[EXPECTED_COLS].copy()
    df["Code"] = df["Code"].astype(str).str.strip()
    df = df[df["Code"].ne("")]  # drop empty codes
    df["Old Rate"] = pd.to_numeric(df["Old Rate"], errors="coerce")
    df["New Rate"] = pd.to_numeric(df["New Rate"], errors="coerce")
    df["Effective Date"] = pd.to_datetime(df["Effective Date"], errors="coerce", utc=True)
    df.dropna(how="all", inplace=True)
    return df

def df_to_detail_dicts(df: pd.DataFrame) -> List[Dict[str, Any]]:
    details: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        eff = r["Effective Date"]
        eff_py = None if pd.isna(eff) else eff.to_pydatetime()  # tz-aware UTC
        details.append({
            "dst_code": None if pd.isna(r["Code"]) else str(r["Code"]).strip(),
            "rate_existing": None if pd.isna(r["Old Rate"]) else float(r["Old Rate"]),
            "rate_new": None if pd.isna(r["New Rate"]) else float(r["New Rate"]),
            "effective_date": eff_py,
            "change_type": None if pd.isna(r["Change Type"]) else str(r["Change Type"]).strip(),
            "status": None if pd.isna(r["Status"]) else str(r["Status"]).strip(),
            "notes": None if pd.isna(r["Notes"]) else str(r["Notes"]).strip(),
        })
    return details

# ------------ main pipeline ------------

def push_all_ok_results(attachments_root: str | Path) -> Tuple[int, int, int]:
    """
    For each folder whose metadata compar(i)son_result.result == 'ok':
      - insert into rate_uploads (sender, received_at)
      - read every file ending with *_comparision_result.{xlsx|xls|csv}
      - bulk insert rows into rate_upload_details
      - update metadata.json -> results_pushed[filename] = True/False
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

        result_files = find_result_files(child)
        if not result_files:
            continue

        # Only push files for vendors that compare stage marked True
        comp = (meta.get("comparision_result") or meta.get("comparison_result") or {})
        ok_vendor_stems = {Path(k).stem for k, v in comp.items() if k != "result" and v is True}

        def _vendor_stem_from_result_file(p: Path) -> str:
            s = p.stem
            suf = "_comparision_result"
            return s[:-len(suf)] if s.endswith(suf) else s

        result_files = [f for f in result_files if _vendor_stem_from_result_file(f) in ok_vendor_stems]
        if not result_files:
            continue

        rp = meta.get("results_pushed") or {}
        already_all = result_files and all(rp.get(f.name) is True for f in result_files)
        if already_all:
            print(f"  [SKIP] all results already pushed for {child.name}")
            continue

        folders_done += 1
        print(f"\n[FOLDER] {child}")

        sender = str(meta.get("sender") or "").strip()
        received_at = parse_received_at(meta)

        try:
            upload_id = insert_rate_upload(sender_email=sender or None, received_at=received_at)
        except Exception as e:
            print(f"  ✖ Failed to create rate_upload row: {e}")
            # mark only not-yet-pushed files as failed
            for f in result_files:
                if rp.get(f.name) is True:
                    continue
                mark_results_pushed(child, f.name, False)
            continue

        # per-file skip for already-pushed
        rp = meta.get("results_pushed") or {}
        for f in result_files:
            if rp.get(f.name) is True:
                print(f"  - Skipping already pushed: {f.name}")
                continue

            print(f"  - Processing {f.name}")
            try:
                df = read_comparison_table(f)
                if df.empty:
                    print("    ⚠ empty comparison file; nothing to push")
                    mark_results_pushed(child, f.name, "empty file no results to push to the data base")
                    continue
                details = df_to_detail_dicts(df)
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

push_all_ok_results(ATTACHMENTS_ROOT)
