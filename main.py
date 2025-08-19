
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
from typing import Iterable, Tuple



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


from preprocess_data import load_clean_rates   

ALLOWED_EXTS = {".xlsx", ".xls", ".csv"}

def iter_preprocessed_dirs(attachments_root: Path) -> Iterable[Path]:
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
        if bool(data.get("preprocessed")) is True:
            yield child

def files_to_clean(folder: Path) -> Iterable[Path]:
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

def clean_preprocessed_folders(attachments_dir: str | Path) -> Tuple[int, int]:
    """
    For each preprocessed folder:
      - run load_clean_rates(file, file, 0) for every allowed file inside.
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
            except Exception as e:
                print(f"    ✖ failed cleaning {file_path.name}: {e}")

        if not any_files:
            print("  (no CSV/Excel files found)")

    print("\n=== Cleaning summary ===")
    print(f"Folders processed: {folders_done}")
    print(f"Files cleaned:     {files_done}")
    return folders_done, files_done


clean_preprocessed_folders("attachments")

# # also remove the nan values

# #________________ Ratesheet Comparision Script _____________

# LEFT_FILE = r"test_files\Second_File_Updated_cleaned.xlsx" #current rate file
# RIGHT_FILE = r"test_files\First_File_Updated_cleaned.xlsx" #new rate file
# OUT_FILE = "comparison.xlsx"
# AS_OF_DATE = "2025-07-08"
# NOTICE_DAYS = 7
# RATE_TOL = 0.0
# SHEET_LEFT = None
# SHEET_RIGHT = None

# left = read_table(LEFT_FILE, SHEET_LEFT)
# right = read_table(RIGHT_FILE, SHEET_RIGHT)
# result = compare(left, right, AS_OF_DATE, NOTICE_DAYS, RATE_TOL)
# write_excel(result, OUT_FILE)
# print(f"Wrote {OUT_FILE} with {len(result)} rows.")


# #________________ Database Script _____________

