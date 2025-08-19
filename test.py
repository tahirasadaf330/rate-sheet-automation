import pandas as pd
from pathlib import Path

def _find_dst_code_col(df):
    """Return the actual column name to use for 'Dst Code' (case/space tolerant)."""
    # try exact first
    if 'Dst Code' in df.columns:
        return 'Dst Code'
    # fallbacks: normalize spaces/case
    norm = {c: ''.join(str(c).strip().lower().split()) for c in df.columns}
    for col, key in norm.items():
        if key in ('dstcode', 'destcode', 'destinationcode'):
            return col
    # last resort: look for both 'dst' and 'code' words
    for col in df.columns:
        lc = str(col).lower()
        if 'code' in lc and ('dst' in lc or 'dest' in lc or 'destination' in lc):
            return col
    raise KeyError("Could not find a 'Dst Code' column.")

def _load_codes(path, sheet_name=0):
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    df = pd.read_excel(path, sheet_name=sheet_name)
    col = _find_dst_code_col(df)
    # treat as string, strip spaces; drop NaNs
    codes = (
        df[col]
        .astype(str)
        .str.strip()
        .replace({'nan': None, 'None': None})
        .dropna()
    )
    return set(codes), col

def compare_dst_code(file1, file2, sheet1=0, sheet2=0, write_report=True):
    """
    Compare 'Dst Code' sets between two Excel files.

    Returns a dict with summary + differences. Optionally writes a CSV report.
    """
    s1, col1 = _load_codes(file1, sheet1)
    s2, col2 = _load_codes(file2, sheet2)

    same = (s1 == s2)
    only_in_1 = sorted(s1 - s2)
    only_in_2 = sorted(s2 - s1)
    in_both = sorted(s1 & s2)

    print("=== Dst Code comparison ===")
    print(f"File 1: {file1}  (column: {col1}, unique codes: {len(s1)})")
    print(f"File 2: {file2}  (column: {col2}, unique codes: {len(s2)})")
    print(f"\nAre they identical? -> {'YES' if same else 'NO'}")

    if not same:
        print(f"\nOnly in File 1 ({len(only_in_1)}):")
        if only_in_1[:10]:  # show a preview
            print(only_in_1[:10], "..." if len(only_in_1) > 10 else "")
        print(f"\nOnly in File 2 ({len(only_in_2)}):")
        if only_in_2[:10]:
            print(only_in_2[:10], "..." if len(only_in_2) > 10 else "")

    result = {
        "identical": same,
        "file1": str(file1),
        "file2": str(file2),
        "file1_unique_count": len(s1),
        "file2_unique_count": len(s2),
        "only_in_file1": only_in_1,
        "only_in_file2": only_in_2,
        "in_both_count": len(in_both),
    }

    if write_report:
        base = f"dst_code_diff_{Path(file1).stem}_vs_{Path(file2).stem}.csv"
        out_path = Path(file1).parent / base
        # build a small report dataframe
        max_len = max(len(only_in_1), len(only_in_2))
        left = only_in_1 + [""] * (max_len - len(only_in_1))
        right = only_in_2 + [""] * (max_len - len(only_in_2))
        report = pd.DataFrame({"Only in File 1": left, "Only in File 2": right})
        report.to_csv(out_path, index=False)
        print(f"\nDiff report written to: {out_path}")
        result["report_path"] = str(out_path)

    return result

# Example usage:
compare_dst_code(r"C:\Users\User\OneDrive - Hayo Telecom, Inc\Documents\Work\Rate Sheet Automation\Code\Refined_Codebase\quickcom_rates.xlsx", r"C:\Users\User\OneDrive - Hayo Telecom, Inc\Documents\Work\Rate Sheet Automation\Code\quickcomefetched389.xlsx")
