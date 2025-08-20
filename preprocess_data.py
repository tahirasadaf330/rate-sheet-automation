import pandas as pd, os, re, numpy as np
from datetime import datetime
from dateutil import parser as dparse

REQUIRED_COLS = [
    'Dst Code', 'Rate', 'Effective Date', 'Billing Increment'
]

EXCEL_EPOCH = datetime(1899, 12, 30)          # Excel’s epoch (PC versions)

# ──────────────────────────────── headers ───────────────────────────────────

def detect_header_row(raw: pd.DataFrame) -> int:
    """
    Find the row index that, after normalization + alias mapping,
    contains ALL required canonical columns.
    """
    # Canonical targets
    targets = set(REQUIRED_COLS)

    for idx, row in raw.iterrows():
        # 1) normalize each cell
        normed = [_norm(x) for x in row if pd.notna(x)]
        # print(normed)

        if not normed:
            continue
        # 2) map normalized -> canonical using ALIAS_MAP (unknowns stay None)
        mapped = {ALIAS_MAP.get(n) for n in normed if ALIAS_MAP.get(n)}

        # print(mapped)
        
        # 3) if we covered all targets, we found the header row
        if targets.issubset({m for m in mapped if m}):
            return idx

    raise ValueError(
        "Header not found. None of the rows contained all required columns "
        f"after normalization/aliasing. Required: {REQUIRED_COLS}"
    )


def _norm(s: str) -> str:
    s = str(s).strip().lower()
    s = s.replace('-', ' ').replace('/', ' ')
    s = ' '.join(s.split())            # collapse spaces
    return s.replace(' ', '_')         # final form, e.g. "effective_date"


# Map of normalized header variants -> canonical
ALIAS_MAP = {
    # Dst Code
    'dst_code': 'Dst Code',
    'dstcode': 'Dst Code',
    'code': 'Dst Code',
    'destination_code': 'Dst Code',
    'dest_code': 'Dst Code',
    'area_code': 'Dst Code',
    'dial_code': 'Dst Code',
    'dialcode': 'Dst Code',
    'codes': 'Dst Code',

    # Rate
    'rate': 'Rate',
    'rates': 'Rate',
    'price': 'Rate',
    'new_rates': 'Rate',
    'new_rate': 'Rate',
    'cost': 'Rate',
    'pricing': 'Rate',
    'price': 'Rate',
    'pricing_in': 'Rate',
    'rate_min($)': 'Rate',

    # Effective Date
    'effective_date': 'Effective Date',
    'effective': 'Effective Date',
    'eff_date': 'Effective Date',
    'effective_from': 'Effective Date',
    'start_date': 'Effective Date',
    'valid_from': 'Effective Date',
    'date': 'Effective Date',
    'effectivedate': 'Effective Date',

    # Billing Increment
    'billing_increment': 'Billing Increment',
    'billing_increament': 'Billing Increment',   # common typo
    'billing_increments': 'Billing Increment',
    'billing_inc': 'Billing Increment',
    'billing': 'Billing Increment',
    'billingincrement': 'Billing Increment',
}


def _canonicalize_headers(df: pd.DataFrame) -> pd.DataFrame:
    original = list(df.columns)
    mapped = {}

    for c in df.columns:
        key = _norm(c)
        if key in ALIAS_MAP:
            mapped[c] = ALIAS_MAP[key]
        else:
            mapped[c] = c  # keep unknowns
    
    df = df.rename(columns=mapped)

    # Verify required columns exist (in canonical names)
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        # Helpful error with what we saw and our normalized forms
        seen_norm = {col: _norm(col) for col in original}
        raise ValueError(
            "Missing required columns: "
            f"{missing}. Found headers: {original}. "
            f"Normalized: {seen_norm}. "
            "Add more variants to ALIAS_MAP if needed."
        )
    return df


# ──────────────────────────────── footer ─────────────────────────────────

# Customize the keywords if you want to add more
_NOTES_RE = re.compile(r'^(note|notes|remark|remarks|disclaimer|footer|terms and conditions)\b', re.IGNORECASE)

def _is_empty_row(row: pd.Series) -> bool:
    """True if every cell in the row is NaN or only whitespace."""
    for v in row:
        if pd.isna(v):
            continue
        if isinstance(v, str):
            if v.strip() != "":
                return False
        else:
            # non-string, non-NaN value counts as data
            return False
    return True

def trim_after_notes_and_strip_blank_above(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Find the first row where ANY cell starts with one of the notes keywords.
    - Cut the DataFrame to everything ABOVE that row.
    - Then remove trailing blank rows immediately above the notes marker.
    If no notes marker is found, returns df unchanged.
    """
    if df.empty:
        return df

    # Build a normalized view for matching
    norm = df.applymap(lambda x: "" if pd.isna(x) else str(x).strip())

    # Row-wise: does ANY cell start with a notes-like keyword?
    marker_mask = norm.apply(lambda r: any(_NOTES_RE.match(cell) for cell in r), axis=1)

    if not marker_mask.any():
        # no notes marker -> return as-is
        return df

    # Position of first notes marker (iloc)
    marker_index_label = marker_mask.idxmax()  # first True by index order
    marker_iloc = df.index.get_loc(marker_index_label)

    # Everything strictly ABOVE the marker
    end_iloc = marker_iloc - 1

    # Strip trailing blank rows immediately above the marker
    while end_iloc >= 0 and _is_empty_row(df.iloc[end_iloc]):
        end_iloc -= 1

    # If nothing left, return empty frame with same columns
    if end_iloc < 0:
        return df.iloc[0:0].copy()

    return df.iloc[:end_iloc + 1].copy()

# ───────────────────────────────── helpers ──────────────────────────────────

def normalise_date_any(val) -> pd.Timestamp:
    """Return pandas.Timestamp (UTC-naive) or NaT if invalid."""
    if val is None or (isinstance(val, float) and np.isnan(val)) or str(val).strip() == '':
        return pd.NaT

    s = str(val).strip()

    # Excel serial (integer days since 1899-12-30)
    if re.fullmatch(r'\d{1,6}', s):
        try:
            serial = int(s)
            if serial > 0:
                return EXCEL_EPOCH + pd.Timedelta(days=serial)
        except Exception:
            return pd.NaT

    # Remove timezone suffixes like +0000 or Z
    s = re.sub(r'\s*\+\d{4}$', '', s).rstrip('Zz')
    s = s.replace('/', '-').replace('.', '-')

    for dayfirst in (True, False):
        try:
            dt = dparse.parse(s, dayfirst=dayfirst, fuzzy=True,
                              default=datetime(1900, 1, 1))
            return pd.Timestamp(dt.date())
        except Exception:
            continue

    return pd.NaT

def clean_billing_increment(val: str) -> str:
    if pd.isna(val):
        return ''
    parts = [p for p in str(val).split('/') if p.strip().isdigit()]
    # If format like 0/1/1, drop leading zero and keep last two numbers
    if len(parts) >= 2:
        return f"{int(parts[-2])}/{int(parts[-1])}"
    return ''

# ───────────────────────────── loader main ───────────────────────────────────
def load_clean_rates(path: str, output_path: str, sheet=0) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()
    if not os.path.exists(path):
        raise FileNotFoundError(f'File not found: {path}')

    # read raw with no header
    raw = (pd.read_csv(path, header=None, dtype=str)
        if ext in ('.csv', '.txt')
        else pd.read_excel(path, sheet_name=sheet, header=None, dtype=str))

    header_row_idx = detect_header_row(raw)

    # re-read with the detected header row
    df = (pd.read_csv(path, header=header_row_idx, dtype=str)
        if ext in ('.csv', '.txt')
        else pd.read_excel(path, sheet_name=sheet, header=header_row_idx, dtype=str))

    # canonicalize headers NOW (after re-read)
    df = _canonicalize_headers(df)

    df = trim_after_notes_and_strip_blank_above(df)

    # keep only canonical required columns
    df = df[REQUIRED_COLS].copy()


    # ── clean fields ────────────────────────────────────────────────────────
    df['Dst Code'] = df['Dst Code'].astype(str).str.strip()

    # Robust numeric parsing for Rate; keep as float, leave invalids as NaN
    s = df['Rate'].astype(str).str.strip()

    # Normalize common vendor formats
    s = (s
        .str.replace(r'[\$\£\€]', '', regex=True)     # currency symbols
        .str.replace(r'\s+', '', regex=True)          # stray spaces
    )

    df['Rate'] = pd.to_numeric(s, errors='coerce')
    df['Billing Increment'] = df['Billing Increment'].astype(str).str.strip().apply(clean_billing_increment)


    df['Effective Date'] = df['Effective Date'].apply(normalise_date_any)

    df.to_excel(output_path, index=False)


# ──────────────────────────── quick test ─────────────────────────────────────
if __name__ == '__main__':
    FILE_PATH = r'C:\Users\User\OneDrive - Hayo Telecom, Inc\Documents\Work\Rate Sheet Automation\Code\quickcomefetched389.xlsx'
    OUTPUT_FILE_PATH = r'test_files/tes_cleaned.xlsx'
    cleaned = load_clean_rates(FILE_PATH, OUTPUT_FILE_PATH, 0)
   

    print('✅ Cleaned and saved.')
