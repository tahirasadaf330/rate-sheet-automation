import json
from pathlib import Path
from datetime import datetime
from database import insert_rate_upload, _parse_iso_utc  # Import your functions
from typing import Dict, Any  # Add these imports at the top of your file


def _totals_from_stats(stats: Dict[str, Any]) -> Dict[str, int]:
    """
    Map the attachment_stats blob into the counters that rate_uploads expects.
    Missing keys default to zero.
    """
    keys = [
        "total_rows", "new", "increase", "decrease", "unchanged", "closed",
        "backdated_increase", "backdated_decrease", "billing_increment_changes",
    ]
    out = {k: 0 for k in keys}
    for k in keys:
        val = stats.get(k, 0)
        try:
            out[k] = int(val)
        except Exception:
            out[k] = 0
    return out

def push_rate_uploads_from_metadata(metadata_path: str | Path) -> List[int]:
    """
    Read metadata.json and push *summary* rows into rate_uploads.
    Creates one rate_uploads row per item in `attachment_stats`.
    Returns list of inserted ids.
    """
    metadata_path = Path(metadata_path)
    if not metadata_path.exists():
        raise FileNotFoundError(f"metadata not found: {metadata_path}")

    with open(metadata_path, "r", encoding="utf-8") as f:
        meta = json.load(f)

    subject = meta.get("subject") or None
    sender_email = meta.get("sender") or None
    received_at = _parse_iso_utc(meta.get("receivedDateTime_raw"))
    processed_at = _parse_iso_utc(meta.get("processed_at_utc"))

    # Prefer attachment_stats (has the counters you want).
    attachment_stats = meta.get("attachment_stats") or {}

    inserted_ids: List[int] = []

    if attachment_stats:
        # One row per attachment’s stats
        for _filename, stats in attachment_stats.items():
            if not isinstance(stats, dict):
                continue
            totals = _totals_from_stats(stats)
            new_id = insert_rate_upload(
                sender_email=sender_email,
                subject=subject,
                received_at=received_at,       # falls back to NOW() inside SQL if None
                processed_at=processed_at,
                totals=totals,
            )
            inserted_ids.append(new_id)
    else:
        # Absolute fallback: push a single row with zeros, but try to get total_rows
        # from human_eval_details_pre if present. This is only for “just testing”.
        total_rows = 0
        pre = (meta.get("human_eval_details_pre") or {})
        # try to grab any "rows" number from that blob
        for v in pre.values():
            if isinstance(v, dict) and "rows" in v:
                try:
                    total_rows = int(v["rows"])
                    break
                except Exception:
                    pass

        totals = {
            "total_rows": total_rows,
            "new": 0,
            "increase": 0,
            "decrease": 0,
            "unchanged": 0,
            "closed": 0,
            "backdated_increase": 0,
            "backdated_decrease": 0,
            "billing_increment_changes": 0,
        }
        new_id = insert_rate_upload(
            sender_email=sender_email,
            subject=subject,
            received_at=received_at,
            processed_at=processed_at,
            totals=totals,
        )
        inserted_ids.append(new_id)

    return inserted_ids


if __name__ == "__main__":
    # Your exact path from the message:
    meta_path = r"C:\Users\User\OneDrive - Hayo Telecom, Inc\Documents\Work\Rate Sheet Automation\rate-sheet-automation\attachments\rates_at_cimatelecom.com_20251001_003531\metadata.json"
    ids = push_rate_uploads_from_metadata(meta_path)
    print(f"(ok) inserted rate_uploads ids: {ids}")
