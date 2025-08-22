import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from valid_emails import VERIFIED_SENDERS
from typing import Iterable, Dict, Any, Optional, List
from datetime import datetime
from decimal import Decimal

# Load environment variables
load_dotenv()

def get_conn():
    """Create a new PostgreSQL connection using env vars."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_DATABASE"),
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
    )

def insert_authorized_senders(emails):
    """
    Insert a list of emails into authorized_senders.
    Uses ON CONFLICT DO NOTHING to avoid duplicate errors.
    """
    query = """
        INSERT INTO authorized_senders (email, status, created_at, updated_at)
        VALUES %s
        ON CONFLICT (email) DO NOTHING;
    """

    # Prepare rows: status = true, timestamps = now()
    rows = [(email, True, "NOW()", "NOW()") for email in emails]

    # psycopg2 cannot handle NOW() as string, so use SQL functions directly
    values_template = "(%s, %s, NOW(), NOW())"

    with get_conn() as conn, conn.cursor() as cur:
        execute_values(cur, query, [(email, True) for email in emails], template=values_template)
        conn.commit()
        print(f"Inserted {cur.rowcount} new emails into authorized_senders.")


# -----------------------------
# 1) Insert into rate_uploads and RETURN id
# -----------------------------
def insert_rate_upload(sender_email: str, received_at: Optional[datetime] = None) -> int:
    """
    Insert a row into rate_uploads and return its id.
    """
    query = """
        INSERT INTO rate_uploads (sender_email, received_at, created_at, updated_at)
        VALUES (%s, COALESCE(%s, NOW()), NOW(), NOW())
        RETURNING id;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(query, (sender_email, received_at))
        rate_upload_id = cur.fetchone()[0]
        conn.commit()
        print(f"Inserted rate upload from {sender_email} → id={rate_upload_id}")
        return rate_upload_id
    
# # -----------------------------
# # 3) BULK insert many details for the same rate_upload_id
# # -----------------------------
# def bulk_insert_rate_upload_details(
#     rate_upload_id: int,
#     details: Iterable[Dict[str, Any]],
# ) -> int:
#     """
#     Bulk insert many rows into rate_upload_details for one rate_upload_id.
#     Each dict supports keys: dst_code (required), rate_existing, rate_new,
#     effective_date, change_type, status, notes.
#     Returns number of inserted rows.
#     """
#     rows = []
#     for d in details:
#         rows.append((
#             rate_upload_id,
#             d["dst_code"],
#             d.get("rate_existing"),
#             d.get("rate_new"),
#             d.get("effective_date"),
#             d.get("change_type"),
#             d.get("status"),
#             d.get("notes"),
#         ))

#     if not rows:
#         return 0

#     query = """
#         INSERT INTO rate_upload_details
#         (rate_upload_id, dst_code, rate_existing, rate_new, effective_date,
#          change_type, status, notes, created_at, updated_at)
#         VALUES %s;
#     """
#     values_tpl = "(%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())"

#     with get_conn() as conn, conn.cursor() as cur:
#         execute_values(cur, query, rows, template=values_tpl)
#         inserted = cur.rowcount
#         conn.commit()
#         return inserted







def _chunked(seq: List[tuple], size: int):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def bulk_insert_rate_upload_details(
    rate_upload_id: int,
    details: Iterable[Dict[str, Any]],
    batch_size: int = 1000,   # tune as needed
) -> int:
    """
    Bulk insert rows and return the TRUE total inserted count.
    Works even when execute_values paginates internally.
    """
    rows = [
        (
            rate_upload_id,
            d["dst_code"],
            d.get("rate_existing"),
            d.get("rate_new"),
            d.get("effective_date"),
            d.get("change_type"),
            d.get("status"),
            d.get("notes"),
        )
        for d in details
    ]
    if not rows:
        return 0

    sql = """
        INSERT INTO rate_upload_details
        (rate_upload_id, dst_code, rate_existing, rate_new, effective_date,
         change_type, status, notes, created_at, updated_at)
        VALUES %s
        RETURNING 1
    """
    tpl = "(%s,%s,%s,%s,%s,%s,%s,%s,NOW(),NOW())"

    total = 0
    with get_conn() as conn, conn.cursor() as cur:
        for chunk in _chunked(rows, batch_size):
            # Ensure a single statement per chunk by making page_size=len(chunk)
            try:
                # psycopg2>=2.8 supports fetch=True: returns all RETURNING rows across pages
                returned = execute_values(
                    cur, sql, chunk, template=tpl, page_size=len(chunk), fetch=True
                )
                total += len(returned) if returned is not None else 0
            except TypeError:
                # Older psycopg2: no fetch kwarg. Use rowcount per chunk.
                execute_values(cur, sql, chunk, template=tpl, page_size=len(chunk))
                total += cur.rowcount  # count for this chunk (single statement)
        conn.commit()
    return total


# # insert_authorized_senders(VERIFIED_SENDERS)