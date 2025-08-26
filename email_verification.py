#!/usr/bin/env python3
"""
Process Outlook Inbox via Microsoft Graph using app permissions:
- Verify sender against VERIFIED_SENDERS
- Validate Subject via SUBJECT_PATTERN
- Save only allowed attachment types (.csv, .xlsx, etc.) with safe filenames
- Make per-message folders named <sender>_<UTC timestamp>
- Log short body text (HTML → text if needed)
- Write per-message metadata.json alongside saved attachments

Env (.env next to script):
  TENANT_ID=...
  CLIENT_ID=...
  CLIENT_SECRET=...
  USER_EMAIL=target_mailbox@domain.com
Optional:
  VERBOSE=1  # to print decoded token roles
"""

import os, sys, re, json, base64, time, unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Set, Tuple, Optional, List, Dict

import requests
from msal import ConfidentialClientApplication
from dotenv import load_dotenv
from urllib.parse import quote
from open_ai import validate_subject_openai

from valid_emails import VERIFIED_SENDERS  # list of allowed sender emails

# ========================= Debug Toggles =========================
DEBUG = False         # ultra-verbose prints for every step
DRY_RUN = False      # if True, do not write files; only log decisions
# ================================================================

def dbg(*args, **kwargs):
    if DEBUG:
        print("[DEBUG]", *args, **kwargs)

# ------------------------- Config & Regex -------------------------

SCOPES = ["https://graph.microsoft.com/.default"]

VERIFIED = {e.lower().strip() for e in VERIFIED_SENDERS}

# ------------------------- Subject Normalization & Parsing -------------------------

def _normalize_subject(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"[:;|,/\\]+", " ", s)            # common separators → space
    s = re.sub(r"[^A-Za-z0-9_\-\s]", " ", s)     # drop ~!@#$%^&*()[]{}<>'"?.+= etc.
    s = re.sub(r"\s+", " ", s).strip()
    return s

# Strict/ordered patterns
_FREEFORM = re.compile(r"""
    ^\s*
    (?P<company>[A-Za-z0-9_\-\s]+?)\s+
    (?P<trunk>[A-Za-z][\w\-]*)
    (?:\s+trunk\b)?\s+prefix\b\s*(?P<prefix>none|\d+)\s+
    (?P<currency>[A-Za-z]{3})\s*$
""", re.IGNORECASE | re.VERBOSE)

_BRACKETED_LIKE = re.compile(r"""
    ^\s*
    (?P<company>[A-Za-z0-9_\-\s]+?)\s+
    (?P<trunk>\S+)\s+
    (?P<prefix>none|\d+)\s+
    (?P<currency>[A-Za-z]{3})\s*$
""", re.IGNORECASE | re.VERBOSE)

_NO_LABELS = re.compile(r"""
    ^\s*
    (?P<company>.+?)\s+
    (?P<trunk>[A-Za-z][\w\-]*)\s+
    (?P<prefix>\d+|none)\s+
    (?P<currency>[A-Za-z]{3})\s*$
""", re.IGNORECASE | re.VERBOSE)

_ANYORDER = re.compile(r"^(?=.*\bprefix\b)(?=.*\b[A-Za-z]{3}\b).*$", re.IGNORECASE)

def _first_3letter_currency(tokens):
    for t in reversed(tokens):
        if re.fullmatch(r"[A-Za-z]{3}", t):
            return t.upper()
    return None

def _normalize_output(company: str, trunk: str, prefix, currency: str) -> Optional[Dict[str, object]]:
    company = (company or "").strip(" ._-")
    trunk = (trunk or "").strip()
    currency = (currency or "").strip().upper()
    if not company or not trunk or not re.fullmatch(r"[A-Z]{3}", currency):
        return None
    if isinstance(prefix, str):
        if prefix.lower() == "none":
            prefix = None
        elif prefix.isdigit():
            prefix = int(prefix)
        else:
            return None
    elif prefix is not None and not isinstance(prefix, int):
        return None
    if not re.fullmatch(r"[A-Za-z][\w\-]*", trunk):
        return None
    return {"company": company, "trunk": trunk, "prefix": prefix, "currency": currency}

def _extract_anyorder(subject: str) -> Optional[Dict[str, object]]:
    tokens = subject.split()
    currency = _first_3letter_currency(tokens)
    if not currency:
        return None
    prefix = None
    for i, t in enumerate(tokens):
        if t.lower() == "prefix":
            cand = tokens[i+1] if i + 1 < len(tokens) else ""
            val = cand.lower()
            if val == "none":
                prefix = None
                break
            if re.fullmatch(r"\d+", val):
                prefix = int(val)
                break
    if prefix is None:
        try:
            cur_idx = len(tokens) - 1 - list(reversed(tokens)).index(currency)
            if cur_idx - 1 >= 0 and re.fullmatch(r"\d+", tokens[cur_idx - 1]):
                prefix = int(tokens[cur_idx - 1])
        except ValueError:
            pass
    trunk = None
    for i, t in enumerate(tokens):
        if t.lower() == "trunk":
            if i - 1 >= 0 and re.fullmatch(r"[A-Za-z][\w\-]*", tokens[i-1]):
                trunk = tokens[i-1]
                break
            if i + 1 < len(tokens) and re.fullmatch(r"[A-Za-z][\w\-]*", tokens[i+1]):
                trunk = tokens[i+1]
                break
    if trunk is None:
        for t in tokens:
            if t.lower() in {"prefix", "none", currency.lower()}:
                continue
            if re.fullmatch(r"[A-Za-z][\w\-]*", t):
                trunk = t
                break
    company = None
    if trunk:
        try:
            trunk_idx = tokens.index(trunk)
            head = [w for w in tokens[:trunk_idx] if w.lower() not in {"trunk", "prefix"}]
            company = " ".join(head).strip()
        except ValueError:
            pass
    if not company:
        drop = {currency.lower(), "trunk", "prefix", str(prefix) if prefix is not None else ""}
        rest = [w for w in tokens if w.lower() not in drop and not re.fullmatch(r"\d+", w)]
        company = " ".join(rest).strip()
    return _normalize_output(company, trunk, prefix, currency)

def validate_subject(subject: Optional[str]) -> Optional[Dict[str, object]]:
    if not subject:
        return None
    s = _normalize_subject(subject)
    if not s:
        return None
    for rx in (_FREEFORM, _BRACKETED_LIKE, _NO_LABELS):
        m = rx.match(s)
        if m:
            gd = m.groupdict()
            return _normalize_output(gd.get("company",""), gd.get("trunk",""), gd.get("prefix",""), gd.get("currency",""))
    if _ANYORDER.match(s):
        parsed = _extract_anyorder(s)
        if parsed:
            return parsed
    parts = s.split()
    if len(parts) >= 4:
        maybe_currency, maybe_prefix, trunk = parts[-1], parts[-2], parts[-3]
        if re.fullmatch(r"[A-Za-z]{3}", maybe_currency) and re.fullmatch(r"(?:\d+|none)", maybe_prefix, flags=re.I):
            company = " ".join(parts[:-3]).strip()
            return _normalize_output(company, trunk, maybe_prefix, maybe_currency)
    return None

def subject_ok(s: Optional[str]) -> bool:
    return validate_subject(s) is not None

# ------------------------- Utilities -------------------------

def load_env():
    env_path = Path(__file__).with_name(".env")
    load_dotenv(dotenv_path=env_path, override=True)
    missing = [k for k in ["TENANT_ID","CLIENT_ID","CLIENT_SECRET","USER_EMAIL"] if not os.getenv(k)]
    if missing:
        raise SystemExit(f"Missing env vars: {missing}. Check {env_path}")
    return {
        "TENANT_ID": os.getenv("TENANT_ID"),
        "CLIENT_ID": os.getenv("CLIENT_ID"),
        "CLIENT_SECRET": os.getenv("CLIENT_SECRET"),
        "USER_EMAIL": os.getenv("USER_EMAIL"),
        "VERBOSE": os.getenv("VERBOSE","0") == "1",
    }

def b64url_json(seg: str):
    seg = seg + "=" * (-len(seg) % 4)
    return json.loads(base64.urlsafe_b64decode(seg.encode()))

def get_token(tenant_id: str, client_id: str, client_secret: str, verbose=False) -> str:
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    dbg("Auth authority:", authority)
    app = ConfidentialClientApplication(client_id, authority=authority, client_credential=client_secret)
    tok = app.acquire_token_for_client(scopes=SCOPES)
    if "access_token" not in tok:
        print("Token error:", json.dumps(tok, indent=2), file=sys.stderr)
        sys.exit(1)
    if verbose or DEBUG:
        parts = tok["access_token"].split(".")
        if len(parts) > 1:
            try:
                payload = b64url_json(parts[1])
                dbg("Token roles:", payload.get("roles"))
                dbg("Token exp (unix):", payload.get("exp"))
            except Exception as e:
                dbg("Token payload decode failed:", repr(e))
    return tok["access_token"]

def get_session(access_token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json"
    })
    dbg("Session default headers set.")
    return s

def get_with_retries(session: requests.Session, url: str, timeout=30, max_retries=5) -> requests.Response:
    last = None
    for attempt in range(max_retries):
        dbg(f"HTTP GET attempt {attempt+1}/{max_retries}:", url)
        r = session.get(url, timeout=timeout)
        dbg("HTTP status:", r.status_code)
        if r.status_code in (429, 503, 504):
            wait = int(r.headers.get("Retry-After", "0")) or (2 ** attempt)
            dbg(f"Throttled / transient error. Waiting {wait}s then retrying...")
            time.sleep(min(wait, 30))
            last = r
            continue
        return r
    dbg("Max retries reached. Returning last response.")
    return last if last is not None else r

def iso_range(after: Optional[str], before: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Convert YYYY-MM-DD strings into Graph ISO instants:
      after => start of day Z
      before => end of day Z
    """
    def start_of_day(s: str) -> str:
        dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    def end_of_day(s: str) -> str:
        dt = datetime.strptime(s, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
        return dt.isoformat().replace("+00:00", "Z")
    a = start_of_day(after) if after else None
    b = end_of_day(before) if before else None
    dbg("ISO range:", a, "→", b)
    return a, b

def build_messages_url(user_email: str, top: int, after_iso: Optional[str], before_iso: Optional[str], unread_only: bool) -> str:
    user_path = quote(user_email)
    base = f"https://graph.microsoft.com/v1.0/users/{user_path}/mailFolders/Inbox/messages"
    selects = "id,subject,from,receivedDateTime,isRead,hasAttachments"
    order = "receivedDateTime desc"
    filters = []
    if after_iso:
        filters.append(f"receivedDateTime ge {after_iso}")
    if before_iso:
        filters.append(f"receivedDateTime le {before_iso}")
    if unread_only:
        filters.append("isRead eq false")
    filter_q = f"&$filter={' and '.join(filters)}" if filters else ""
    url = f"{base}?$select={selects}&$orderby={order}&$top={top}{filter_q}"
    dbg("Built list URL:", url)
    return url

def safe_filename(name: str) -> str:
    name = os.path.basename(name or "")
    name = unicodedata.normalize('NFKC', name)
    name = re.sub(r'[^A-Za-z0-9._-]+', '_', name).strip('._')
    return name or "attachment"

def unique_path(dirpath: str, filename: str) -> str:
    base, ext = os.path.splitext(filename)
    i = 1
    path = os.path.join(dirpath, filename)
    while os.path.exists(path):
        path = os.path.join(dirpath, f"{base}({i}){ext}")
        i += 1
    return path

def strip_html(html: str) -> str:
    from html import unescape
    html = re.sub(r'(?is)<(script|style).*?>.*?</\1>', '', html)
    html = re.sub(r'(?i)</?(br|p|div|li|tr|h[1-6])[^>]*>', '\n', html)
    text = re.sub(r'(?s)<[^>]+>', '', html)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text).strip()
    return text

# ------------------------- Core Processing -------------------------

def message_sender(msg: dict) -> str:
    frm = (msg.get("from") or {}).get("emailAddress") or {}
    return (frm.get("address") or "").lower().strip()

def save_matching_attachments_for_user(session: requests.Session, user_email: str, msg_id: str,
                                       allowed_exts: Set[str], save_dir: str,
                                       max_bytes: int = 50*1024*1024, min_bytes: int = 1000
                                      ) -> Tuple[bool, int, int, List[str]]:
    """
    Download file attachments whose extension is in allowed_exts.
    Returns (saved_any, considered_count, skipped_count, saved_filenames).

    We first list with a safe $select, then GET each attachment to check @odata.type and contentBytes.
    """
    user_path = quote(user_email)
    list_url = (
        f"https://graph.microsoft.com/v1.0/users/{user_path}/messages/{quote(msg_id)}/attachments"
        "?$select=id,name,contentType,size"
    )

    saved = False
    considered = 0
    skipped = 0
    saved_files: List[str] = []

    if not DRY_RUN:
        os.makedirs(save_dir, exist_ok=True)
    else:
        dbg("DRY_RUN: would create dir", save_dir)

    url = list_url
    while url:
        dbg("Fetching attachments (list):", url)
        r = get_with_retries(session, url)
        if r.status_code != 200:
            try:
                print("Attachment list error:", json.dumps(r.json(), indent=2), file=sys.stderr)
            except Exception:
                print("Attachment list error text:", r.text[:2000], file=sys.stderr)
            r.raise_for_status()

        data = r.json()
        atts = data.get("value", [])
        dbg(f"[DEBUG] Attachments returned: {len(atts)}")

        for att in atts:
            considered += 1
            raw_name = att.get("name") or "attachment"
            filename = safe_filename(raw_name)
            _, ext = os.path.splitext(filename)
            size_meta = att.get("size")

            dbg(f"[DEBUG]   ATTACHMENT(list): name={raw_name} -> safe={filename} ext={ext} size(meta)={size_meta}")

            # Fetch full attachment
            att_id = att.get("id")
            if not att_id:
                dbg("   -> skip: missing attachment id")
                skipped += 1
                continue

            att_url = f"https://graph.microsoft.com/v1.0/users/{user_path}/messages/{quote(msg_id)}/attachments/{quote(att_id)}"
            dbg("   GET attachment detail:", att_url)
            r2 = get_with_retries(session, att_url)
            if r2.status_code != 200:
                try:
                    print("Attachment fetch error:", json.dumps(r2.json(), indent=2), file=sys.stderr)
                except Exception:
                    print("Attachment fetch error text:", r2.text[:2000], file=sys.stderr)
                skipped += 1
                continue

            fatt = r2.json()
            otype = fatt.get("@odata.type", "")
            dbg("   detail @odata.type:", otype)

            if "fileAttachment" not in otype:
                dbg("   -> skip: not a fileAttachment")
                skipped += 1
                continue

            if ext.lower() not in allowed_exts:
                dbg(f"   -> skip: extension {ext.lower()} not in allowed {allowed_exts}")
                skipped += 1
                continue

            b64 = fatt.get("contentBytes")
            if not b64:
                dbg("   -> skip: no contentBytes on fileAttachment")
                skipped += 1
                continue

            try:
                blob = base64.b64decode(b64)
            except Exception as e:
                dbg("   -> skip: base64 decode failed:", repr(e))
                skipped += 1
                continue

            sz = len(blob)
            dbg(f"   contentBytes size={sz}")
            if sz < min_bytes:
                dbg(f"   -> skip: size {sz} < min_bytes {min_bytes}")
                skipped += 1
                continue
            if sz > max_bytes:
                print(f"   ↳ skipped {filename}: {sz} bytes exceeds limit")
                skipped += 1
                continue

            path = unique_path(save_dir, filename)
            if DRY_RUN:
                dbg("   -> DRY_RUN: would save to", path)
            else:
                with open(path, "wb") as f:
                    f.write(blob)
                print(f"   ↳ saved attachment: {path}")
                saved_files.append(os.path.basename(path))
            saved = True

        url = data.get("@odata.nextLink")
        dbg("Next attachments page?", bool(url))

    return saved, considered, skipped, saved_files

def fetch_message_body_text(session: requests.Session, user_email: str, msg_id: str) -> str:
    user_path = quote(user_email)
    url = f"https://graph.microsoft.com/v1.0/users/{user_path}/messages/{quote(msg_id)}?$select=body,bodyPreview"
    dbg("Fetching body:", url)
    r = get_with_retries(session, url)
    if r.status_code != 200:
        try:
            print("Body fetch error:", json.dumps(r.json(), indent=2), file=sys.stderr)
        except Exception:
            print("Body fetch error text:", r.text[:2000], file=sys.stderr)
        r.raise_for_status()
    msg = r.json()
    body = (msg.get("body") or {})
    content_type = (body.get("contentType") or "").lower()
    content = body.get("content") or ""
    dbg("Body contentType:", content_type, "| content length:", len(content))
    if not content:
        preview = (msg.get("bodyPreview") or "")
        dbg("Using bodyPreview length:", len(preview))
        return preview
    if content_type == "html":
        stripped = strip_html(content)
        dbg("HTML stripped length:", len(stripped))
        return stripped
    return content

def write_metadata(save_dir: str, meta: Dict[str, object]) -> None:
    """Write metadata.json in save_dir (idempotent overwrite)."""
    if DRY_RUN:
        dbg("DRY_RUN: would write metadata:", os.path.join(save_dir, "metadata.json"))
        dbg("DRY_RUN meta:", meta)
        return
    try:
        os.makedirs(save_dir, exist_ok=True)
        path = os.path.join(save_dir, "metadata.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
        print(f"  ↳ wrote metadata: {path}")
    except Exception as e:
        print(f"  (warn) failed to write metadata for {save_dir}: {e}", file=sys.stderr)

def process_inbox(session: requests.Session, user_email: str, after: Optional[str], before: Optional[str],
                  page_size: int, allowed_exts: Set[str], attachments_base: str, unread_only: bool):
    after_iso, before_iso = iso_range(after, before)
    url = build_messages_url(user_email, page_size, after_iso, before_iso, unread_only)

    total_fetched = 0
    total_pages = 0
    matched_messages = 0
    saved_messages = 0
    skipped_sender = skipped_subject = skipped_no_attach = skipped_ext = 0
    skipped_read = 0
    skipped_existing_dir = 0  # counter for idempotent skip

    print(f"Query: after={after or '(none)'} | before={before or '(none)'} | page size={page_size} | unread_only={unread_only}")
    print(f"Allowed filetypes: {', '.join(sorted(allowed_exts))}")
    print(f"Verified senders count: {len(VERIFIED)}")
    print("-" * 60)

    while url:
        total_pages += 1
        print(f"\n--- PAGE {total_pages} ------------------------------------------------")
        r = get_with_retries(session, url)
        if r.status_code != 200:
            try:
                print("HTTP error payload:", json.dumps(r.json(), indent=2), file=sys.stderr)
            except Exception:
                print("HTTP error text:", r.text[:2000], file=sys.stderr)
            r.raise_for_status()

        data = r.json()
        items = data.get("value", [])
        page_count = len(items)
        total_fetched += page_count
        print(f"Fetched {page_count} message(s) on this page.")

        if not items:
            print("(No messages returned on this page.)")

        # Process messages newest → oldest (already ordered desc)
        for i, m in enumerate(items, 1):
            msg_id = m.get("id")
            sender = message_sender(m)
            subject = m.get("subject") or ""
            dt_str = m.get("receivedDateTime") or ""
            has_attachments = bool(m.get("hasAttachments"))
            is_unread = not m.get("isRead")

            print(f"\n[{i}/{page_count}] MSG id={msg_id}")
            print(f"  receivedDateTime: {dt_str}")
            print(f"  from: {sender}")
            print(f"  subject: {subject}")
            print(f"  is_unread: {is_unread} | has_attachments: {has_attachments}")

            if unread_only and not is_unread:
                print("  -> skip: message is read but unread_only=True")
                skipped_read += 1
                continue

            if sender not in VERIFIED:
                print("  -> skip: sender NOT in VERIFIED_SENDERS")
                skipped_sender += 1
                continue

            parsed = validate_subject(subject)  # fast regex path
            source = "regex"

            if not parsed:
                source = "openai"
                try:
                    parsed = validate_subject_openai(subject)  # RAW subject, not normalized
                except Exception as e:
                    print(f"  -> skip: LLM fallback crashed: {e}")
                    parsed = None

            if not parsed:
                print(f"  -> skip: subject does not match required fields: {subject!r}")
                skipped_subject += 1
                continue

            meta["subject_parse_source"] = source

            if not has_attachments:
                print("  -> skip: no attachments (hasAttachments=False)")
                skipped_no_attach += 1
                continue

            matched_messages += 1

            # Directory name from sender + UTC timestamp
            try:
                dt = datetime.fromisoformat(dt_str.replace("Z","+00:00"))
            except Exception as e:
                dbg("datetime parse failed:", repr(e), "raw:", dt_str)
                dt = datetime.now(timezone.utc)
            date_time_str = dt.astimezone(timezone.utc).strftime('%Y%m%d_%H%M%S')
            date_only = dt.astimezone(timezone.utc).strftime('%Y-%m-%d')
            time_only = dt.astimezone(timezone.utc).strftime('%H:%M:%S')
            safe_sender = sender.replace('@', '_at_')
            save_dir = os.path.join(attachments_base, f"{safe_sender}_{date_time_str}")
            print("  save_dir:", save_dir)

            # Skip if directory already exists (idempotent)
            if os.path.isdir(save_dir):
                print("  -> skip: directory already exists, assuming this message was processed before")
                skipped_existing_dir += 1
                continue

            # Save attachments
            saved_any, considered_count, skipped_count, saved_files = save_matching_attachments_for_user(
                session, user_email, msg_id, allowed_exts, save_dir
            )
            print(f"  attachments considered: {considered_count} | skipped: {skipped_count} | saved_any={saved_any}")

            if not saved_any:
                print("  -> skip: no attachment passed extension/size checks")
                skipped_ext += 1
                # Cleanup empty directory if created
                try:
                    if os.path.isdir(save_dir) and not os.listdir(save_dir):
                        os.rmdir(save_dir)
                        dbg("  (cleanup) removed empty dir:", save_dir)
                except Exception:
                    pass
                continue

            # Fetch body text (optional for logs)
            body_text = fetch_message_body_text(session, user_email, msg_id)

            # Write metadata.json
            meta = {
                "subject": subject,
                "sender": sender,
                "company": parsed.get("company"),
                "date_utc": date_only,
                "time_utc": time_only,
                "directory": os.path.abspath(save_dir),
                "attachment_count": len(saved_files),
                "attachments": saved_files,
                "receivedDateTime_raw": dt_str,
                "processed_at_utc": datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
            }
            write_metadata(save_dir, meta)

            print("\n✅ VERIFIED EMAIL (with allowed attachment)")
            print(f"From: {sender}")
            print(f"Subject: {subject}")
            print(f"Received: {dt_str}")
            print(f"Body (first 300 chars):\n{(body_text or '')[:300]}")
            print("-" * 60)
            saved_messages += 1

        url = data.get("@odata.nextLink")
        dbg("Next page link present?" , bool(url))

    print("\n======================== SUMMARY ========================")
    print(f"Pages fetched:          {total_pages}")
    print(f"Messages fetched total: {total_fetched}")
    print(f"Matched messages:       {matched_messages}")
    print(f"Saved messages:         {saved_messages}")
    print(f"Skipped (sender):       {skipped_sender}")
    print(f"Skipped (subject):      {skipped_subject}")
    print(f"Skipped (no attach):    {skipped_no_attach}")
    print(f"Skipped (ext/size):     {skipped_ext}")
    print(f"Skipped (read):         {skipped_read}")
    print(f"Skipped (existing dir): {skipped_existing_dir}")
    print("========================================================\n")

# ------------------------- Main (hardcoded settings) -------------------------

def verify_fetch_emails(after: str, before: str, unread_only: bool = True) -> None:
    cfg = load_env()
   
    page_size = 50                    # number of messages per API call
    filetypes = ".csv,.xlsx,.pdf"     # allowed file extensions
    attachments_dir = "attachments"   # base directory where attachments are saved
              # only process unread emails

    # Normalize extensions
    allowed_exts = {e.strip().lower() for e in filetypes.split(",") if e.strip()}
    if not allowed_exts:
        allowed_exts = {".csv", ".xlsx"}

    token = get_token(cfg["TENANT_ID"], cfg["CLIENT_ID"], cfg["CLIENT_SECRET"], verbose=cfg["VERBOSE"])
    session = get_session(token)

    print(f"Querying Inbox for: {cfg['USER_EMAIL']}")
    print(f"VERIFIED_SENDERS count: {len(VERIFIED)}")
    if DEBUG:
        print("VERIFIED_SENDERS sample:", list(sorted(VERIFIED))[:10], "..." if len(VERIFIED) > 10 else "")

    process_inbox(
        session=session,
        user_email=cfg["USER_EMAIL"],
        after=after,
        before=before,
        page_size=page_size,
        allowed_exts=allowed_exts,
        attachments_base=attachments_dir,
        unread_only=unread_only
    )

if __name__ == "__main__":
    after = "2025-08-19"              # only include emails on/after this date (YYYY-MM-DD) or None
    before = "2025-08-19"             # only include emails on/before this date (YYYY-MM-DD) or None
    unread_only = False    
    verify_fetch_emails(after, before, unread_only)
