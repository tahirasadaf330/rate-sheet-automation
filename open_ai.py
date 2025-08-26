
from openai import OpenAI
from dotenv import load_dotenv
import os, json, re, traceback

load_dotenv()
os.environ["OPENAI_API_KEY"] = os.getenv("OPEN_AI_API_KEY")

load_dotenv()
client = OpenAI()  

def validate_subject_openai(subject: str):
    """
    Returns {"company": str, "trunk": str, "prefix": int|None, "currency": str}
    or None if missing/uncertain/invalid.
    """
    if not subject or not subject.strip():
        return None
    
    print('DEBUG: Openai has been invoked to validate subject:', subject)

    prompt = f"""
You extract fields from telecom rate-sheet email subjects.

Required fields (ALL must be present):
- company: supplier company name (strip any surrounding brackets)
- trunk: trunk token in the subject (e.g. STD, PRM, CC, PRIME, SILVER, GOLD, PLATINUM). Do not invent.
- prefix: integer dial prefix; may appear as "prefix:1001", "Prefix 1001", or "#2223". If explicitly "none", use null.
- currency: 3-letter ISO like USD/EUR.

Output rules:
- Company must be the vendor name immediately adjacent to the trunk/prefix/currency group; if multiple names appear, choose the one nearest to that group.
- If ANY field is missing/uncertain, output JSON null (exactly null) for that key inside the json.
- Output ONLY a single JSON object with keys: company, trunk, prefix, currency.

Examples:

Subject: Tick tel PRM trunk Prefix:100 USD
Output: {{"company":"Tick tel","trunk":"PRM","prefix":100,"currency":"USD"}}

Subject: [Vasudev Global Pte Ltd.] [Prime] [#11] [USD]
Output: {{"company":"Vasudev Global Pte Ltd.","trunk":"Prime","prefix":11,"currency":"USD"}}

Subject: HAYO FULL A-Z REPLACE ORIG PLATINUM SIPSTATUS COMMUNICATIONS SRL PREFIX:10587 USD
Output: {{"company":"SIPSTATUS COMMUNICATIONS SRL","trunk":"PLATINUM","prefix":10587,"currency":"USD"}}

Subject: HAYO FULL A-Z REPLACE ORIG PLATINUM SIPSTATUS COMMUNICATIONS SRL PREFIX:10587
Output: null

Now extract for this subject:

Subject: {subject}
Output:
""".strip()

    # 1) Call the API
    try:
        resp = client.responses.create(
            model="gpt-4o-mini",
            input=prompt,
            temperature=0,
            max_output_tokens=200
        )
    except Exception as e:
        print("[openai] API call failed:", e)
        return None

    # 2) Parse JSON
    raw = (resp.output_text or "").strip()
    try:
        data = json.loads(raw)
    except Exception as e:
        print("[openai] Bad JSON from model:", repr(raw), "| error:", e)
        return None

    # 3) Model chose null
    if data is None:
        return None
    if not isinstance(data, dict):
        print("[openai] Not a dict:", data)
        return None

    # 4) Normalize & validate locally (defensive)
    company = data.get("company")
    trunk   = data.get("trunk")
    prefix  = data.get("prefix")
    currency= data.get("currency")

    if not isinstance(company, str) or not company.strip():
        return None
    company = company.strip().strip("[]").strip()

    if not isinstance(trunk, str) or not re.fullmatch(r"[A-Za-z][\w-]*", trunk.strip()):
        return None
    trunk = trunk.strip()

    if isinstance(prefix, str):
        s = prefix.strip()
        if s.lower() == "none":
            prefix = None
        else:
            m = re.search(r"\d+", s)
            if not m:
                return None
            prefix = int(m.group(0))
    elif prefix is not None and not isinstance(prefix, int):
        return None

    if not isinstance(currency, str) or not re.fullmatch(r"[A-Za-z]{3}", currency.strip()):
        return None
    currency = currency.upper()

    return {"company": company, "trunk": trunk, "prefix": prefix, "currency": currency}
