


# from openpyxl import load_workbook
# import pandas as pd

# path = r'C:\Users\User\OneDrive - Hayo Telecom, Inc\Documents\Work\Rate Sheet Automation\rate-sheet-automation\test_files\Express_Teleservice_Corp__rates_to_HAYO_TELECOM_ETS__SIP_Pre_____25_08_2025.xlsx'

# wb = load_workbook(path, data_only=True, read_only=False)
# ws = wb.active
# ws.calculate_dimension()          # force a fresh scan of used range

# rows = list(ws.iter_rows(values_only=True)) # iterate actual cells
# df = pd.DataFrame(rows)

# # trim empty padding
# df.dropna(how="all", inplace=True)
# df.dropna(axis=1, how="all", inplace=True)
# df.reset_index(drop=True, inplace=True)

# print(df.shape)
# print(df.head(20))



# # df = pd.read_excel(path, header=None, dtype=str)

# # print(df.head(20))



import os
import sys

# Load .env if it's there
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

def first_set(*names, default=None):
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return default

# Map your Laravel-ish vars to what psycopg2 needs
host = first_set("DB_HOST", default="127.0.0.1")
port = int(first_set("DB_PORT", default="5432"))
dbname = first_set("DB_DATABASE", "DB_NAME")
user = first_set("DB_USERNAME", "DB_USER")
password = first_set("DB_PASSWORD")

missing = [k for k, v in {
    "DB_DATABASE/DB_NAME": dbname,
    "DB_USERNAME/DB_USER": user,
    "DB_PASSWORD": password
}.items() if not v]

if missing:
    print("Missing env vars:", ", ".join(missing))
    sys.exit(1)

print(f"Trying {host}:{port} db={dbname} user={user}")

import psycopg2

try:
    with psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        connect_timeout=5,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT version(), current_database(), current_user;")
            version, current_db, current_user = cur.fetchone()
            print("Connected ✔")
            print("Server version:", version)
            print("current_database:", current_db)
            print("current_user:", current_user)

            cur.execute("SELECT 1;")
            one = cur.fetchone()[0]
            print("Simple query result:", one)
except Exception as e:
    print("Connection failed:", repr(e))
    sys.exit(2)

