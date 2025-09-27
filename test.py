


from openpyxl import load_workbook
import pandas as pd

path = r'C:\Users\User\OneDrive - Hayo Telecom, Inc\Documents\Work\Rate Sheet Automation\rate-sheet-automation\attachments_new\rate_at_qoolize.com_20250902_123633\Hayo_-_Premium_-_In_-Tech_Prefix__7013_-_02_Sep_2025.Xlsx'


wb = load_workbook(path, data_only=True, read_only=True)
ws = wb.active
ws.calculate_dimension()          # force a fresh scan of used range

rows = list(ws.iter_rows(values_only=True)) # iterate actual cells
df = pd.DataFrame(rows)

df = pd.read_excel(path)

# trim empty padding
df.dropna(how="all", inplace=True)
df.dropna(axis=1, how="all", inplace=True)
df.reset_index(drop=True, inplace=True)

# df = pd.read_excel(path, sheet_name=0)
print(df.shape)
print(df.head(20))


