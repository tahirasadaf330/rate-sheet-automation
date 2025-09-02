


from openpyxl import load_workbook
import pandas as pd

path = r'C:\Users\User\OneDrive - Hayo Telecom, Inc\Documents\Work\Rate Sheet Automation\rate-sheet-automation\test_files\Hayo Telecom Inc. Platinum 20.30.Aug.25.2025 174897940.xlsx'

wb = load_workbook(path, data_only=True, read_only=True)
ws = wb.active
ws.calculate_dimension()          # force a fresh scan of used range

rows = list(ws.iter_rows(values_only=True)) # iterate actual cells
df = pd.DataFrame(rows)

# trim empty padding
df.dropna(how="all", inplace=True)
df.dropna(axis=1, how="all", inplace=True)
df.reset_index(drop=True, inplace=True)

print(df.shape)
print(df.head(20))






