


# from openpyxl import load_workbook
# import pandas as pd

# path = r'C:\Users\User\OneDrive - Hayo Telecom, Inc\Documents\Work\Rate Sheet Automation\rate-sheet-automation\attachments to be compared\rates_at_alkaip.com_20250904_151710\R_A_HAYO_TELECOM_INC_090425.xlsx'


# wb = load_workbook(path, data_only=True, read_only=True)
# ws = wb.active
# ws.calculate_dimension()          # force a fresh scan of used range

# rows = list(ws.iter_rows(values_only=True)) # iterate actual cells
# df = pd.DataFrame(rows)

# # trim empty padding
# df.dropna(how="all", inplace=True)
# df.dropna(axis=1, how="all", inplace=True)
# df.reset_index(drop=True, inplace=True)

# # df = pd.read_excel(path, sheet_name=0)
# print(df.shape)
# print(df.head(20))






