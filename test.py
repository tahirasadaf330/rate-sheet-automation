import pandas as pd
import pandas as pd

# Explicitly set the 'Effective Date' column to string type
df = pd.read_excel(r'C:\Users\User\OneDrive - Hayo Telecom, Inc\Documents\Work\Rate Sheet Automation\rate-sheet-automation\attachments\Book1.xlsx', dtype={'Effective Date': str})

print(df.head())
