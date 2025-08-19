
""" 
Step 1: run the email verification script to fetch all the new valid files which will be stored in the attachments folder
Step 2: then run the jerasoft script to fetch all the relevant tables for comparision
Step 3: run the preprocess script on all the files 
Step 4: run the ratesheet comparision script to generate the comparision report
Step 5: run the database script to push the comparision results to the database 

"""

"""
while saving the files from email also create a meta data file for that directory that should include subject, sender, date, time
path to the directory.

then read that meta data file and create all the comparision files using jerasoft and save in the same directory.

then preprocess all the files 

"""

from jerasoft import export_rates_by_query
from ratesheet_comparision_engine import read_table, compare, write_excel


#_____________ Email Verification Script_____________

#currently on hold due to issues with the email integration


#_______________ Jerasoft Script _____________



info = export_rates_by_query(
        target_query="Quickcom tel PRM trunk Prefix:1001 USD",
        output_path="quickcom_rates.xlsx",
)

#_______________ Preprocess Script _____________




# also remove the nan values

#________________ Ratesheet Comparision Script _____________

LEFT_FILE = r"test_files\Second_File_Updated_cleaned.xlsx" #current rate file
RIGHT_FILE = r"test_files\First_File_Updated_cleaned.xlsx" #new rate file
OUT_FILE = "comparison.xlsx"
AS_OF_DATE = "2025-07-08"
NOTICE_DAYS = 7
RATE_TOL = 0.0
SHEET_LEFT = None
SHEET_RIGHT = None

left = read_table(LEFT_FILE, SHEET_LEFT)
right = read_table(RIGHT_FILE, SHEET_RIGHT)
result = compare(left, right, AS_OF_DATE, NOTICE_DAYS, RATE_TOL)
write_excel(result, OUT_FILE)
print(f"Wrote {OUT_FILE} with {len(result)} rows.")


#________________ Database Script _____________

