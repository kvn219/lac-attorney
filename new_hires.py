import pandas as pd
import os
from google.cloud import storage
from dotenv import load_dotenv
from data_definitions import ETHNICITY_MAPPING, JOB_TITLE_MAPPING_01, JOB_TITLE_MAPPING_02

load_dotenv()


data = pd.read_csv(
    'https://controllerdata.lacity.org/api/views/g9h8-fvhu/rows.csv', low_memory=False)
df = data[data['DEPARTMENT_TITLE'] == 'CITY ATTORNEY']
cols = ["RECORD_NBR", "JOB_CLASS_PGRADE", "PAY_YEAR", "GENDER",
        "ETHNICITY", "JOB_TITLE", "JOB_STATUS", "EMPLOYMENT_TYPE"]

df2 = df.groupby(cols)["REGULAR_PAY"].sum().reset_index()
df2['JOB_CLASS_PGRADE_NUMERIC'] = df2['JOB_CLASS_PGRADE'].rank(
    method='dense', ascending=True).astype(int)
df2['JOB_CLASS_PGRADE_RANK'] = df2.groupby(
    'RECORD_NBR')['JOB_CLASS_PGRADE_NUMERIC'].rank('dense').astype(int)
df2['NEW_HIRE'] = df2.groupby('RECORD_NBR')[
    'PAY_YEAR'].rank('dense').astype(int)


df2['ETHNICITY'] = df2['ETHNICITY'].str.strip().replace(ETHNICITY_MAPPING)


df2['GROUPING_01'] = df2['JOB_TITLE'].str.strip().replace(JOB_TITLE_MAPPING_01)
df2['GROUPING_01'] = df2['GROUPING_01'].str.strip()

df2['GROUPING_02'] = df2['JOB_TITLE'].str.strip().replace(JOB_TITLE_MAPPING_02)
df2['GROUPING_02'] = df2['GROUPING_02'].str.strip()


# drop 1st year
new_hires = df2[(df2['PAY_YEAR'] != 2013) & (df2['NEW_HIRE'] == 1)]

# drop duplicates
new_hires = new_hires.drop_duplicates(subset='RECORD_NBR', keep='first')

new_hires['PAY_YEAR'] = new_hires['PAY_YEAR'].apply(lambda x: f"{x}0101")


bucket_name = os.getenv('GA_BUCKET')
fname = "new_hires.csv"

client = storage.Client()
bucket = client.get_bucket(bucket_name)
blob = storage.Blob(f"data/{fname}", bucket)
df_str = new_hires.to_csv(index=False, encoding='utf-8')
blob.upload_from_string(df_str)

print(new_hires)
