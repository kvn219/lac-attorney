import pandas as pd
import os
from google.cloud import storage
from dotenv import load_dotenv
from data_definitions import ETHNICITY_MAPPING, JOB_TITLE_MAPPING_01, JOB_TITLE_MAPPING_02

load_dotenv()

data = pd.read_csv(
    'https://controllerdata.lacity.org/api/views/g9h8-fvhu/rows.csv', low_memory=False)

df = data[data['DEPARTMENT_TITLE'] == 'CITY ATTORNEY']

df['GROUPING'] = df['JOB_TITLE'].str.strip().replace(JOB_TITLE_MAPPING_01)

cols = ["RECORD_NBR", "PAY_YEAR", "GENDER",
        "ETHNICITY", 'GROUPING', 'EMPLOYMENT_TYPE']

df2 = df.groupby(cols)["TOTAL_PAY"].sum().reset_index()

cols2 = ['RECORD_NBR', 'PAY_YEAR', 'GENDER', 'ETHNICITY',
         'JOB_TITLE', 'GROUPING', 'EMPLOYMENT_TYPE']

df3 = df[cols2].fillna('')

df3['JOB_TITLE'] = df3.groupby(['RECORD_NBR', 'PAY_YEAR', 'GENDER', 'ETHNICITY', 'GROUPING', 'EMPLOYMENT_TYPE'])[
    'JOB_TITLE'].transform(lambda x: ';'.join(x))

salaries = pd.merge(df2, df3, on=[
                    'RECORD_NBR', 'PAY_YEAR', 'GENDER', 'ETHNICITY', 'GROUPING', 'EMPLOYMENT_TYPE'])
salaries = salaries.drop_duplicates()


df['JOB_TITLE_II'] = df['JOB_TITLE'].str.strip().replace(JOB_TITLE_MAPPING_02)
cols_foo = ['RECORD_NBR', 'PAY_YEAR', 'GENDER', 'ETHNICITY', 'JOB_TITLE_II']
df4 = df[cols_foo]
df4 = df4.drop_duplicates()

salaries = pd.merge(salaries, df4, on=[
                    'RECORD_NBR', 'PAY_YEAR', 'GENDER', 'ETHNICITY'])
salaries = salaries.drop_duplicates()

salaries['ETHNICITY'] = salaries['ETHNICITY'].str.strip().replace(
    ETHNICITY_MAPPING)

salaries['PAY_YEAR'] = salaries['PAY_YEAR'].apply(lambda x: f"{x}0101")

print(salaries)

bucket_name = os.getenv('GA_BUCKET')
fname = 'salaries.csv'

client = storage.Client()
bucket = client.get_bucket(bucket_name)
blob = storage.Blob(f"data/{fname}", bucket)
df_str = salaries.to_csv(index=False, encoding='utf-8')
blob.upload_from_string(df_str)
