import pandas as pd
import os
from google.cloud import storage
from dotenv import load_dotenv
from data_definitions import ETHNICITY_MAPPING, JOB_TITLE_MAPPING_01, JOB_TITLE_MAPPING_02

import warnings
from pandas.core.common import SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

load_dotenv()


def salaries(data):

    df = data[data['DEPARTMENT_TITLE'] == 'CITY ATTORNEY']

    df['GROUPING'] = df['JOB_TITLE'].str.strip().replace(JOB_TITLE_MAPPING_01)

    cols = ['RECORD_NBR', 'PAY_YEAR', 'GENDER', 'ETHNICITY', 'GROUPING', 'EMPLOYMENT_TYPE']
    df2 = df.groupby(cols)["TOTAL_PAY"].sum().reset_index()

    cols2 = ['RECORD_NBR', 'PAY_YEAR', 'GENDER', 'ETHNICITY', 'JOB_TITLE', 'GROUPING', 'EMPLOYMENT_TYPE']
    df3 = df[cols2].fillna('')

    df3['JOB_TITLE'] = df3.groupby(cols)['JOB_TITLE'].transform(lambda x: ';'.join(x))

    salaries = pd.merge(df2, df3, on=cols)
    salaries = salaries.drop_duplicates()

    df['JOB_TITLE_II'] = df['JOB_TITLE'].str.strip().replace(JOB_TITLE_MAPPING_02)

    cols3 = ['RECORD_NBR', 'PAY_YEAR', 'GENDER', 'ETHNICITY', 'JOB_TITLE_II']
    df4 = df[cols3]
    df4 = df4.drop_duplicates()

    salaries = pd.merge(salaries, df4, on=['RECORD_NBR', 'PAY_YEAR', 'GENDER', 'ETHNICITY'])
    salaries = salaries.drop_duplicates()

    salaries['ETHNICITY'] = salaries['ETHNICITY'].str.strip().replace(ETHNICITY_MAPPING)
    salaries['PAY_YEAR'] = salaries['PAY_YEAR'].apply(lambda x: f"{x}0101")

    bucket_name = os.getenv('GA_BUCKET')
    fname = 'salaries.csv'
    ga_bucket_path = f'data/{fname}'

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = storage.Blob(ga_bucket_path, bucket)
    df_str = salaries.to_csv(index=False, encoding='utf-8')
    blob.upload_from_string(df_str)

    print("uploaded salaries.csv")
