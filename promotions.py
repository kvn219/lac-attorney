import pandas as pd
import os
from google.cloud import storage
from dotenv import load_dotenv
from data_definitions import ETHNICITY_MAPPING, JOB_TITLE_MAPPING_01, JOB_TITLE_MAPPING_02

import warnings
from pandas.core.common import SettingWithCopyWarning
warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

load_dotenv()


def promotions(data):

    df = data[data['DEPARTMENT_TITLE'] == 'CITY ATTORNEY']

    cols = ['RECORD_NBR', 'JOB_CLASS_PGRADE', 'PAY_YEAR', 'GENDER', 'ETHNICITY', 'JOB_TITLE', 'JOB_STATUS', 'EMPLOYMENT_TYPE']

    df2 = df.groupby(cols)["REGULAR_PAY"].sum().reset_index()
    df2['JOB_CLASS_PGRADE_NUMERIC'] = df2['JOB_CLASS_PGRADE'].rank(method='dense', ascending=True).astype(int)
    df2['JOB_CLASS_PGRADE_RANK'] = df2.groupby('RECORD_NBR')['JOB_CLASS_PGRADE_NUMERIC'].rank('dense').astype(int)
    df2['NEW_HIRE'] = df2.groupby('RECORD_NBR')['PAY_YEAR'].rank('dense').astype(int)
    df2['PROMOTION'] = df2.groupby(['RECORD_NBR', 'JOB_CLASS_PGRADE_RANK'])['JOB_CLASS_PGRADE_RANK'].rank('first').astype(int)
    df2['ETHNICITY'] = df2['ETHNICITY'].str.strip().replace(ETHNICITY_MAPPING)

    promotions = df2[(df2['PAY_YEAR'] != 2013) & (df2['PROMOTION'] == 1) & (df2['NEW_HIRE'] != 1)]

    promotions['GROUPING_01'] = promotions['JOB_TITLE'].str.strip().replace(JOB_TITLE_MAPPING_01)
    promotions['GROUPING_02'] = promotions['JOB_TITLE'].str.strip().replace(JOB_TITLE_MAPPING_02)

    promotions['GROUPING_01'] = promotions['GROUPING_01'].str.strip()
    promotions['GROUPING_02'] = promotions['GROUPING_02'].str.strip()

    promotions['PAY_YEAR'] = promotions['PAY_YEAR'].apply(lambda x: f'{x}0101')

    bucket_name = os.getenv('GA_BUCKET')
    fname = 'promotions.csv'
    ga_bucket_path = f'data/{fname}'

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = storage.Blob(ga_bucket_path, bucket)
    df_str = promotions.to_csv(index=False, encoding='utf-8')
    blob.upload_from_string(df_str)

    print("uploaded promotions.csv")
