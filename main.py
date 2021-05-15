import pandas as pd

from google.cloud import storage


data = pd.read_csv('https://controllerdata.lacity.org/api/views/g9h8-fvhu/rows.csv', low_memory=False)

print(data)