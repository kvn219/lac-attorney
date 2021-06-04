import pandas as pd
from promotions import promotions
from salaries import salaries
from new_hires import new_hires

url = 'https://controllerdata.lacity.org/api/views/g9h8-fvhu/rows.csv'
data = pd.read_csv(url, low_memory=False)
new_hires(data)
promotions(data)
salaries(data)
