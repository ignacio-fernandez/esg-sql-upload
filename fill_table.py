import sys
import json
import os
import fnmatch
import pandas as pd
import numpy as np
import pymysql
import datetime
import sqlalchemy
import re
from dateutil.parser import parse
#from sqlalchemy import create_engine, types

# loop through columns
# isinstance(var, data_type)
# delete file after inserting to db
# argparse
# pd.to_sql()
# separate file for adding rows
# use ESG command?
# Google API

def get_regexes():
    with open('esg_columns_regexes.json') as f:
        data = f.read()

    js = json.loads(data)

    return js

def get_data_types(df):
    data_types = {}
    col_name_regexes = get_regexes()
    for col_name in df.columns:
        re_match = False

        for k in col_name_regexes.keys():
            x = re.search(f'{k}', col_name)

            if x:
                re_match = True
                data = col_name_regexes[k]
                if isinstance(data, list):
                    data_types[col_name] = sqlalchemy.__dict__[data[0]](data[1])
                else:
                    data_types[col_name] = sqlalchemy.__dict__[data]()
                break;

        if not re_match:
            data_types[col_name] = sqlalchemy.types.NVARCHAR(length=255)

    return data_types

def cleanup_dates(df):
    invalid_date = 19000100
    date_col = 'IVA_RATING_DATE'
    df[date_col] = df[date_col].apply(lambda x: np.nan if x == invalid_date else x)

    return df

def create_dataframe(path_to_file):
    print(path_to_file)
    df = pd.read_excel(path_to_file, engine='openpyxl')
    df = df.rename(columns=lambda x: x.strip().replace(' ', ''))
    df = cleanup_dates(df)
    return df

root_path = '../foo'#sys.argv[1]
pattern = '*.xlsx'

engine = sqlalchemy.create_engine("mysql+pymysql://" + 'root' + ":" + '123' + "@" + 'localhost' + "/" + 'foo')

for root, dirs, files in os.walk(root_path):
    for filename in fnmatch.filter(files, pattern):
        path_to_file = os.path.join(root, filename)
        df = create_dataframe(path_to_file)
        data_types = get_data_types(df)
        rows_affected = df.to_sql('ESG', engine, if_exists='append', index=False, dtype=data_types)
        print(rows_affected, 'rows affected')
        #os.remove(path_to_file)
        print('Deleted', path_to_file)
