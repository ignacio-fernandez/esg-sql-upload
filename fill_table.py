import json
import os
import fnmatch
import sys
import zipfile
import pandas as pd
import sqlalchemy
import re
from helpers import cleanup_dates, get_non_intersect, add_col_query


def get_regexes():
    with open('esg_columns_regexes.json') as f:
        data = f.read()

    js = json.loads(data)

    return js


def get_data_type_strings(columns):
    d = {}
    col_name_regexes = get_regexes()
    for col_name in columns:
        re_match = False

        for k in col_name_regexes:
            x = re.search(f'{k}', col_name)

            if x:
                re_match = True
                data = col_name_regexes[k]
                if isinstance(data, list):
                    d[col_name] = [data[0], data[1]]
                else:
                    d[col_name] = data
                break

        if not re_match:
            d[col_name] = ['NVARCHAR', 255]

    return d


def convert_to_datatypes(data_type_strings):
    d = {}
    for k in data_type_strings:
        if isinstance(data_type_strings[k], list):
            d[k] = sqlalchemy.__dict__[data_type_strings[k][0]](data_type_strings[k][1])
        else:
            d[k] = sqlalchemy.__dict__[data_type_strings[k]]()
    return d


def create_dataframe(path_to_file, is_csv):
    print(path_to_file)
    if is_csv:
        df = pd.read_csv(path_to_file)
    else:
        df = pd.read_excel(path_to_file, engine='openpyxl')
    df = df.rename(columns=lambda x: x.strip().replace(' ', ''))
    df = cleanup_dates(df)
    return df


if len(sys.argv) != 2 and len(sys.argv) != 5:
    print('Incorrect number of parameters', len(sys.argv))
    sys.exit(1)


if sys.argv[1] == 'help':
    print('fill_table.py <zip file> <database name> <table name> <is csv (1 if true else 0)>')
    sys.exit(0)

root_path = sys.argv[1]
database_name = sys.argv[2]
table_name = sys.argv[3]
is_csv = sys.argv[4]
pattern = '*.csv' if is_csv else '*.xlsx'
prev_cols = []

engine = sqlalchemy.create_engine("mysql+pymysql://" + 'root' + ":" + '123' + "@" + 'localhost' + "/" + database_name)

for root, dirs, files in os.walk(root_path):
    for filename in fnmatch.filter(files, pattern):
        path_to_file = os.path.join(root, filename)
        df = create_dataframe(path_to_file, is_csv)

        if len(prev_cols) == 0:
            data_types = get_data_type_strings(df.columns)
            prev_cols = df.columns
        elif len(prev_cols) != len(df.columns):
            new_cols = get_non_intersect(prev_cols, df.columns)
            prev_cols = df.columns
            data_types = get_data_type_strings(new_cols)
            query = add_col_query('ESG', data_types)
            engine.execute(add_col_query('ESG', data_types))

        rows_affected = df.to_sql('ESG', engine, if_exists='append', index=False, dtype=convert_to_datatypes(data_types))
        print(rows_affected, 'rows affected')
        #os.remove(path_to_file)
        print('Deleted', path_to_file)
