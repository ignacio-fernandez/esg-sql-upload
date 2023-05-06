import json
import pandas as pd
import re
import sqlalchemy
from helpers import cleanup_dates


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
