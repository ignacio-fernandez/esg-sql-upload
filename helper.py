import pandas as pd
import json
import re
import sqlalchemy

path = 'D:/foo/ESG Ratings Timeseries Expanded 2007 to 2012/ESG Ratings Timeseries Expanded 2007 to 2012.xlsx'

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

def create_dataframe(path_to_file):
    print(path_to_file)
    df = pd.read_excel(path_to_file, engine='openpyxl')
    df = df.rename(columns=lambda x: x.strip().replace(' ', ''))
    return df

df = create_dataframe(path)
data_types = get_data_types(df)
print(data_types)
#engine = sqlalchemy.create_engine("mysql+pymysql://" + 'root' + ":" + 'Elderscro11s!' + "@" + 'localhost' + "/" + 'foo')
#rows_affected = df.to_sql('ESG', engine, if_exists='append', index=False, dtype=data_types)
#print(rows_affected)

