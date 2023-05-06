import numpy as np
import re
from dateutil.parser import parse


def cleanup_cell(c):
    try:
        invalid_date = 19000100

        if c == invalid_date:
            return np.nan

        if c == '' or c is np.nan:
            return np.nan

        d = parse(c)
        return d.strftime('%Y-%m-%d')
    except Exception:
        return np.nan


def get_date_cols(cols):
    return list(filter(lambda x: re.search('(?i)date', x), cols))


def cleanup_dates(df):
    date_cols = get_date_cols(df.columns)
    for c in date_cols:
        df[c] = df[c].apply(cleanup_cell)

    return df


def get_non_intersect(a, b):
    return list(set(a) ^ set(b))


def add_col_query(table, data_types):
    query = f'ALTER TABLE {table} '
    end = len(data_types) - 1
    for idx, k in enumerate(data_types):
        query += f'ADD {k} '
        if isinstance(data_types[k], list):
            query += f'{data_types[k][0]}({data_types[k][1]})'
        else:
            query += f'{data_types[k]}'
        if idx < end:
            query += ', '
    query += ';'
    return query
