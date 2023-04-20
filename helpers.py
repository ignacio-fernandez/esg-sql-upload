import numpy as np
from dateutil.parser import parse


def cleanup_cell(c):
    invalid_date = 19000100

    if c == invalid_date:
        return np.nan

    if c == '' or c is np.nan:
        return np.nan

    d = parse(c)
    return d.strftime('%Y-%m-%d')


def cleanup_dates(df, date_cols):
    for c in date_cols:
        df[c] = df[c].apply(cleanup_cell)

    return df
