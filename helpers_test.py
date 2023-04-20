import pandas as pd
import numpy as np
from helpers import cleanup_dates


def setup():
    df = pd.DataFrame({'IVA_RATING_DATE': [19000100, '', '2000-01-01'], 'frac_date_col': ['12/1/2000', np.nan, np.nan]})
    return df


assert np.isnan(cleanup_dates(setup(), ['IVA_RATING_DATE']).iloc[0, 0])
assert np.isnan(cleanup_dates(setup(), ['IVA_RATING_DATE']).iloc[1, 0])
assert cleanup_dates(setup(), ['IVA_RATING_DATE']).iloc[2, 0] == '2000-01-01', cleanup_dates(setup(), ['IVA_RATING_DATE']).iloc[2, 0]
test_date = cleanup_dates(setup(), ['frac_date_col']).iloc[0, 1]
assert test_date == '2000-12-01', test_date
