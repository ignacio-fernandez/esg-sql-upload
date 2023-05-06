import pandas as pd
import numpy as np
from helpers import cleanup_dates, get_date_cols, get_non_intersect


def setup():
    df = pd.DataFrame({'IVA_RATING_DATE': [19000100, '', '2000-01-01'],
                       'frac_date_col': ['12/1/2000', np.nan, np.nan],
                      'id_col': [1, 1, 1]
                       })
    return df


# cleanup_dates tests
assert np.isnan(cleanup_dates(setup()).iloc[0, 0])
assert np.isnan(cleanup_dates(setup()).iloc[1, 0])
assert cleanup_dates(setup()).iloc[2, 0] == '2000-01-01', cleanup_dates(setup()).iloc[2, 0]
test_date = cleanup_dates(setup()).iloc[0, 1]
assert test_date == '2000-12-01', test_date


# get_date_cols tests
assert len(get_date_cols([])) == 0
assert len(get_date_cols(setup().columns)) == 2
assert get_date_cols(setup().columns)[0] == 'IVA_RATING_DATE'
assert get_date_cols(setup().columns)[1] == 'frac_date_col'

# get_non_intersect tests
assert len(get_non_intersect([], [])) == 0
assert len(get_non_intersect([1], [])) == 1
assert get_non_intersect([1], [])[0] == 1
assert len(get_non_intersect([], [1])) == 1
assert get_non_intersect([], [1])[0] == 1
assert len(get_non_intersect([1], [1])) == 0
assert len(get_non_intersect([1], [1, 2])) == 1
assert get_non_intersect([1], [1, 2])[0] == 2

