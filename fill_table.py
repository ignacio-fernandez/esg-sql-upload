import os
import fnmatch
import sys
import zipfile
import sqlalchemy
from helpers import get_non_intersect, add_col_query
from table_functions import *


if len(sys.argv) != 2 and len(sys.argv) != 5:
    print('Incorrect number of parameters', len(sys.argv), '\nType python ./fill_table.py help for more information')
    sys.exit(1)

if sys.argv[1] == 'help':
    print('python fill_table.py <zip file> <database name> <table name> <is csv (1 if true else 0)>')
    sys.exit(2)


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
