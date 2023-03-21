import os, fnmatch
#import pymysql
#pymysql.install_as_MySQLdb()
#import MySQLdb as db
import pandas as pd
import datetime
from dateutil.parser import parse
import mysql.connector

# loop through columns
# isinstance(var, data_type)
# delete file after inserting to db
# argparse
# pd.to_sql()
# separate file for adding rows
# use ESG command?
# Google API

def create_columns(filename):
    sql_command = 'CREATE TABLE ESG '
    df = pd.read_excel(filename, engine='openpyxl')
    n = len(df.columns)
    sql_command += '(\n'
    string_max_length = 50 # make this more robust
    for i in range(n):
        col_name = df.columns[i]
        cell = df.loc[0, col_name]
        col_name = col_name.replace(' ', '')
        sql_command += '\t' + col_name + ' '

        if isinstance(cell, int):
            sql_command += 'int'
        elif isinstance(cell, float): #decimal
            sql_command += 'float'
        elif isinstance(cell, str):
            if len(cell) > string_max_length:
                string_max_length = len(cell)

            sql_command += f'varchar({string_max_length})'
        elif isinstance(parse(f'{cell}'), datetime.datetime):
            sql_command += 'DateTime'
        if i != n - 1:
            sql_command += ','
        sql_command += '\n'
    sql_command += ');'

    return sql_command, True

root_path = input('Enter a directory to fetch excel sheets: ')
pattern = '*.xlsx'
called = False

for root, dirs, files in os.walk(root_path):
    if(len(files) == 0):
        continue
    for filename in fnmatch.filter(files, pattern):
        path_to_file = os.path.join(root, filename)
        print(path_to_file)
        sql_command, called = create_columns(path_to_file)
        if called:
            break
    if called:
        break

mydb = mysql.connector.connect(host='localhost', user='ignacio', password='123', database='foo')
cursor = mydb.cursor()
cursor.execute(sql_command)
