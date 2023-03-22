import sqlalchemy
import pandas as pd

engine = sqlalchemy.create_engine("mysql+pymysql://" + 'root' + ":" + 'Elderscro11s!' + "@" + 'localhost' + "/" + 'foo')

df = pd.DataFrame({'id': [1, 2, 3], 'nullable': ['blah', None, 'bl']})
data_types = {'id': sqlalchemy.types.INTEGER(), 'nullable': sqlalchemy.types.NVARCHAR(5)}

df.to_sql('test', engine, if_exists='replace', index=False, dtype=data_types)
