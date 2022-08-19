from mosprivoz import *
import sqlalchemy
import pandas as pd

DB_URI = "postgres://tjwycdcqmhotok:e957b5593222ea8af40fe594d639775daec4ed46c51e34819abc6c4a74debfa9@ec2-54-229-217-195.eu-west-1.compute.amazonaws.com:5432/d864flhgj9d9at"

import os
import psycopg2


host = "ec2-54-229-217-195.eu-west-1.compute.amazonaws.com"
database="d864flhgj9d9at"
user = "tjwycdcqmhotok"
port = "5432"
password = "e957b5593222ea8af40fe594d639775daec4ed46c51e34819abc6c4a74debfa9"
engine = sqlalchemy.create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/{database}")
engine.connect()

if __name__ == '__main__':
    parsing_moment = datetime.now()  # Записываем момент времени обработки
    parsing_day = date.today()
    items = parsing_data()
    df = pd.DataFrame.from_dict(items, orient='index')  # перевели в DataFrame
    df.columns = ['links', 'name', 'old_price', 'new_price', 'link']
    df = df.reset_index()
    df['parsing_moment'] = parsing_moment
    df['parsing_day'] = parsing_day
    df.to_sql(
        name='mos_privoz_operational_metrics',
        schema='public',
        con=engine,
        index=False,
        if_exists='replace')

