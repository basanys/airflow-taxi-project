import pandas as pd
from sqlalchemy import create_engine
from time import time


def ingest_callable(user, password, host, port, db, table_name, parquet_file, execution_date):


    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()

    t1 = time()
    df = pd.read_parquet(parquet_file)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')
    t2 = time()

    t = t2 - t1
    print(f"inserted to {table_name} table in {t} seconds")
