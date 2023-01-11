import os
from time import time
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine

def ingest_callable(user,password,host,port,db,table_name,parquet_file_name):
    print(table_name,parquet_file_name)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    
    print('connection established successfully, inserting data...')
    
    parquet_file = pq.ParquetFile(parquet_file_name)

    pd.read_parquet(parquet_file_name, engine='pyarrow').head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    for batch in parquet_file.iter_batches():
        t_start = time()
        
        batch_df = batch.to_pandas()
        
        batch_df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('Inserted new chunk, took %.3f second' % (t_end - t_start))
    
    print('Process completed')

