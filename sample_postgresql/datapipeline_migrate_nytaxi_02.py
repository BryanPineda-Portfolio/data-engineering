import pandas as pd

from time import time
from sqlalchemy import create_engine
from logger import get_logger, configure_logger

# types
from typing import Type
from pandas.core.frame import DataFrame 
from pandas.io.parsers.readers import TextFileReader
from sqlalchemy.engine.base import Engine

logger = get_logger(__name__)
configure_logger(logger)

def main() -> None:

    # read csv to dataframe
    csv_full: str = "yellow_tripdata_2021-01.csv"

    conn_nytaxi: str = "postgresql://root:root12345@localhost:5432/ny_taxi"
    engine: Engine = create_engine(conn_nytaxi)

    # pickup datetimes are currently in TEXT, these needs to be converted accordingly
    # df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    # df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    # capture schema of updated df (schema will be used for postgres query generation)
    # df_schema: str = pd.io.sql.get_schema(df, "yellow_tripdata_schema", con=engine)

    # implement dataframe iterator s=to split large datasets to chunks
    logger.info(f"Reading csv: {csv_full}")
    chunk: int = 100000
    df_iter: TextFileReader = pd.read_csv(csv_full, iterator=True, chunksize=chunk)

    # flag for iter 1
    is_first_iter: bool = True
    tblname: str = "yellow_taxi_data"

    logger.info("Processing data chunks...")

    for df_chunk in df_iter:

        t_start: float = time()
        
        # pickup datetimes are currently in TEXT, these needs to be converted accordingly
        df_chunk["tpep_pickup_datetime"] = pd.to_datetime(df_chunk["tpep_pickup_datetime"])
        df_chunk["tpep_dropoff_datetime"] = pd.to_datetime(df_chunk["tpep_dropoff_datetime"])

        # create table schema in postgres (table without data)
        if is_first_iter:
            df_chunk.head(0).to_sql(name=tblname, con=engine, if_exists="replace")
        
        df_chunk.to_sql(name=tblname, con=engine, if_exists="append")

        t_end: float = time()
        logger.info(f"{chunk}-sized chunk inserted...duration: {t_end - t_start:.4f} seconds")

        is_first_iter = False

    logger.info(f"Pipeline completed...")


def debug_types():

    df = pd.read_csv("yellow_tripdata_2021-01.csv", nrows=100)
    df_iter = pd.read_csv("yellow_tripdata_2021-01.csv", iterator=True, chunksize=100000)
    t = time()

    print(type(df))
    print(type(df_iter))
    print(type(t))


if __name__ == "__main__":

    # debug_types()
    main()