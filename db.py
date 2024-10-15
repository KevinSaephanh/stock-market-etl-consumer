from astrapy import DataAPIClient
from logger import logger
from config import settings

client = DataAPIClient(settings.DB_TOKEN)
db = client.get_database_by_api_endpoint(settings.DB_URL)


def get_stocks_collection():
    return db.get_collection("stock_data")


def create_stocks_table():
    try:
        db.command("""CREATE TABLE IF NOT EXISTS stocks 
                    (symbol TEXT, 
                     year INT, 
                     date DATE, 
                     timeframe TEXT, 
                     open DECIMAL, 
                     high DECIMAL, 
                     low DECIMAL, 
                     close DECIMAL, 
                     adjusted_close DECIMAL, 
                     volume BIGINT, 
                     dividend DECIMAL, 
                     PRIMARY KEY ((symbol, year), date, timeframe) 
                     ) WITH CLUSTERING ORDER BY (date DESC)""")
        logger.info("Table created successfully")
    except Exception as e:
        logger.error("Error occurred while trying to create table. Caused by: %s", e)
        raise e