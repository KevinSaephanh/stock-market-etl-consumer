from db import get_stocks_collection
from logger import logger


def bulk_insert_stock_data(data):
    try:
        batch = []
        # Add each record to the batch
        for date, num_data in data:
            batch.append({
                "symbol": data["symbol"],
                "date": date,
                "timeframe": data["timeframe"],
                "open": num_data["open"],
                "high": num_data["high"],
                "low": num_data["low"],
                "close": num_data["close"],
                "adjusted_close": num_data["adjusted_close"],
                "volume": num_data["volume"],
                "dividend": num_data["dividend"],
            })
        # Save batch data
        stocks_collection = get_stocks_collection()
        stocks_collection.insert_many(batch)
        logger.info("Successfully inserted all stock data")
        return batch
    except Exception as e:
        logger.error("Error occurred with: %s", e)
        raise e
