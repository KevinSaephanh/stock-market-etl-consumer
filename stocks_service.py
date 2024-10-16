from db import get_stocks_collection
from logger import logger
from time_series_data import TimeSeriesData


def bulk_insert_stock_data(symbol: str, time_series_data: TimeSeriesData):
    try:
        time_series = time_series_data["time_series"]
        timeframe = time_series_data["timeframe"]
        batch = []
        
        # Add each record to the batch
        for date, metrics in time_series.items():
            batch.append({
                "symbol": symbol,
                "year": int(date[:4]),
                "date": date,
                "timeframe": timeframe,
                "open": float(metrics.get("1. open", 0.0)),
                "high": float(metrics.get("2. high", 0.0)),
                "low": float(metrics.get("3. low", 0.0)),
                "close": float(metrics.get("4. close", 0.0)),
                "adjusted_close": float(metrics.get("5. adjusted close", 0.0)),
                "volume": int(metrics.get("6. volume", 0)),
                "dividend": float(metrics.get("7. dividend amount", 0.0)),
            })
            
        # Save batch data
        stocks_collection = get_stocks_collection()
        stocks_collection.insert_many(batch)
        logger.info("Successfully inserted all stock data")
        return batch
    except Exception as e:
        logger.error("Error occurred with: %s", e)
        raise e
