import csv
from logger import logger
from time_series_data import TimeSeriesData


def export_to_csv(symbol: str, time_series_data: TimeSeriesData):
    timeframe = time_series_data.get("timeframe").lower()
    time_series = time_series_data.get("time_series")
    file_name = f"{symbol}_{timeframe}.csv"
    
    # Time series not provided
    if not time_series:
        logger.error("No time series data available for symbol: %s", symbol)
        return None
    
    fields = ["date", "open", "high", "low", "close", "adjusted_close", "volume", "dividend"]
    try:
        with open(f"{file_name}.csv", "w", newline='') as file:
            writer = csv.writer(file)
            writer.writerow(fields)

            # Add all fields from stock data to current row
            for date, entry in time_series.items():
                row = [date] + [entry[field] for field in fields[1:]]
                writer.writerow(row)
        logger.info("Exported stock data to CSV")
        return file_name
    except Exception as e:
        logger.error("Unexpected error occurred while exporting to CSV: %s", e)
        return None
