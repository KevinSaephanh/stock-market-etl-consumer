import csv
from logger import logger


def export_to_csv(data):
    file_name = f"{data["symbol"]}_{data["timeframe"].lower()}"
    time_series = data["time_series"]
    fields = list(time_series[0].keys())
    
    with open(f"{file_name}.csv", "w", newline='') as file:
        writer = csv.writer(file)
        writer.writerow(fields)
    
        # Add all fields from stock data to current row
        for entry in time_series:
            writer.writerow([entry[field] for field in fields])
    logger.info("Exported stock data to CSV")
    return file_name
