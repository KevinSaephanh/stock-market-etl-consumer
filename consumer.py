import json
from typing import Optional
from confluent_kafka import Consumer
from config import settings
from logger import logger
from csv_service import export_to_csv
from s3_service import upload_to_s3
from time_series_data import TimeSeriesData

consumer_config = {
    "bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
    "group.id": settings.KAFKA_CONSUMER_GROUP_ID,
    "auto.offset.reset": "earliest",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": settings.KAFKA_USERNAME,
    "sasl.password": settings.KAFKA_PASSWORD,
    "session.timeout.ms": 45000,
}
consumer = Consumer(consumer_config)
consumer.subscribe([settings.KAFKA_TOPIC])


def consume_stock_data():
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None and msg.error() is None:
                stock_data = json.loads(msg.value().decode('utf-8'))
                symbol = msg.key().decode('utf-8') 
                time_series_data = parse_stock_time_series(stock_data)
                                
                # Export inserted data to CSV
                file_name = export_to_csv(symbol, time_series_data)
                
                # Upload CSV to S3
                upload_to_s3(file_name)
    except json.JSONDecodeError as e:
        logger.error("Failed to decode JSON: %s", {e})
    except KeyboardInterrupt as e:
        logger.error("Keyboard interruption: %s", {e})
    except Exception as e:
        logger.error("Error occurred during data ingestion and transformation: %s", e)
        raise e
    finally:
        consumer.close()


def parse_stock_time_series(stock_data) -> Optional[TimeSeriesData]:
    time_series_mapping = {
        "Time Series (Daily)": "daily",
        "Weekly Adjusted Time Series": "weekly",
        "Monthly Adjusted Time Series": "monthly"
    }
    for key, value in time_series_mapping.items():
        if key in stock_data:
            return TimeSeriesData(
                time_series=stock_data[key],
                timeframe=value
            )
    logger.error("Invalid data format: missing expected time series keys")
    return None