from confluent_kafka import Consumer
from config import settings
from logger import logger
from stocks_service import bulk_insert_stock_data
from csv_service import export_to_csv
from s3_service import upload_to_s3

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
            message = consumer.poll(1.0)
            if message is not None and message.error() is None:
                # Insert polled records into DB
                value = message.value().decode("utf-8")
                inserted_data = bulk_insert_stock_data(value)
                # Export inserted data to CSV
                file_name = export_to_csv(inserted_data)
                # Upload CSV to S3
                upload_to_s3(file_name)
    except KeyboardInterrupt:
        logger.error("Keyboard interruption")
    except Exception as e:
        logger.error("Error occurred during data ingestion and transformation: %s", e)
        raise e
    finally:
        consumer.close()
