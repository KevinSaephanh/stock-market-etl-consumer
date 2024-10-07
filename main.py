import signal
import sys
from cassandra.cluster import Cluster
from logger import logger
from consumer import consumer, consume_stock_data


def cassandra_connection():
    try:
        logger.info("Connecting to DB")
        cluster = Cluster(["127.0.0.1"], port=9042)
        session = cluster.connect()
        logger.info("Connected to DB")
        return session
    except Exception as e:
        logger.error("Failed to connect to DB. Cause: %s", e)
        raise e


def signal_handler(sig, _):
    logger.info("Signal %s received, shutting down...", sig)
    consumer.close()
    sys.exit(0)


if __name__ == "__main__":
    logger.info("Starting app")
    session = cassandra_connection()

    # Set up signal handling for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    consume_stock_data(session)
