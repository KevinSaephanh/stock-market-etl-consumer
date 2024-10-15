import signal
import sys
from logger import logger
from db import create_stocks_table
from consumer import consumer, consume_stock_data


def signal_handler(sig, _):
    logger.info("Signal %s received, shutting down...", sig)
    try:
        consumer.close()
        logger.info("Consumer closed successfully")
    except Exception as e:
        logger.error("Failed to close consumer: %s", e)
    finally:
        sys.exit(0)


def main():
    logger.info("Starting app")

    # Set up signal handling for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    create_stocks_table()
    consume_stock_data()


if __name__ == "__main__":
    main()
