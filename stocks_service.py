from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from logger import logger
import uuid


def bulk_insert_stock_data(session, data):
    logger.info("Preparing batch statement")
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

    timeframe = data["timeframe"]
    insert_statement = session.prepare(
        f"""
        INSERT INTO stocks_{timeframe.lower()} (id, symbol, date, open, high, low, close, adjusted_close, volume, dividend)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    )

    try:
        # Add each insert to the batch
        for date, num_data in data:
            batch.add(
                insert_statement,
                (
                    uuid.uuid4(),
                    date,
                    num_data["open"],
                    num_data["high"],
                    num_data["low"],
                    num_data["close"],
                    num_data["adjusted_close"],
                    num_data["volume"],
                    num_data["dividend"],
                ),
            )
        # Save batch data
        session.execute(batch)
        logger.info("Successfully inserted stock data")
    except Exception as e:
        logger.error("Error occurred with: %s", e)
        raise e
