from pymongo import MongoClient
import psycopg2
import logging
from datetime import datetime, time as dtime

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("KafkaFlightConsumer")

def mongo_connection():
    try:
        logger.info("Connecting to MongoDB..")
        client = MongoClient("mongodb://root:example@mongodb:27017")
        db = client["mydb"]
        collection = db["mycollection"]
        logger.info("Connected to MongoDB successfully.")
        return collection
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

def postgres_connection():
    try:
        logger.info("Connecting to PostgreSQL...")
        conn = psycopg2.connect(
            dbname="airflow",
            user="airflow",
            password="airflow",
            host="postgres",
            port="5432")
        cursor = conn.cursor()
        logger.info("Connected to PostgreSQL successfully.")
        return cursor, conn
    except Exception as e:
        logger.error(f"Failed to connect to PostgreSQL: {e}")
        raise

def get_data_from_mongo(cursor, collection):
    try:
        logger.info("Fetching data from MongoDB collection...")
        documents = collection.find({})
        first5 = collection.find().limit(1)
        for doc in first5:
            logger.info(doc)
        for doc in documents:
            symbol = doc["symbol"]
            timestamp_str = doc.get("timestamp")
            if not timestamp_str:
                logger.warning(f"Missing timestamp in document with symbol {doc.get('symbol')}")
                continue 
            dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            date = dt.date()
            cursor.execute("SELECT id FROM companies WHERE symbol = %s", (symbol,))
            result = cursor.fetchone()
            if not result:
                logger.warning(f"Company symbol {symbol} not found in companies table.")
                continue
            company_id = result[0]

            open_price = doc.get("open", 0.0)
            time_ = dt.time()
            current_price = doc.get("current_price", 0.0)

            if time_ == dtime(18, 0):
                close_price = current_price

                start_dt = datetime.combine(date, dtime(8, 0))
                end_dt = datetime.combine(date, dtime(18, 0, 0))

                high_query = {
                    "symbol": symbol,
                    "timestamp": {
                        "$gte": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                        "$lte": end_dt.strftime("%Y-%m-%d %H:%M:%S")
                    }
                }
                all_prices = list(collection.find(high_query))

                if not all_prices:
                    logger.warning(f"No prices found for symbol {symbol} on {date}")
                    continue

                high_price = max(p.get("current_price", 0.0) for p in all_prices)
                low_price = min(p.get("current_price", 0.0) for p in all_prices)
                total_volume = max(int(p.get("volume", 0)) for p in all_prices)

                cursor.execute("""
                    INSERT INTO prices (company_id, date, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (company_id, date) DO UPDATE
                    SET open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume;
                """, (company_id, date, open_price, high_price, low_price, close_price, total_volume))

    except Exception as e:
        logger.error(f"Error while processing data: {e}")
        raise

def from_mongo_to_postgres():
    collection = mongo_connection()
    cursor, conn = postgres_connection()
    try:
        get_data_from_mongo(cursor, collection)
        conn.commit()
        logger.info("Data inserted/updated successfully.")
    except Exception as e:
        logger.error(f"Failed during data transfer: {e}")
    finally:
        cursor.close()
        conn.close()
        logger.info("PostgreSQL connection closed.")

if __name__ == "__main__":
    from_mongo_to_postgres()
