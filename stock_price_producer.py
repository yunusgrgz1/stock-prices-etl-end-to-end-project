from pyspark.sql import SparkSession
import json
import logging
import requests
from datetime import datetime
import pandas
from pyspark.sql.functions import *

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("KafkaStockProducer")

producer_params = {
    'kafka.bootstrap.servers': 'broker:29092',
    'topic': 'stock-market-producer'
}

url = "http://flask-api:5000/api/market"

def spark_session():
    spark = SparkSession.builder \
        .appName("KafkaProducerStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    return spark

def fetch_data():
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            logger.error(f"API error {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Request error: {e}")
        return None
    
def spark_dataframe(spark, data):
    try:
        df = spark.createDataFrame([(json.dumps(data),)], ['value'])
        df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", producer_params['kafka.bootstrap.servers']) \
            .option("topic", producer_params['topic']) \
            .save()
        logger.info("Data sent to Kafka.")
        return df
    except Exception as e:
        logger.error(f"Error sending data to Kafka: {e}")

def process_stream(df, epoch_id):
    spark = spark_session()
    data = fetch_data()
    if not data:
        logger.warning("No data to process.")
        return
    logger.info(f"company name {data[0]['name']}")

    spark_dataframe(spark, data)

def main():
    logger.info("Starting Kafka producer streaming...")

    spark = spark_session()

    trigger_df = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 5) \
        .load()

    query = trigger_df.writeStream \
        .foreachBatch(process_stream) \
        .option("checkpointLocation", "/tmp/checkpoint-stock-market-producer") \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()
