#!/usr/bin/env python3
"""
ETL Job: Process NYC Taxi data into Iceberg table
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, hour, unix_timestamp
import logging
import sys
import os
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_spark_session():
    """Create Spark session with Iceberg catalog configuration."""
    return SparkSession.builder \
        .appName("NYC-Taxi-ETL") \
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.iceberg.warehouse", "/opt/hive/data/warehouse") \
        .config("spark.sql.defaultCatalog", "iceberg") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \
        .config("spark.default.parallelism", "8") \
        .getOrCreate()


def verify_metastore_connection(spark):
    """Verify Hive Metastore connectivity."""
    try:
        spark.sql("SHOW DATABASES").collect()
        return True
    except Exception as e:
        logger.error(f"HMS connection failed: {e}")
        return False


def download_data(url, output_path):
    """Download parquet file from URL."""
    response = requests.get(url, timeout=300)
    with open(output_path, "wb") as f:
        f.write(response.content)
    return output_path


def extract_data(spark, file_path):
    """Read parquet file into DataFrame."""
    return spark.read.parquet(file_path)


def transform_data(df):
    """Clean and transform taxi trip data."""
    return df.filter(
        col("tpep_pickup_datetime").isNotNull() &
        col("tpep_dropoff_datetime").isNotNull() &
        (col("trip_distance") > 0)
    ).select(
        "*",
        ((unix_timestamp(col("tpep_dropoff_datetime")) - 
          unix_timestamp(col("tpep_pickup_datetime"))) / 60).alias("trip_duration_minutes"),
        to_date(col("tpep_pickup_datetime")).alias("pickup_date"),
        hour(col("tpep_pickup_datetime")).alias("pickup_hour")
    ).cache()


def load_data(spark, df, database, table):
    """Write DataFrame to Iceberg table."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"DROP TABLE IF EXISTS iceberg.{database}.{table}")
    
    df.writeTo(f"iceberg.{database}.{table}") \
        .using("iceberg") \
        .partitionedBy("pickup_date") \
        .option("write.parquet.compression-codec", "snappy") \
        .createOrReplace()
    
    df.unpersist()


def verify_table(spark, database, table):
    """Verify table is in Iceberg format."""
    table_info = spark.sql(f"DESCRIBE EXTENDED iceberg.{database}.{table}").collect()
    provider = [row for row in table_info if row[0] == "Provider"]
    
    if not provider or provider[0][1] != "iceberg":
        raise Exception("Table is not in Iceberg format")
    
    result = spark.sql(f"SELECT COUNT(*) as count FROM iceberg.{database}.{table}").collect()
    return result[0]["count"]


def main():
    """Main ETL pipeline execution."""
    spark = None
    
    try:
        spark = create_spark_session()
        
        if not verify_metastore_connection(spark):
            sys.exit(1)
        
        # Extract
        data_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
        file_path = os.path.join(os.getcwd(), "yellow_tripdata_2024-01.parquet")
        download_data(data_url, file_path)
        df = extract_data(spark, file_path)
        
        # Transform
        df_processed = transform_data(df)
        df_processed.count()
        
        # Load
        load_data(spark, df_processed, "nyc_taxi", "yellow_trips")
        
        # Verify
        verify_table(spark, "nyc_taxi", "yellow_trips")
        
        return 0
        
    except Exception as e:
        logger.error(f"ETL failed: {e}", exc_info=True)
        sys.exit(1)
        
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()