def wait_for_kafka_topic(bootstrap_servers: str, topic: str, attempts: int = 12, sleep_seconds: int = 5) -> bool:
    """Wait for Kafka broker TCP to be reachable. Assumes topic already created.
    Avoids Spark-based checks that can NPE if connectors mismatch.
    """
    import socket
    import logging
    import time
    log = logging.getLogger(__name__)
    host, port = None, None
    try:
        # Parse bootstrap_servers like 'kafka:29092' or 'host1:9092,host2:9092'
        first = bootstrap_servers.split(',')[0].strip()
        parts = first.rsplit(':', 1)
        host = parts[0]
        port = int(parts[1]) if len(parts) > 1 else 9092
    except Exception:
        host, port = bootstrap_servers, 9092

    for i in range(attempts):
        try:
            with socket.create_connection((host, port), timeout=3):
                log.info(f"Kafka reachable at {host}:{port}; assuming topic {topic} exists")
                return True
        except Exception as e:
            log.warning(f"Waiting for Kafka {host}:{port} (topic {topic}): attempt {i+1}/{attempts}: {e}")
            time.sleep(sleep_seconds)
    log.error(f"Kafka not reachable after {attempts*sleep_seconds}s at {host}:{port}")
    return False

import os
import sys
import logging
import traceback
import tempfile
import time
import shutil
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count, sum as _sum, avg, max as _max,
    current_timestamp, to_timestamp, hour, when, approx_count_distinct, lit, coalesce, regexp_replace
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, TimestampType
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_spark_session():
    try:
        default_checkpoint_base = (
            os.getenv("CHECKPOINT_DIR")
            or os.getenv("SPARK_CHECKPOINT_DIR")
            or os.path.join(tempfile.gettempdir(), "spark-checkpoints")
        )
        default_checkpoint = os.path.join(default_checkpoint_base, "default")
        spark = SparkSession.builder \
            .appName("PurchaseTrendAnalysis") \
            .config("spark.sql.streaming.checkpointLocation", default_checkpoint) \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error("Error creating Spark session: %s", str(e))
        logger.error(traceback.format_exc())
        sys.exit(1)


def purge_checkpoint_dir(path: str):
    try:
        checkpoint_path = Path(path)
        if checkpoint_path.exists():
            shutil.rmtree(checkpoint_path)
            logger.info(f"Removed stale checkpoint directory: {checkpoint_path}")
    except Exception as exc:
        logger.warning(f"Failed to purge checkpoint directory {path}: {exc}")


def define_transaction_schema():
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("order_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("department_id", IntegerType(), True),
        StructField("aisle", StringType(), True),
        StructField("aisle_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("reordered", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("price", FloatType(), True),
        StructField("discount", FloatType(), True)
    ])

def read_kafka_stream(spark, kafka_servers, topic):
    try:
        logger.info(f"Reading from Kafka topic: {topic}")
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "1000") \
            .load()
        logger.info("Successfully connected to Kafka stream")
        return df
    except Exception as e:
        logger.error("Error reading from Kafka: %s", str(e))
        raise

def parse_transactions(kafka_df, schema):
    try:
        parsed_df = kafka_df.select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp").alias("kafka_timestamp")
        ).select("data.*", "kafka_timestamp")

        parsed_df = parsed_df.withColumn(
            "event_timestamp",
            coalesce(
                to_timestamp(regexp_replace(col("timestamp"), 'T', ' '), "yyyy-MM-dd HH:mm:ss"),
                col("kafka_timestamp")
            )
        ).withColumn(
            "total_amount",
            (
                coalesce(col("price"), lit(0.0))
                * coalesce(col("quantity"), lit(0))
                * (1 - coalesce(col("discount"), lit(0.0)))
            )
        ).withColumn("processing_time", current_timestamp())

        logger.info("Transaction parsing configured successfully")
        return parsed_df
    except Exception as e:
        logger.error("Error parsing transactions: %s", str(e))
        logger.error(traceback.format_exc())
        raise

def aggregate_by_product(transactions_df):
    try:
        aggregated = transactions_df \
            .withWatermark("event_timestamp", "5 minutes") \
            .groupBy(
                window("event_timestamp", "1 minute", "30 seconds"),
                "product_id",
                "product_name",
                "department"
            ).agg(
                count("*").alias("transaction_count"),
                _sum("quantity").alias("total_quantity"),
                _sum("total_amount").alias("total_revenue"),
                avg("price").alias("avg_price"),
                _sum(when(col("reordered") == 1, 1).otherwise(0)).alias("reorder_count"),
                approx_count_distinct("user_id").alias("unique_customers"),
                _max("event_timestamp").alias("latest_purchase")
            ).select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "product_id", "product_name", "department",
                "transaction_count", "total_quantity",
                "total_revenue", "avg_price",
                "reorder_count", "unique_customers",
                "latest_purchase", current_timestamp().alias("processing_time")
            )
        logger.info("Product aggregation completed successfully")
        return aggregated
    except Exception as e:
        logger.error("Error in product aggregation: %s", str(e))
        raise

def aggregate_by_department(transactions_df):
    try:
        dept_aggregated = transactions_df \
            .withWatermark("event_timestamp", "5 minutes") \
            .groupBy(window("event_timestamp", "2 minutes"), "department") \
            .agg(
                count("*").alias("transaction_count"),
                _sum("quantity").alias("total_quantity"),
                _sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_order_value"),
                approx_count_distinct("user_id").alias("unique_users"),
                approx_count_distinct("product_id").alias("unique_products")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "department",
                "transaction_count", "total_quantity", "total_revenue",
                "avg_order_value", "unique_users", "unique_products",
                current_timestamp().alias("processing_time")
            )
        logger.info("Department aggregation completed successfully")
        return dept_aggregated
    except Exception as e:
        logger.error("Error in department aggregation: %s", str(e))
        raise

def aggregate_by_hour(transactions_df):
    try:
        hourly_agg = transactions_df \
            .withWatermark("event_timestamp", "5 minutes") \
            .groupBy(window("event_timestamp", "5 minutes"), hour("event_timestamp").alias("hour_of_day")) \
            .agg(
                count("*").alias("transaction_count"),
                _sum("total_amount").alias("total_revenue"),
                avg("total_amount").alias("avg_transaction_value"),
                approx_count_distinct("user_id").alias("active_users"),
                approx_count_distinct("product_id").alias("active_products")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                "hour_of_day", "transaction_count",
                "total_revenue", "avg_transaction_value",
                "active_users", "active_products",
                current_timestamp().alias("processing_time")
            )
        logger.info("Hourly aggregation completed successfully")
        return hourly_agg
    except Exception as e:
        logger.error("Error in hourly aggregation: %s", str(e))
        raise

def write_to_hdfs(df, path, checkpoint_path, output_mode="append"):
    try:
        query = df.writeStream \
            .format("parquet") \
            .option("path", path) \
            .option("checkpointLocation", checkpoint_path) \
            .outputMode(output_mode) \
            .option("compression", "snappy") \
            .trigger(processingTime="30 seconds") \
            .start()
        logger.info(f"Writing stream to HDFS path: {path} [mode={output_mode}]")
        return query
    except Exception as e:
        logger.error("Error writing to HDFS: %s", str(e))
        raise

def write_to_console(df, query_name, output_mode="update"):
    try:
        query = df.writeStream \
            .format("console") \
            .outputMode(output_mode) \
            .option("truncate", "false") \
            .option("numRows", "10") \
            .queryName(query_name) \
            .trigger(processingTime="30 seconds") \
            .start()
        logger.info(f"Console output started for: {query_name}")
        return query
    except Exception as e:
        logger.error("Error writing to console: %s", str(e))
        raise

def main():
    try:
        bootstrap_servers = (os.getenv("KAFKA_BOOTSTRAP_SERVERS") or "kafka:29092").strip()
        topic = (os.getenv("KAFKA_TOPIC") or "purchase-transactions").strip()
        hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://hadoop-namenode:8020")
        checkpoint_dir = os.getenv("CHECKPOINT_DIR") or os.path.join(tempfile.gettempdir(), "spark-checkpoints")

        raw_path = f"{hdfs_namenode}/purchase-analytics/raw"
        product_path = f"{hdfs_namenode}/purchase-analytics/aggregated/by_product"
        dept_path = f"{hdfs_namenode}/purchase-analytics/aggregated/by_department"
        hourly_path = f"{hdfs_namenode}/purchase-analytics/aggregated/by_hour"

        checkpoint_base = (
            os.getenv("CHECKPOINT_DIR")
            or os.getenv("SPARK_CHECKPOINT_DIR")
            or os.path.join(tempfile.gettempdir(), "spark-checkpoints")
        )
        raw_checkpoint = os.path.join(checkpoint_base, "raw")
        product_checkpoint = os.path.join(checkpoint_base, "product")
        dept_checkpoint = os.path.join(checkpoint_base, "department")
        hourly_checkpoint = os.path.join(checkpoint_base, "hourly")

        if (os.getenv("RESET_CHECKPOINTS_ON_START", "true").lower() in {"true", "1", "yes", "y"}) and checkpoint_base:
            for target in {checkpoint_base, raw_checkpoint, product_checkpoint, dept_checkpoint, hourly_checkpoint}:
                purge_checkpoint_dir(target)

        spark = create_spark_session()
        schema = define_transaction_schema()
        wait_for_kafka_topic(bootstrap_servers, topic)
        kafka_df = read_kafka_stream(spark, bootstrap_servers, topic)
        transactions_df = parse_transactions(kafka_df, schema)

        queries = {}
        queries["raw_data"] = write_to_hdfs(transactions_df, raw_path, raw_checkpoint)
        product_agg = aggregate_by_product(transactions_df)
        queries["product_agg"] = write_to_hdfs(product_agg, product_path, product_checkpoint, "append")
        queries["product_console"] = write_to_console(product_agg, "product_console", "update")

        dept_agg = aggregate_by_department(transactions_df)
        queries["dept_agg"] = write_to_hdfs(dept_agg, dept_path, dept_checkpoint, "append")

        hourly_agg = aggregate_by_hour(transactions_df)
        queries["hourly_agg"] = write_to_hdfs(hourly_agg, hourly_path, hourly_checkpoint, "append")

        logger.info(f"All {len(queries)} streaming queries started successfully.")

        # Wait for any query termination (keeps driver alive)
        spark.streams.awaitAnyTermination()

    except Exception as e:
        logger.error("Error in main execution: %s", str(e))
        logger.error(traceback.format_exc())
    finally:
        logger.info("Stopping all streaming queries...")
        for name, query in locals().get('queries', {}).items():
            try:
                query.stop()
                logger.info(f"Stopped query: {name}")
            except Exception as e:
                logger.error(f"Error stopping {name}: %s", str(e))
        logger.info("Streaming application stopped.")

if __name__ == "__main__":
    main()
