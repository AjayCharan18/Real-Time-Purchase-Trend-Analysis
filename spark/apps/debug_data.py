#!/usr/bin/env python3

from pyspark.sql import SparkSession

def debug_hdfs_data():
    spark = SparkSession.builder \
        .appName("DebugHDFSData") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:8020") \
        .getOrCreate()
    
    try:
        # Check aggregated data
        agg_path = "hdfs://hadoop-namenode:8020/purchase-analytics/aggregated/by_product"
        print(f"Reading aggregated data from: {agg_path}")
        
        df_agg = spark.read.parquet(agg_path)
        print(f"Aggregated data schema:")
        df_agg.printSchema()
        print(f"Aggregated data count: {df_agg.count()}")
        print("Sample aggregated data:")
        df_agg.show(10)
        
        # Check raw data
        raw_path = "hdfs://hadoop-namenode:8020/purchase-analytics/raw"
        print(f"\nReading raw data from: {raw_path}")
        
        df_raw = spark.read.parquet(raw_path)
        print(f"Raw data schema:")
        df_raw.printSchema()
        print(f"Raw data count: {df_raw.count()}")
        print("Sample raw data:")
        df_raw.show(5)
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    debug_hdfs_data()
