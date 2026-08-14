import os
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit
from delta.tables import DeltaTable
import re

def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def process_bronze_to_staging(input_dir, output_dir, start_date=None, end_date=None):
    components = ["compressor", "condenser", "evaporator", "expansion_valve"]
    os.makedirs(output_dir, exist_ok=True)

    spark = SparkSession.builder \
        .appName("HVAC_Bronze_to_Staging") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .getOrCreate()
        
    try:
        for component in components:
            bronze_path = os.path.join(input_dir, component)
            staging_path = os.path.join(output_dir, component)
            
            if not os.path.exists(bronze_path) or not DeltaTable.isDeltaTable(spark, bronze_path):
                print(f"Skipping {component} - Bronze data not found.")
                continue
                
            print(f"Processing Bronze -> Staging for {component}...")
            
            df = spark.read.format("delta").load(bronze_path)
            
            if start_date:
                df = df.filter(to_date(col("date")) >= to_date(lit(start_date)))
            if end_date:
                df = df.filter(to_date(col("date")) <= to_date(lit(end_date)))
                
            # Rename columns to snake_case
            for c in df.columns:
                snake_c = camel_to_snake(c)
                if snake_c != c:
                    df = df.withColumnRenamed(c, snake_c)
                    
            df.cache()
            if df.rdd.isEmpty():
                print(f"No records to process for {component}.")
                df.unpersist()
                continue
                
            if DeltaTable.isDeltaTable(spark, staging_path):
                target_table = DeltaTable.forPath(spark, staging_path)
                # Use event_id for deduplication
                target_table.alias("t").merge(
                    df.alias("s"),
                    "t.event_id = s.event_id"
                ).whenNotMatchedInsertAll().execute()
            else:
                df.write.format("delta") \
                    .mode("append") \
                    .partitionBy("date") \
                    .save(staging_path)
                    
            print(f"Successfully processed records to Staging: {component}")
            df.unpersist()
    except Exception as e:
        print(f"Error processing Bronze to Staging: {e}")
        import traceback
        traceback.print_exc()
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--start-date", help="Process from this date (YYYY-MM-DD)", default=None)
    parser.add_argument("--end-date", help="Process until this date (YYYY-MM-DD)", default=None)
    args = parser.parse_args()
    process_bronze_to_staging(args.input_dir, args.output_dir, args.start_date, args.end_date)
