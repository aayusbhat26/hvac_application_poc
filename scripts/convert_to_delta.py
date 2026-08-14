import os
import glob
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_unixtime, to_date, lit
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip

def process_files(input_dir, output_dir):
    # Initialize Spark
    builder = SparkSession.builder \
        .appName("HVAC_Raw_to_Bronze") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    try:
        # Read JSON files natively with Spark
        # We read all json files in the input_dir recursively
        print(f"Reading raw JSON files from {input_dir}")
        # Find all location_* directories and pass them to Spark
        location_dirs = glob.glob(os.path.join(input_dir, "location_*"))
        if not location_dirs:
            print(f"No location directories found in {input_dir}")
            return
            
        df_raw = spark.read.option("recursiveFileLookup", "true").json(location_dirs, multiLine=True)
        
        # Explode the records array
        df_exploded = df_raw.select(
            "*", 
            explode("records").alias("record")
        ).drop("records")
        
        # Select base metadata + nested record fields dynamically to prevent data loss
        # The envelope has many fields (e.g. componentManufacturer), we keep everything from envelope and record
        df_flat = df_exploded.select("record.*", "*").drop("records", "record")
        
        # Ensure timestamp is cast to long and derive date
        df_flat = df_flat.withColumn("timestamp", col("timestamp").cast("long"))
        df_flat = df_flat.withColumn("date", to_date(from_unixtime(col("timestamp") / 1000)))
        
        components = ["compressor", "condenser", "evaporator", "expansion_valve"]
        os.makedirs(output_dir, exist_ok=True)
        
        for comp in components:
            comp_df = df_flat.filter(col("componentType") == comp)
            
            # Use cache and count to avoid multiple evaluations
            comp_df.cache()
            if comp_df.rdd.isEmpty():
                print(f"No data for {comp}, skipping.")
                comp_df.unpersist()
                continue
                
            table_path = os.path.join(output_dir, comp)
            print(f"Writing records for {comp} to {table_path} ...")
            
            if DeltaTable.isDeltaTable(spark, table_path):
                target_table = DeltaTable.forPath(spark, table_path)
                target_table.alias("t").merge(
                    comp_df.alias("s"),
                    "t.eventId = s.eventId"
                ).whenNotMatchedInsertAll().execute()
            else:
                comp_df.write.format("delta") \
                    .mode("append") \
                    .partitionBy("date") \
                    .save(table_path)
                    
            print(f"Successfully wrote {comp} Delta table.")
            comp_df.unpersist()
            
    except Exception as e:
        print(f"Error processing files: {e}")
        import traceback
        traceback.print_exc()
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert JSON batch data to Delta tables.")
    parser.add_argument("--input-dir", required=True, help="Directory containing the raw JSON files")
    parser.add_argument("--output-dir", required=True, help="Directory to save the Delta tables")
    
    args = parser.parse_args()
    process_files(args.input_dir, args.output_dir)
