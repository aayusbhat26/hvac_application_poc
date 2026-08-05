import os
import argparse
import pandas as pd
import re
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def process_bronze_to_staging(input_dir, output_dir):
    components = ["compressor", "condenser", "evaporator", "expansion_valve"]
    os.makedirs(output_dir, exist_ok=True)

    for component in components:
        bronze_path = os.path.join(input_dir, component)
        staging_path = os.path.join(output_dir, component)

        if not os.path.exists(bronze_path):
            print(f"Skipping {component} - Bronze data not found.")
            continue

        print(f"Processing Bronze -> Staging for {component}...")
        
        try:
            # Read bronze delta table
            dt = DeltaTable(bronze_path)
            df = dt.to_pandas()

            # Rename columns to snake_case to match staging schema
            df.columns = [camel_to_snake(col) for col in df.columns]

            # Write to staging delta table
            write_deltalake(
                staging_path,
                df,
                mode="overwrite", # In POC, overwrite is simpler for daily batch without CDC
                partition_by=["date"] if "date" in df.columns else None
            )
            print(f"Successfully wrote {len(df)} records to Staging: {component}")
        except Exception as e:
            print(f"Error processing {component}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    process_bronze_to_staging(args.input_dir, args.output_dir)
