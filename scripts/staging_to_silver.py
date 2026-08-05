import os
import argparse
import pandas as pd
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

def process_staging_to_silver(input_dir, output_dir):
    components = ["compressor", "condenser", "evaporator", "expansion_valve"]
    os.makedirs(output_dir, exist_ok=True)

    for component in components:
        staging_path = os.path.join(input_dir, component)
        silver_path = os.path.join(output_dir, component)

        if not os.path.exists(staging_path):
            print(f"Skipping {component} - Staging data not found.")
            continue

        print(f"Processing Staging -> Silver for {component}...")
        
        try:
            # Read staging delta table
            dt = DeltaTable(staging_path)
            df = dt.to_pandas()

            initial_count = len(df)
            
            # Deduplicate by event_id
            if 'event_id' in df.columns:
                df = df.drop_duplicates(subset=['event_id'])
                
            # Drop rows where data_quality is missing or stale
            if 'data_quality' in df.columns:
                df = df[df['data_quality'].isin(['GOOD', 'PARTIAL'])]

            final_count = len(df)
            print(f"Deduplication/Cleaning removed {initial_count - final_count} records.")

            # Write to silver delta table
            write_deltalake(
                silver_path,
                df,
                mode="overwrite", 
                partition_by=["date"] if "date" in df.columns else None
            )
            print(f"Successfully wrote {len(df)} records to Silver: {component}")
        except Exception as e:
            print(f"Error processing {component}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    process_staging_to_silver(args.input_dir, args.output_dir)
