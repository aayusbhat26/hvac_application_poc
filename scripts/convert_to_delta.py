import os
import glob
import json
import argparse
from datetime import datetime
import pandas as pd
from deltalake.writer import write_deltalake

def process_files(input_dir, output_dir):
    # Find all JSON files in the input directory, excluding the metadata folder
    search_pattern = os.path.join(input_dir, "**", "*.json")
    all_files = glob.glob(search_pattern, recursive=True)
    
    # Filter out metadata files
    data_files = [f for f in all_files if "metadata" not in f.replace("\\", "/").split("/")]
    
    if not data_files:
        print(f"No data files found in {input_dir}")
        return

    # Group files by componentType so we can write one Delta table per component
    tables_data = {
        "compressor": [],
        "condenser": [],
        "evaporator": [],
        "expansion_valve": []
    }

    print(f"Found {len(data_files)} JSON files to process.")

    for file_path in data_files:
        try:
            with open(file_path, 'r') as f:
                batch_data = json.load(f)
            
            component_type = batch_data.get("componentType")
            records = batch_data.get("records", [])
            
            if not component_type or not records:
                continue
                
            # Flatten the records and add some batch-level metadata if needed
            # For bronze, we usually just want the records + a partition column
            df = pd.DataFrame(records)
            
            # Inject required metadata from the batch envelope
            meta_fields = [
                "companyId", "companyName", "locationId", "locationName", 
                "locationCity", "locationState", "locationCountry", 
                "hvacMachineId", "componentId", "componentType", 
                "batchId", "uploadedBy", "sourceFileName", "circuitId"
            ]
            for field in meta_fields:
                if field in batch_data:
                    df[field] = batch_data[field]
            
            # Ensure timestamp is an integer
            df['timestamp'] = df['timestamp'].astype('int64')
            
            # Derive a 'date' column for partitioning (YYYY-MM-DD)
            # Timestamp is in epoch milliseconds
            df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.strftime('%Y-%m-%d')
            
            # Append to the corresponding list
            if component_type in tables_data:
                tables_data[component_type].append(df)
            else:
                tables_data[component_type] = [df]
                
        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    # Write each component's data to a Delta table
    os.makedirs(output_dir, exist_ok=True)
    
    for component_type, dfs in tables_data.items():
        if not dfs:
            print(f"No data for {component_type}, skipping.")
            continue
            
        combined_df = pd.concat(dfs, ignore_index=True)
        table_path = os.path.join(output_dir, component_type)
        
        print(f"Writing {len(combined_df)} records for {component_type} to {table_path} ...")
        
        # Write to delta, partitioning by date
        # If the table already exists, it appends the new data.
        write_deltalake(
            table_path, 
            combined_df, 
            mode="append", 
            partition_by=["date"],
            schema_mode="merge"
        )
        print(f"Successfully wrote {component_type} Delta table.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert JSON batch data to Delta tables.")
    parser.add_argument("--input-dir", required=True, help="Directory containing the raw JSON files")
    parser.add_argument("--output-dir", required=True, help="Directory to save the Delta tables")
    
    args = parser.parse_args()
    process_files(args.input_dir, args.output_dir)
