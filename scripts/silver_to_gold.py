import os
import argparse
import json
import pandas as pd
from datetime import datetime
from deltalake.writer import write_deltalake
from deltalake import DeltaTable

def process_silver_to_gold(input_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. GENERATE DIMENSION TABLES FROM CONFIGS
    print("Generating Gold Dimension Tables...")
    try:
        with open('config/topology/customers.json', 'r') as f:
            customers = json.load(f)
        df_company = pd.DataFrame(customers)
        df_company = df_company.rename(columns={'companyId': 'company_id', 'companyName': 'company_name', 'accountType': 'account_type'})
        write_deltalake(os.path.join(output_dir, "dim_company"), df_company, mode="overwrite")

        with open('config/topology/locations.json', 'r') as f:
            locations = json.load(f)
        df_location = pd.DataFrame(locations)
        df_location = df_location.rename(columns={'locationId': 'location_id', 'companyId': 'company_id', 'locationName': 'location_name'})
        write_deltalake(os.path.join(output_dir, "dim_location"), df_location, mode="overwrite")

        with open('config/topology/hvac_machines.json', 'r') as f:
            machines = json.load(f)
        df_machine = pd.DataFrame(machines)
        df_machine = df_machine.rename(columns={'hvacMachineId': 'hvac_machine_id', 'locationId': 'location_id'})
        write_deltalake(os.path.join(output_dir, "dim_hvac_machine"), df_machine, mode="overwrite")
        print("Successfully generated dim_company, dim_location, dim_hvac_machine")
    except Exception as e:
        print(f"Error generating dimensions: {e}")

    # 2. GENERATE FACT TABLES FROM SILVER TELEMETRY
    components = ["compressor", "condenser", "evaporator", "expansion_valve"]
    all_silver_dfs = []

    for comp in components:
        silver_path = os.path.join(input_dir, comp)
        if os.path.exists(silver_path):
            try:
                dt = DeltaTable(silver_path)
                df = dt.to_pandas()
                df['component_type'] = comp
                all_silver_dfs.append(df)
            except Exception as e:
                print(f"Error reading silver table {comp}: {e}")
                
    if not all_silver_dfs:
        print("No silver telemetry data found to build fact tables.")
        return

    # Combine all telemetry to compute fleet-wide or component-wide facts
    # For a POC, we just combine common columns
    common_cols = ['hvac_machine_id', 'location_id', 'company_id', 'timestamp', 'date', 'health_status', 'component_id', 'component_type']
    
    combined_df = pd.DataFrame()
    for df in all_silver_dfs:
        cols_to_keep = [c for c in common_cols if c in df.columns]
        combined_df = pd.concat([combined_df, df[cols_to_keep]], ignore_index=True)

    if combined_df.empty:
        return

    # Create fact_component_health_daily
    print("Generating fact_component_health_daily...")
    # Group by date, location_id, hvac_machine_id, component_id to calculate daily health
    health_daily = combined_df.groupby(['date', 'location_id', 'hvac_machine_id', 'component_id', 'component_type']).agg(
        total_readings=('timestamp', 'count'),
        critical_readings=('health_status', lambda x: (x == 'Critical').sum())
    ).reset_index()
    
    # Simple logic: health_score = 100 - (critical_readings / total_readings * 100)
    health_daily['health_score_pct'] = 100.0 - (health_daily['critical_readings'] / health_daily['total_readings'] * 100.0)
    
    write_deltalake(os.path.join(output_dir, "fact_component_health_daily"), health_daily, mode="overwrite", partition_by=["date"])
    print("Successfully wrote fact_component_health_daily")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    process_silver_to_gold(args.input_dir, args.output_dir)
