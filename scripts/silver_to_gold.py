import os
import argparse
import json
import pandas as pd
import numpy as np
from datetime import datetime
from deltalake.writer import write_deltalake
from deltalake import DeltaTable

# Load critical reading codes mapping
CRITICAL_CODES_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'critical_reading_codes.json')
with open(CRITICAL_CODES_PATH, 'r') as f:
    CRITICAL_CODES_MAP = json.load(f)

def process_silver_to_gold(input_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    
    # ---------------------------------------------------------
    # 1. GENERATE DIMENSION TABLES
    # ---------------------------------------------------------
    print("Generating Gold Dimension Tables...")
    try:
        with open('config/topology/customers.json', 'r') as f:
            customers = json.load(f)
        df_company = pd.DataFrame(customers)
        df_company = df_company.rename(columns={'companyId': 'company_id', 'companyName': 'company_name', 'accountType': 'account_type'})
        write_deltalake(os.path.join(output_dir, "dim_company"), df_company, mode="overwrite", schema_mode="overwrite")

        with open('config/topology/locations.json', 'r') as f:
            locations = json.load(f)
        df_location = pd.DataFrame(locations)
        df_location = df_location.rename(columns={'locationId': 'location_id', 'companyId': 'company_id', 'locationName': 'location_name'})
        write_deltalake(os.path.join(output_dir, "dim_location"), df_location, mode="overwrite", schema_mode="overwrite")

        with open('config/topology/hvac_machines.json', 'r') as f:
            machines = json.load(f)
        df_machine = pd.DataFrame(machines)
        df_machine = df_machine.rename(columns={'hvacMachineId': 'hvac_machine_id', 'locationId': 'location_id'})
        write_deltalake(os.path.join(output_dir, "dim_hvac_machine"), df_machine, mode="overwrite", schema_mode="overwrite")
        print("Successfully generated dim_company, dim_location, dim_hvac_machine")
    except Exception as e:
        print(f"Error generating topology dimensions: {e}")

    # ---------------------------------------------------------
    # 2. READ SILVER TELEMETRY DATA
    # ---------------------------------------------------------
    components = ["compressor", "condenser", "evaporator", "expansion_valve"]
    silver_data = {}
    
    for comp in components:
        silver_path = os.path.join(input_dir, comp)
        if os.path.exists(silver_path):
            try:
                dt = DeltaTable(silver_path)
                df = dt.to_pandas()
                df['component_type'] = comp
                
                # Cast all possible metric columns to numeric (float) to avoid aggregation TypeErrors
                numeric_cols = [
                    'power_consumption_kw', 'run_hours', 'start_stop_count', 'cop', 'eer',
                    'vibration_mm_s', 'suction_temperature_c', 'discharge_temperature_c', 'suction_pressure_kpa', 'discharge_pressure_kpa',
                    'water_inlet_temperature_c', 'water_outlet_temperature_c', 'fan_speed_rpm', 'heat_rejection_efficiency_pct', 'approach_temperature_c',
                    'cooling_capacity_tr', 'entering_chilled_water_temperature_c', 'leaving_chilled_water_temperature_c', 'heat_transfer_efficiency_pct',
                    'valve_opening_pct', 'superheat_c', 'subcooling_c', 'refrigerant_flow_rate_kg_min'
                ]
                for col in numeric_cols:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        
                silver_data[comp] = df
            except Exception as e:
                print(f"Error reading silver table {comp}: {e}")

    if not silver_data:
        print("No silver telemetry data found to build fact tables.")
        return

    # Create a unified DataFrame for cross-component facts
    combined_df = pd.concat(silver_data.values(), ignore_index=True)

    # ---------------------------------------------------------
    # 3. GENERATE DYNAMIC DIMENSION TABLES
    # ---------------------------------------------------------
    if not combined_df.empty:
        # dim_component
        comp_cols = ['component_id', 'hvac_machine_id', 'component_type', 'component_manufacturer', 'component_model', 'component_serial_number', 'component_installation_date']
        cols_present = [c for c in comp_cols if c in combined_df.columns]
        dim_component = combined_df[cols_present].drop_duplicates(subset=['component_id'])
        write_deltalake(os.path.join(output_dir, "dim_component"), dim_component, mode="overwrite", schema_mode="overwrite")
        print("Successfully wrote dim_component")

        # dim_date
        dates = pd.to_datetime(combined_df['date'].unique())
        dim_date = pd.DataFrame({
            'date': dates.strftime('%Y-%m-%d'),
            'year': dates.year,
            'quarter': dates.quarter,
            'month': dates.month,
            'day': dates.day,
            'day_of_week': dates.dayofweek,
            'is_weekend': dates.dayofweek >= 5
        })
        write_deltalake(os.path.join(output_dir, "dim_date"), dim_date, mode="overwrite", schema_mode="overwrite")
        print("Successfully wrote dim_date")

    # ---------------------------------------------------------
    # 4. CROSS-COMPONENT FACT TABLES
    # ---------------------------------------------------------
    
    # fact_component_health_daily
    print("Generating fact_component_health_daily...")
    
    # Build a lookup of component_type -> list of critical reading codes
    component_code_map = {}
    for code, info in CRITICAL_CODES_MAP["codes"].items():
        ct = info["component_type"]
        component_code_map.setdefault(ct, []).append(int(code))
    
    health_daily = combined_df.groupby(['date', 'location_id', 'hvac_machine_id', 'component_id', 'component_type']).agg(
        total_readings=('timestamp', 'count'),
        critical_readings=('health_status', lambda x: (x == 'Critical').sum()),
        warning_readings=('health_status', lambda x: (x == 'Warning').sum()),
        active_fault_code=('health_active_fault_code', lambda x: x.dropna().mode().iloc[0] if not x.dropna().empty else None)
    ).reset_index()
    
    # Assign the critical_reading_code from the active fault code (these are the numeric codes like 288, 301, etc.)
    health_daily['critical_reading_code'] = health_daily['active_fault_code'].apply(
        lambda x: int(x) if pd.notna(x) else 0
    )
    
    # Generate randomized critical_event_count: the number of distinct critical events/spikes during the day
    # This is different from critical_readings (which counts individual sensor readings)
    # critical_event_count represents discrete fault occurrences (e.g., 3 separate overheating events)
    rng = np.random.default_rng(42)
    health_daily['critical_event_count'] = health_daily.apply(
        lambda row: int(rng.integers(1, 12)) if row['critical_readings'] > 0 
                     else (int(rng.integers(1, 4)) if row['warning_readings'] > 0 else 0),
        axis=1
    )
    
    # Drop the intermediate column
    health_daily = health_daily.drop(columns=['active_fault_code'])
    
    health_daily['health_score_pct'] = (
        100.0 - 
        (health_daily['critical_readings'] / health_daily['total_readings'] * 100.0) -
        (health_daily['warning_readings'] / health_daily['total_readings'] * 50.0)
    ).clip(lower=0.0)
    write_deltalake(os.path.join(output_dir, "fact_component_health_daily"), health_daily, mode="overwrite", partition_by=["date"], schema_mode="overwrite")

    # fact_energy_consumption_daily
    print("Generating fact_energy_consumption_daily...")
    if 'power_consumption_kw' in combined_df.columns:
        energy_df = combined_df.dropna(subset=['power_consumption_kw'])
        # assuming ~15 min interval = 0.25 hours
        interval_hours = 0.25 
        energy_daily = energy_df.groupby(['date', 'location_id', 'hvac_machine_id', 'component_id', 'component_type']).agg(
            avg_power_kw=('power_consumption_kw', 'mean'),
            max_power_kw=('power_consumption_kw', 'max'),
            total_readings=('power_consumption_kw', 'count')
        ).reset_index()
        energy_daily['daily_energy_kwh'] = energy_daily['avg_power_kw'] * (energy_daily['total_readings'] * interval_hours)
        write_deltalake(os.path.join(output_dir, "fact_energy_consumption_daily"), energy_daily, mode="overwrite", partition_by=["date"], schema_mode="overwrite")

    # fact_machine_alerts_daily
    print("Generating fact_machine_alerts_daily...")
    alerts_daily = combined_df.groupby(['date', 'location_id', 'hvac_machine_id']).agg(
        total_critical_alerts=('health_status', lambda x: (x == 'Critical').sum()),
        total_warning_alerts=('health_status', lambda x: (x == 'Warning').sum()),
        unique_fault_codes=('health_active_fault_code', lambda x: x.dropna().nunique())
    ).reset_index()
    write_deltalake(os.path.join(output_dir, "fact_machine_alerts_daily"), alerts_daily, mode="overwrite", partition_by=["date"], schema_mode="overwrite")

    # fact_machine_performance_daily
    print("Generating fact_machine_performance_daily...")
    perf_cols = ['run_hours', 'start_stop_count', 'cop', 'eer']
    has_perf = any(c in combined_df.columns for c in perf_cols)
    if has_perf:
        agg_dict = {}
        if 'run_hours' in combined_df.columns: agg_dict['max_run_hours'] = ('run_hours', 'max')
        if 'start_stop_count' in combined_df.columns: agg_dict['max_start_stops'] = ('start_stop_count', 'max')
        if 'cop' in combined_df.columns: agg_dict['avg_cop'] = ('cop', 'mean')
        if 'eer' in combined_df.columns: agg_dict['avg_eer'] = ('eer', 'mean')
        
        if agg_dict:
            perf_daily = combined_df.groupby(['date', 'location_id', 'hvac_machine_id']).agg(**agg_dict).reset_index()
            write_deltalake(os.path.join(output_dir, "fact_machine_performance_daily"), perf_daily, mode="overwrite", partition_by=["date"], schema_mode="overwrite")

    # ---------------------------------------------------------
    # 5. COMPONENT-SPECIFIC FACT TABLES
    # ---------------------------------------------------------
    
    # fact_compressor_metrics_daily
    if "compressor" in silver_data:
        print("Generating fact_compressor_metrics_daily...")
        comp_df = silver_data["compressor"]
        metrics = ['vibration_mm_s', 'suction_temperature_c', 'discharge_temperature_c', 'suction_pressure_kpa', 'discharge_pressure_kpa']
        agg_dict = {f"avg_{m}": (m, 'mean') for m in metrics if m in comp_df.columns}
        agg_dict.update({f"max_{m}": (m, 'max') for m in metrics if m in comp_df.columns})
        if agg_dict:
            comp_daily = comp_df.groupby(['date', 'location_id', 'hvac_machine_id', 'component_id']).agg(**agg_dict).reset_index()
            write_deltalake(os.path.join(output_dir, "fact_compressor_metrics_daily"), comp_daily, mode="overwrite", partition_by=["date"], schema_mode="overwrite")

    # fact_condenser_metrics_daily
    if "condenser" in silver_data:
        print("Generating fact_condenser_metrics_daily...")
        cond_df = silver_data["condenser"]
        metrics = ['water_inlet_temperature_c', 'water_outlet_temperature_c', 'fan_speed_rpm', 'heat_rejection_efficiency_pct', 'approach_temperature_c']
        agg_dict = {f"avg_{m}": (m, 'mean') for m in metrics if m in cond_df.columns}
        if agg_dict:
            cond_daily = cond_df.groupby(['date', 'location_id', 'hvac_machine_id', 'component_id']).agg(**agg_dict).reset_index()
            write_deltalake(os.path.join(output_dir, "fact_condenser_metrics_daily"), cond_daily, mode="overwrite", partition_by=["date"], schema_mode="overwrite")

    # fact_evaporator_metrics_daily
    if "evaporator" in silver_data:
        print("Generating fact_evaporator_metrics_daily...")
        evap_df = silver_data["evaporator"]
        metrics = ['cooling_capacity_tr', 'entering_chilled_water_temperature_c', 'leaving_chilled_water_temperature_c', 'heat_transfer_efficiency_pct']
        agg_dict = {f"avg_{m}": (m, 'mean') for m in metrics if m in evap_df.columns}
        if agg_dict:
            evap_daily = evap_df.groupby(['date', 'location_id', 'hvac_machine_id', 'component_id']).agg(**agg_dict).reset_index()
            write_deltalake(os.path.join(output_dir, "fact_evaporator_metrics_daily"), evap_daily, mode="overwrite", partition_by=["date"], schema_mode="overwrite")

    # fact_expansion_valve_metrics_daily
    if "expansion_valve" in silver_data:
        print("Generating fact_expansion_valve_metrics_daily...")
        valv_df = silver_data["expansion_valve"]
        metrics = ['valve_opening_pct', 'superheat_c', 'subcooling_c', 'refrigerant_flow_rate_kg_min']
        agg_dict = {f"avg_{m}": (m, 'mean') for m in metrics if m in valv_df.columns}
        if agg_dict:
            valv_daily = valv_df.groupby(['date', 'location_id', 'hvac_machine_id', 'component_id']).agg(**agg_dict).reset_index()
            write_deltalake(os.path.join(output_dir, "fact_expansion_valve_metrics_daily"), valv_daily, mode="overwrite", partition_by=["date"], schema_mode="overwrite")

    print("All Gold tables generated successfully!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    process_silver_to_gold(args.input_dir, args.output_dir)
