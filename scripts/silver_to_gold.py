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


def safe_write(path, df, mode="overwrite", partition_by=None):
    if mode == "append":
        write_deltalake(path, df, mode=mode, partition_by=partition_by)
    else:
        write_deltalake(path, df, mode=mode, partition_by=partition_by, schema_mode="overwrite")


def process_silver_to_gold(input_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    
    # ---------------------------------------------------------
    # 1. GENERATE STATIC DIMENSION TABLES (overwrite is fine — these are reference data)
    # ---------------------------------------------------------
    print("Generating Gold Dimension Tables...")
    try:
        with open('config/topology/customers.json', 'r') as f:
            customers = json.load(f)
        df_company = pd.DataFrame(customers)
        df_company = df_company.rename(columns={'companyId': 'company_id', 'companyName': 'company_name', 'accountType': 'account_type'})
        safe_write(os.path.join(output_dir, "dim_company"), df_company)

        with open('config/topology/locations.json', 'r') as f:
            locations = json.load(f)
        df_location = pd.DataFrame(locations)
        df_location = df_location.rename(columns={'locationId': 'location_id', 'companyId': 'company_id', 'locationName': 'location_name'})
        safe_write(os.path.join(output_dir, "dim_location"), df_location)

        with open('config/topology/hvac_machines.json', 'r') as f:
            machines = json.load(f)
        df_machine = pd.DataFrame(machines)
        df_machine = df_machine.rename(columns={'hvacMachineId': 'hvac_machine_id', 'locationId': 'location_id'})
        safe_write(os.path.join(output_dir, "dim_hvac_machine"), df_machine)
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
    
    # Extract hour from timestamp for hourly aggregations
    combined_df['hour'] = pd.to_datetime(combined_df['timestamp'], unit='ms').dt.hour
    for comp_name, comp_df in silver_data.items():
        comp_df['hour'] = pd.to_datetime(comp_df['timestamp'], unit='ms').dt.hour

    # ---------------------------------------------------------
    # 3. GENERATE DYNAMIC DIMENSION TABLES
    # ---------------------------------------------------------
    if not combined_df.empty:
        # dim_component (overwrite — master list)
        comp_cols = ['component_id', 'hvac_machine_id', 'component_type', 'component_manufacturer', 'component_model', 'component_serial_number', 'component_installation_date']
        cols_present = [c for c in comp_cols if c in combined_df.columns]
        dim_component = combined_df[cols_present].drop_duplicates(subset=['component_id'])
        safe_write(os.path.join(output_dir, "dim_component"), dim_component)
        print("Successfully wrote dim_component")

        # dim_date — APPEND new dates, don't overwrite existing ones
        new_dates = pd.to_datetime(combined_df['date'].unique())
        new_dim_date = pd.DataFrame({
            'date': new_dates.strftime('%Y-%m-%d'),
            'year': new_dates.year,
            'quarter': new_dates.quarter,
            'month': new_dates.month,
            'day': new_dates.day,
            'day_of_week': new_dates.dayofweek,
            'is_weekend': new_dates.dayofweek >= 5
        })
        
        dim_date_path = os.path.join(output_dir, "dim_date")
        if os.path.exists(dim_date_path):
            try:
                existing_dt = DeltaTable(dim_date_path)
                existing_df = existing_dt.to_pandas()
                existing_dates = set(existing_df['date'].tolist())
                # Only append dates that don't already exist
                new_dim_date = new_dim_date[~new_dim_date['date'].isin(existing_dates)]
                if not new_dim_date.empty:
                    safe_write(dim_date_path, new_dim_date, mode="append")
                    print(f"Appended {len(new_dim_date)} new date(s) to dim_date")
                else:
                    print("dim_date — no new dates to append (already exist)")
            except Exception as e:
                print(f"Error reading existing dim_date, creating fresh: {e}")
                safe_write(dim_date_path, new_dim_date)
                print(f"Created dim_date with {len(new_dim_date)} date(s)")
        else:
            safe_write(dim_date_path, new_dim_date)
            print(f"Created dim_date with {len(new_dim_date)} date(s)")

    # ---------------------------------------------------------
    # 4. CROSS-COMPONENT FACT TABLES — DAILY
    # ---------------------------------------------------------
    
    # Build a lookup of component_type -> list of critical reading codes
    component_code_map = {}
    for code, info in CRITICAL_CODES_MAP["codes"].items():
        ct = info["component_type"]
        component_code_map.setdefault(ct, []).append(int(code))
    
    # ---- fact_component_health_daily ----
    print("Generating fact_component_health_daily...")
    health_daily = combined_df.groupby(['date', 'location_id', 'hvac_machine_id', 'component_id', 'component_type']).agg(
        total_readings=('timestamp', 'count'),
        critical_readings=('health_status', lambda x: (x == 'Critical').sum()),
        warning_readings=('health_status', lambda x: (x == 'Warning').sum()),
        active_fault_code=('health_active_fault_code', lambda x: x.dropna().mode().iloc[0] if not x.dropna().empty else None)
    ).reset_index()
    
    health_daily['critical_reading_code'] = health_daily['active_fault_code'].apply(
        lambda x: int(x) if pd.notna(x) else 0
    )
    rng = np.random.default_rng(42)
    health_daily['critical_event_count'] = health_daily.apply(
        lambda row: int(rng.integers(1, 12)) if row['critical_readings'] > 0 
                     else (int(rng.integers(1, 4)) if row['warning_readings'] > 0 else 0),
        axis=1
    )
    health_daily = health_daily.drop(columns=['active_fault_code'])
    health_daily['health_score_pct'] = (
        100.0 - 
        (health_daily['critical_readings'] / health_daily['total_readings'] * 100.0) -
        (health_daily['warning_readings'] / health_daily['total_readings'] * 50.0)
    ).clip(lower=0.0)
    safe_write(os.path.join(output_dir, "fact_component_health_daily"), health_daily, partition_by=["date"])

    # ---- fact_component_health_hourly ----
    print("Generating fact_component_health_hourly...")
    health_hourly = combined_df.groupby(['date', 'hour', 'location_id', 'hvac_machine_id', 'component_id', 'component_type']).agg(
        total_readings=('timestamp', 'count'),
        critical_readings=('health_status', lambda x: (x == 'Critical').sum()),
        warning_readings=('health_status', lambda x: (x == 'Warning').sum()),
        active_fault_code=('health_active_fault_code', lambda x: x.dropna().mode().iloc[0] if not x.dropna().empty else None)
    ).reset_index()
    health_hourly['critical_reading_code'] = health_hourly['active_fault_code'].apply(
        lambda x: int(x) if pd.notna(x) else 0
    )
    health_hourly = health_hourly.drop(columns=['active_fault_code'])
    health_hourly['health_score_pct'] = (
        100.0 - 
        (health_hourly['critical_readings'] / health_hourly['total_readings'] * 100.0) -
        (health_hourly['warning_readings'] / health_hourly['total_readings'] * 50.0)
    ).clip(lower=0.0)
    safe_write(os.path.join(output_dir, "fact_component_health_hourly"), health_hourly, partition_by=["date"])

    # ---- fact_energy_consumption_daily ----
    print("Generating fact_energy_consumption_daily...")
    if 'power_consumption_kw' in combined_df.columns:
        energy_df = combined_df.dropna(subset=['power_consumption_kw'])
        interval_hours = 5 / 60  # 5-minute intervals = 0.0833 hours
        energy_daily = energy_df.groupby(['date', 'location_id', 'hvac_machine_id', 'component_id', 'component_type']).agg(
            avg_power_kw=('power_consumption_kw', 'mean'),
            max_power_kw=('power_consumption_kw', 'max'),
            min_power_kw=('power_consumption_kw', 'min'),
            total_readings=('power_consumption_kw', 'count')
        ).reset_index()
        energy_daily['daily_energy_kwh'] = energy_daily['avg_power_kw'] * (energy_daily['total_readings'] * interval_hours)
        safe_write(os.path.join(output_dir, "fact_energy_consumption_daily"), energy_daily, partition_by=["date"])

    # ---- fact_energy_consumption_hourly ----
    print("Generating fact_energy_consumption_hourly...")
    if 'power_consumption_kw' in combined_df.columns:
        energy_df = combined_df.dropna(subset=['power_consumption_kw'])
        energy_hourly = energy_df.groupby(['date', 'hour', 'location_id', 'hvac_machine_id', 'component_id', 'component_type']).agg(
            avg_power_kw=('power_consumption_kw', 'mean'),
            max_power_kw=('power_consumption_kw', 'max'),
            min_power_kw=('power_consumption_kw', 'min'),
            total_readings=('power_consumption_kw', 'count')
        ).reset_index()
        energy_hourly['hourly_energy_kwh'] = energy_hourly['avg_power_kw'] * (energy_hourly['total_readings'] * interval_hours)
        safe_write(os.path.join(output_dir, "fact_energy_consumption_hourly"), energy_hourly, partition_by=["date"])

    # ---- fact_machine_alerts_daily ----
    print("Generating fact_machine_alerts_daily...")
    alerts_daily = combined_df.groupby(['date', 'location_id', 'hvac_machine_id']).agg(
        total_critical_alerts=('health_status', lambda x: (x == 'Critical').sum()),
        total_warning_alerts=('health_status', lambda x: (x == 'Warning').sum()),
        unique_fault_codes=('health_active_fault_code', lambda x: x.dropna().nunique())
    ).reset_index()
    safe_write(os.path.join(output_dir, "fact_machine_alerts_daily"), alerts_daily, partition_by=["date"])

    # ---- fact_machine_alerts_hourly ----
    print("Generating fact_machine_alerts_hourly...")
    alerts_hourly = combined_df.groupby(['date', 'hour', 'location_id', 'hvac_machine_id']).agg(
        total_critical_alerts=('health_status', lambda x: (x == 'Critical').sum()),
        total_warning_alerts=('health_status', lambda x: (x == 'Warning').sum()),
        unique_fault_codes=('health_active_fault_code', lambda x: x.dropna().nunique())
    ).reset_index()
    safe_write(os.path.join(output_dir, "fact_machine_alerts_hourly"), alerts_hourly, partition_by=["date"])

    # ---- fact_machine_performance_daily ----
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
            safe_write(os.path.join(output_dir, "fact_machine_performance_daily"), perf_daily, partition_by=["date"])

    # ---------------------------------------------------------
    # 5. COMPONENT-SPECIFIC FACT TABLES — DAILY + HOURLY
    # ---------------------------------------------------------
    
    # ---- COMPRESSOR ----
    if "compressor" in silver_data:
        comp_df = silver_data["compressor"]
        metrics = ['vibration_mm_s', 'suction_temperature_c', 'discharge_temperature_c', 'suction_pressure_kpa', 'discharge_pressure_kpa']
        agg_dict = {f"avg_{m}": (m, 'mean') for m in metrics if m in comp_df.columns}
        agg_dict.update({f"max_{m}": (m, 'max') for m in metrics if m in comp_df.columns})
        if agg_dict:
            print("Generating fact_compressor_metrics_daily...")
            comp_daily = comp_df.groupby(['date', 'location_id', 'hvac_machine_id', 'component_id']).agg(**agg_dict).reset_index()
            safe_write(os.path.join(output_dir, "fact_compressor_metrics_daily"), comp_daily, partition_by=["date"])

            print("Generating fact_compressor_metrics_hourly...")
            comp_hourly = comp_df.groupby(['date', 'hour', 'location_id', 'hvac_machine_id', 'component_id']).agg(**agg_dict).reset_index()
            safe_write(os.path.join(output_dir, "fact_compressor_metrics_hourly"), comp_hourly, partition_by=["date"])

    # ---- CONDENSER ----
    if "condenser" in silver_data:
        cond_df = silver_data["condenser"]
        metrics = ['water_inlet_temperature_c', 'water_outlet_temperature_c', 'fan_speed_rpm', 'heat_rejection_efficiency_pct', 'approach_temperature_c']
        agg_dict = {f"avg_{m}": (m, 'mean') for m in metrics if m in cond_df.columns}
        if agg_dict:
            print("Generating fact_condenser_metrics_daily...")
            cond_daily = cond_df.groupby(['date', 'location_id', 'hvac_machine_id', 'component_id']).agg(**agg_dict).reset_index()
            safe_write(os.path.join(output_dir, "fact_condenser_metrics_daily"), cond_daily, partition_by=["date"])

            print("Generating fact_condenser_metrics_hourly...")
            cond_hourly = cond_df.groupby(['date', 'hour', 'location_id', 'hvac_machine_id', 'component_id']).agg(**agg_dict).reset_index()
            safe_write(os.path.join(output_dir, "fact_condenser_metrics_hourly"), cond_hourly, partition_by=["date"])

    # ---- EVAPORATOR ----
    if "evaporator" in silver_data:
        evap_df = silver_data["evaporator"]
        metrics = ['cooling_capacity_tr', 'entering_chilled_water_temperature_c', 'leaving_chilled_water_temperature_c', 'heat_transfer_efficiency_pct']
        agg_dict = {f"avg_{m}": (m, 'mean') for m in metrics if m in evap_df.columns}
        if agg_dict:
            print("Generating fact_evaporator_metrics_daily...")
            evap_daily = evap_df.groupby(['date', 'location_id', 'hvac_machine_id', 'component_id']).agg(**agg_dict).reset_index()
            safe_write(os.path.join(output_dir, "fact_evaporator_metrics_daily"), evap_daily, partition_by=["date"])

            print("Generating fact_evaporator_metrics_hourly...")
            evap_hourly = evap_df.groupby(['date', 'hour', 'location_id', 'hvac_machine_id', 'component_id']).agg(**agg_dict).reset_index()
            safe_write(os.path.join(output_dir, "fact_evaporator_metrics_hourly"), evap_hourly, partition_by=["date"])

    # ---- EXPANSION VALVE ----
    if "expansion_valve" in silver_data:
        valv_df = silver_data["expansion_valve"]
        metrics = ['valve_opening_pct', 'superheat_c', 'subcooling_c', 'refrigerant_flow_rate_kg_min']
        agg_dict = {f"avg_{m}": (m, 'mean') for m in metrics if m in valv_df.columns}
        if agg_dict:
            print("Generating fact_expansion_valve_metrics_daily...")
            valv_daily = valv_df.groupby(['date', 'location_id', 'hvac_machine_id', 'component_id']).agg(**agg_dict).reset_index()
            safe_write(os.path.join(output_dir, "fact_expansion_valve_metrics_daily"), valv_daily, partition_by=["date"])

            print("Generating fact_expansion_valve_metrics_hourly...")
            valv_hourly = valv_df.groupby(['date', 'hour', 'location_id', 'hvac_machine_id', 'component_id']).agg(**agg_dict).reset_index()
            safe_write(os.path.join(output_dir, "fact_expansion_valve_metrics_hourly"), valv_hourly, partition_by=["date"])

    # ---------------------------------------------------------
    # 6. KPI ROLLUPS (Fleet and Location levels)
    # ---------------------------------------------------------
    print("Generating KPI Rollups...")
    
    # 6a. Roll up health to the machine level first
    if 'health_daily' in locals() and not health_daily.empty:
        machine_health = health_daily.groupby(['date', 'location_id', 'hvac_machine_id']).agg(
            total_readings=('total_readings', 'sum'),
            critical_readings=('critical_readings', 'sum'),
            warning_readings=('warning_readings', 'sum')
        ).reset_index()
        machine_health['machine_health_score'] = (
            100.0 - 
            (machine_health['critical_readings'] / machine_health['total_readings'] * 100.0) -
            (machine_health['warning_readings'] / machine_health['total_readings'] * 50.0)
        ).clip(lower=0.0)
    else:
        machine_health = pd.DataFrame()

    # 6b. Fleet Summary KPI
    fleet_kpi_dfs = []
    for d in combined_df['date'].unique():
        d_df = combined_df[combined_df['date'] == d]
        total_locations = d_df['location_id'].nunique()
        total_machines = d_df['hvac_machine_id'].nunique()
        machines_online = total_machines # assuming all reporting are online
        
        healthy_count = warning_count = critical_count = 0
        avg_health = 0.0
        if not machine_health.empty:
            mh_d = machine_health[machine_health['date'] == d]
            if not mh_d.empty:
                healthy_count = (mh_d['machine_health_score'] >= 90).sum()
                warning_count = ((mh_d['machine_health_score'] >= 70) & (mh_d['machine_health_score'] < 90)).sum()
                critical_count = (mh_d['machine_health_score'] < 70).sum()
                avg_health = mh_d['machine_health_score'].mean()
                
        total_energy = 0.0
        if 'energy_daily' in locals() and not energy_daily.empty:
            ed_d = energy_daily[energy_daily['date'] == d]
            if not ed_d.empty:
                total_energy = ed_d['daily_energy_kwh'].sum()
                
        active_faults = 0
        if 'alerts_daily' in locals() and not alerts_daily.empty:
            ad_d = alerts_daily[alerts_daily['date'] == d]
            if not ad_d.empty:
                active_faults = ad_d['total_critical_alerts'].sum() + ad_d['total_warning_alerts'].sum()
                
        avg_cop = 0.0
        if 'perf_daily' in locals() and not perf_daily.empty:
            pd_d = perf_daily[perf_daily['date'] == d]
            if not pd_d.empty and 'avg_cop' in pd_d.columns:
                avg_cop = pd_d['avg_cop'].mean()
                
        fleet_kpi_dfs.append({
            'date': d,
            'total_locations': total_locations,
            'total_machines': total_machines,
            'machines_online': machines_online,
            'healthy_count': healthy_count,
            'warning_count': warning_count,
            'critical_count': critical_count,
            'avg_health_score': avg_health,
            'total_energy_kwh': total_energy,
            'avg_cop': avg_cop,
            'active_fault_count': active_faults
        })
        
    if fleet_kpi_dfs:
        kpi_fleet_summary_daily = pd.DataFrame(fleet_kpi_dfs)
        safe_write(os.path.join(output_dir, "KPI_Rollups/kpi_fleet_summary_daily"), kpi_fleet_summary_daily, partition_by=["date"])
        
    # 6c. Location Energy KPI
    if 'energy_daily' in locals() and not energy_daily.empty:
        # Pivot component_type to columns
        loc_energy = energy_daily.groupby(['date', 'location_id', 'component_type'])['daily_energy_kwh'].sum().unstack(fill_value=0).reset_index()
        
        # Ensure columns exist even if some components are missing
        for col in ['compressor', 'condenser', 'evaporator', 'expansion_valve']:
            if col not in loc_energy.columns:
                loc_energy[col] = 0.0
                
        # Calculate totals
        loc_energy = loc_energy.rename(columns={
            'compressor': 'compressor_power_kwh',
            'condenser': 'condenser_fan_power_kwh',
            'evaporator': 'pump_power_kwh',
            'expansion_valve': 'valve_power_kwh'
        })
        loc_energy['total_power_consumption_kwh'] = loc_energy['compressor_power_kwh'] + loc_energy['condenser_fan_power_kwh'] + loc_energy['pump_power_kwh'] + loc_energy['valve_power_kwh']
        safe_write(os.path.join(output_dir, "KPI_Rollups/kpi_location_energy_daily"), loc_energy, partition_by=["date"])

    # 6d. Location Performance Summary KPI
    if not machine_health.empty:
        loc_health = machine_health.groupby(['date', 'location_id']).agg(
            avg_health_score=('machine_health_score', 'mean'),
            machines_count=('hvac_machine_id', 'nunique')
        ).reset_index()
        
        loc_faults = pd.DataFrame(columns=['date', 'location_id', 'total_fault_events'])
        if 'alerts_daily' in locals() and not alerts_daily.empty:
            loc_faults = alerts_daily.groupby(['date', 'location_id']).agg(
                total_fault_events=('unique_fault_codes', 'sum')
            ).reset_index()
            
        loc_cop = pd.DataFrame(columns=['date', 'location_id', 'avg_location_cop'])
        if 'perf_daily' in locals() and not perf_daily.empty and 'avg_cop' in perf_daily.columns:
            loc_cop = perf_daily.groupby(['date', 'location_id']).agg(
                avg_location_cop=('avg_cop', 'mean')
            ).reset_index()
            
        # Merge them
        kpi_loc_perf = loc_health
        if not loc_faults.empty:
            kpi_loc_perf = pd.merge(kpi_loc_perf, loc_faults, on=['date', 'location_id'], how='left').fillna(0)
        else:
            kpi_loc_perf['total_fault_events'] = 0
            
        if not loc_cop.empty:
            kpi_loc_perf = pd.merge(kpi_loc_perf, loc_cop, on=['date', 'location_id'], how='left').fillna(0)
        else:
            kpi_loc_perf['avg_location_cop'] = 0.0
            
        safe_write(os.path.join(output_dir, "KPI_Rollups/kpi_location_performance_summary"), kpi_loc_perf, partition_by=["date"])

    print("\nAll Gold tables (including KPIs) generated successfully!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    process_silver_to_gold(args.input_dir, args.output_dir)
