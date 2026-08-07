import os
import argparse
import pandas as pd
import numpy as np
from deltalake import DeltaTable
from deltalake.writer import write_deltalake

# ============================================================================
# BUSINESS QUALITY RULES PER COMPONENT TYPE
# These define valid ranges, mandatory fields, and cross-field validations.
# ============================================================================

QUALITY_RULES = {
    "compressor": {
        "mandatory_fields": ["event_id", "timestamp", "component_id", "hvac_machine_id", "location_id", "health_status"],
        "numeric_range_checks": {
            "suction_pressure_kpa":       (100, 800),     # physically impossible outside this
            "discharge_pressure_kpa":     (500, 2500),
            "oil_pressure_kpa":           (50, 600),
            "suction_temperature_c":      (-20, 30),
            "discharge_temperature_c":    (30, 120),      # above 120 is unrealistic even in fault
            "oil_temperature_c":          (10, 100),
            "motor_winding_temperature_c":(20, 150),
            "voltage_v":                  (350, 480),
            "current_a":                  (0, 100),
            "power_consumption_kw":       (0, 150),
            "frequency_hz":              (45, 65),
            "power_factor":              (0.5, 1.0),
            "rpm":                       (0, 5000),
            "vibration_mm_s":            (0, 20),         # above 20 is sensor malfunction
            "bearing_temperature_c":     (10, 130),
            "eer":                       (2, 25),
            "cop":                       (1, 10),
        },
        "cross_field_checks": [
            # discharge pressure must be higher than suction pressure
            ("discharge_pressure_kpa", ">", "suction_pressure_kpa"),
            # discharge temp must be higher than suction temp
            ("discharge_temperature_c", ">", "suction_temperature_c"),
        ]
    },
    "condenser": {
        "mandatory_fields": ["event_id", "timestamp", "component_id", "hvac_machine_id", "location_id", "health_status"],
        "numeric_range_checks": {
            "condenser_pressure_kpa":             (500, 2500),
            "pressure_drop_kpa":                  (0, 100),
            "water_inlet_temperature_c":          (5, 60),
            "water_outlet_temperature_c":         (5, 70),
            "cooling_tower_water_temperature_c":  (5, 55),
            "ambient_air_temperature_c":          (-10, 60),
            "water_flow_rate_gpm":                (0, 1500),
            "water_valve_position_pct":           (0, 100),
            "fan_speed_rpm":                      (0, 2000),
            "fan_current_a":                      (0, 50),
            "fan_power_kw":                       (0, 30),
            "voltage_v":                          (350, 480),
            "frequency_hz":                       (45, 65),
            "power_factor":                       (0.5, 1.0),
            "power_consumption_kw":               (0, 50),
            "approach_temperature_c":             (-5, 20),
            "heat_rejection_efficiency_pct":      (0, 100),
        },
        "cross_field_checks": [
            # water outlet must be warmer than inlet (heat is being rejected)
            ("water_outlet_temperature_c", ">", "water_inlet_temperature_c"),
        ]
    },
    "evaporator": {
        "mandatory_fields": ["event_id", "timestamp", "component_id", "hvac_machine_id", "location_id", "health_status"],
        "numeric_range_checks": {
            "entering_chilled_water_temperature_c": (0, 30),
            "leaving_chilled_water_temperature_c":  (-5, 25),
            "temperature_difference_c":             (0, 20),
            "evaporator_pressure_kpa":              (100, 700),
            "water_flow_rate_gpm":                  (0, 1500),
            "lwt_setpoint_c":                       (2, 15),
            "pump_power_consumption_kw":            (0, 60),
            "pump_current_a":                       (0, 80),
            "pump_frequency_hz":                    (45, 65),
            "cooling_capacity_tr":                  (0, 500),
            "heat_transfer_efficiency_pct":         (0, 100),
        },
        "cross_field_checks": [
            # entering water temp must be higher than leaving (water is being cooled)
            ("entering_chilled_water_temperature_c", ">", "leaving_chilled_water_temperature_c"),
        ]
    },
    "expansion_valve": {
        "mandatory_fields": ["event_id", "timestamp", "component_id", "hvac_machine_id", "location_id", "health_status"],
        "numeric_range_checks": {
            "valve_opening_pct":             (0, 100),
            "liquid_line_temperature_c":     (5, 60),
            "evaporator_outlet_temperature_c": (-10, 30),
            "superheat_c":                   (0, 30),
            "subcooling_c":                  (0, 25),
            "inlet_pressure_kpa":            (400, 2500),
            "outlet_pressure_kpa":           (100, 800),
            "pressure_drop_kpa":             (0, 1500),
            "refrigerant_flow_rate_kg_min":  (0, 60),
            "actuator_voltage_v":            (10, 35),
            "actuator_current_a":            (0, 2),
            "power_consumption_w":           (0, 30),
        },
        "cross_field_checks": [
            # inlet pressure must be higher than outlet pressure
            ("inlet_pressure_kpa", ">", "outlet_pressure_kpa"),
        ]
    },
}


def run_quality_checks(df, component, rules):
    """
    Run business quality checks on a staging DataFrame.
    Returns: (clean_df, quality_report_dict)
    """
    initial_count = len(df)
    flags = pd.Series(False, index=df.index)  # True = flagged for removal
    report = {
        "component": component,
        "initial_records": initial_count,
        "checks": {}
    }

    # ---------------------------------------------------------------
    # CHECK 1: Deduplication by event_id
    # ---------------------------------------------------------------
    if 'event_id' in df.columns:
        dup_mask = df.duplicated(subset=['event_id'], keep='first')
        dup_count = dup_mask.sum()
        flags |= dup_mask
        report["checks"]["duplicate_event_ids"] = int(dup_count)

    # ---------------------------------------------------------------
    # CHECK 2: Data quality filter (keep GOOD and PARTIAL only)
    # ---------------------------------------------------------------
    if 'data_quality' in df.columns:
        bad_quality = ~df['data_quality'].isin(['GOOD', 'PARTIAL'])
        bad_count = bad_quality.sum()
        flags |= bad_quality
        report["checks"]["bad_data_quality"] = int(bad_count)

    # ---------------------------------------------------------------
    # CHECK 3: Mandatory field null checks
    # ---------------------------------------------------------------
    mandatory = rules.get("mandatory_fields", [])
    for field in mandatory:
        if field in df.columns:
            null_mask = df[field].isna() | (df[field].astype(str).str.strip() == '')
            null_count = null_mask.sum()
            if null_count > 0:
                flags |= null_mask
                report["checks"][f"null_{field}"] = int(null_count)

    # ---------------------------------------------------------------
    # CHECK 4: Timestamp validation (must be positive, non-zero)
    # ---------------------------------------------------------------
    if 'timestamp' in df.columns:
        bad_ts = (df['timestamp'] <= 0) | df['timestamp'].isna()
        ts_count = bad_ts.sum()
        if ts_count > 0:
            flags |= bad_ts
            report["checks"]["invalid_timestamp"] = int(ts_count)

    # ---------------------------------------------------------------
    # CHECK 5: Future timestamp check (cannot be > 48 hours from now)
    # ---------------------------------------------------------------
    if 'timestamp' in df.columns:
        now_ms = int(pd.Timestamp.now('UTC').timestamp() * 1000)
        future_limit = now_ms + (48 * 3600 * 1000)  # 48 hours buffer
        future_mask = df['timestamp'] > future_limit
        future_count = future_mask.sum()
        if future_count > 0:
            flags |= future_mask
            report["checks"]["future_timestamps"] = int(future_count)

    # ---------------------------------------------------------------
    # CHECK 6: Numeric range validations
    # ---------------------------------------------------------------
    range_checks = rules.get("numeric_range_checks", {})
    out_of_range_total = 0
    for field, (lo, hi) in range_checks.items():
        if field in df.columns:
            numeric_col = pd.to_numeric(df[field], errors='coerce')
            oor_mask = (numeric_col < lo) | (numeric_col > hi)
            # Don't flag NaN as out of range — they're handled by dropout/partial quality
            oor_mask = oor_mask & numeric_col.notna()
            oor_count = oor_mask.sum()
            if oor_count > 0:
                flags |= oor_mask
                report["checks"][f"out_of_range_{field}"] = int(oor_count)
                out_of_range_total += oor_count
    report["checks"]["total_out_of_range"] = int(out_of_range_total)

    # ---------------------------------------------------------------
    # CHECK 7: Cross-field consistency validations
    # ---------------------------------------------------------------
    cross_checks = rules.get("cross_field_checks", [])
    for field_a, op, field_b in cross_checks:
        if field_a in df.columns and field_b in df.columns:
            col_a = pd.to_numeric(df[field_a], errors='coerce')
            col_b = pd.to_numeric(df[field_b], errors='coerce')
            both_valid = col_a.notna() & col_b.notna()
            if op == ">":
                violation = both_valid & (col_a <= col_b)
            elif op == "<":
                violation = both_valid & (col_a >= col_b)
            elif op == ">=":
                violation = both_valid & (col_a < col_b)
            else:
                continue
            v_count = violation.sum()
            if v_count > 0:
                flags |= violation
                report["checks"][f"cross_field_{field_a}_{op}_{field_b}"] = int(v_count)

    # ---------------------------------------------------------------
    # CHECK 8: Health status validation (must be one of valid statuses)
    # ---------------------------------------------------------------
    if 'health_status' in df.columns:
        valid_statuses = {'Healthy', 'Warning', 'Critical'}
        invalid_status = ~df['health_status'].isin(valid_statuses) & df['health_status'].notna()
        status_count = invalid_status.sum()
        if status_count > 0:
            flags |= invalid_status
            report["checks"]["invalid_health_status"] = int(status_count)

    # ---------------------------------------------------------------
    # APPLY FLAGS: Remove flagged records
    # ---------------------------------------------------------------
    clean_df = df[~flags].copy()
    report["records_removed"] = int(flags.sum())
    report["records_passed"] = len(clean_df)

    return clean_df, report


def process_staging_to_silver(input_dir, output_dir):
    components = ["compressor", "condenser", "evaporator", "expansion_valve"]
    os.makedirs(output_dir, exist_ok=True)

    all_reports = []

    for component in components:
        staging_path = os.path.join(input_dir, component)
        silver_path = os.path.join(output_dir, component)

        if not os.path.exists(staging_path):
            print(f"Skipping {component} - Staging data not found.")
            continue

        print(f"\nProcessing Staging -> Silver for {component}...")
        print("=" * 60)

        try:
            # Read staging delta table
            dt = DeltaTable(staging_path)
            df = dt.to_pandas()

            rules = QUALITY_RULES.get(component, {})
            clean_df, report = run_quality_checks(df, component, rules)
            all_reports.append(report)

            # Print quality report
            print(f"  Initial records:  {report['initial_records']}")
            print(f"  Records removed:  {report['records_removed']}")
            print(f"  Records passed:   {report['records_passed']}")
            if report['checks']:
                print(f"  Quality check details:")
                for check_name, count in report['checks'].items():
                    if count > 0:
                        print(f"    - {check_name}: {count}")

            # Write to silver delta table
            write_deltalake(
                silver_path,
                clean_df,
                mode="overwrite",
                partition_by=["date"] if "date" in clean_df.columns else None
            )
            print(f"  Successfully wrote {len(clean_df)} records to Silver: {component}")
        except Exception as e:
            print(f"Error processing {component}: {e}")
            import traceback
            traceback.print_exc()

    # Print summary
    print("\n" + "=" * 60)
    print("STAGING -> SILVER QUALITY SUMMARY")
    print("=" * 60)
    total_in = sum(r['initial_records'] for r in all_reports)
    total_out = sum(r['records_passed'] for r in all_reports)
    total_removed = sum(r['records_removed'] for r in all_reports)
    print(f"  Total records in:      {total_in}")
    print(f"  Total records passed:  {total_out}")
    print(f"  Total records removed: {total_removed}")
    if total_in > 0:
        print(f"  Pass rate:             {total_out / total_in * 100:.2f}%")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    process_staging_to_silver(args.input_dir, args.output_dir)
