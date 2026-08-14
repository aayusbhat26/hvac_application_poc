import os
import argparse
import time
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, lit, when, row_number, concat_ws, current_timestamp, to_date
from pyspark.sql.types import DoubleType
from delta.tables import DeltaTable
from spark_utils import get_spark_session, ensure_dir, path_exists

QUALITY_RULES = {
    "compressor": {
        "mandatory_fields": ["event_id", "timestamp", "component_id", "hvac_machine_id", "location_id", "health_status"],
        "numeric_range_checks": {
            "suction_pressure_kpa":       (100, 800),     
            "discharge_pressure_kpa":     (500, 2500),
            "oil_pressure_kpa":           (50, 600),
            "suction_temperature_c":      (-20, 30),
            "discharge_temperature_c":    (30, 120),      
            "oil_temperature_c":          (10, 100),
            "motor_winding_temperature_c":(20, 150),
            "voltage_v":                  (350, 480),
            "current_a":                  (0, 100),
            "power_consumption_kw":       (0, 150),
            "frequency_hz":              (45, 65),
            "power_factor":              (0.5, 1.0),
            "rpm":                       (0, 5000),
            "vibration_mm_s":            (0, 20),         
            "bearing_temperature_c":     (10, 130),
            "eer":                       (2, 25),
            "cop":                       (1, 10),
        },
        "cross_field_checks": [
            ("discharge_pressure_kpa", ">", "suction_pressure_kpa", 50),
            ("discharge_temperature_c", ">", "suction_temperature_c", 5),
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
            ("water_outlet_temperature_c", ">", "water_inlet_temperature_c", 2),
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
            ("entering_chilled_water_temperature_c", ">", "leaving_chilled_water_temperature_c", 1),
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
            ("inlet_pressure_kpa", ">", "outlet_pressure_kpa", 50),
        ]
    },
}

def apply_quality_rules(df, rules):
    """
    Applies quality rules and adds 'is_valid' and 'failure_reason' columns.
    """
    df = df.withColumn("is_valid", lit(True))
    df = df.withColumn("failure_reason", lit(""))

    def flag_invalid(condition, reason):
        return df.withColumn(
            "failure_reason",
            when(condition & col("is_valid"), lit(reason))
            .otherwise(when(condition, concat_ws(" | ", col("failure_reason"), lit(reason)))
            .otherwise(col("failure_reason")))
        ).withColumn(
            "is_valid",
            when(condition, lit(False)).otherwise(col("is_valid"))
        )

    # 1. Deduplication flag
    windowSpec = Window.partitionBy("event_id").orderBy(col("timestamp").desc())
    df = df.withColumn("row_num", row_number().over(windowSpec))
    df = flag_invalid(col("row_num") > 1, "Duplicate event_id")
    df = df.drop("row_num")

    # 2. Data quality filter
    if "data_quality" in df.columns:
        df = flag_invalid(~col("data_quality").isin(["GOOD", "PARTIAL"]), "Bad data_quality")

    # 3. Mandatory fields
    for field in rules.get("mandatory_fields", []):
        if field in df.columns:
            df = flag_invalid(col(field).isNull() | (col(field).cast("string") == ""), f"Null mandatory field: {field}")

    # 4. Timestamp > 0
    if "timestamp" in df.columns:
        df = flag_invalid(col("timestamp").isNull() | (col("timestamp") <= 0), "Invalid timestamp")

    # 5. Future timestamp (with proper timezone buffer, using ms)
    if "timestamp" in df.columns:
        # 365 days in future (allows generating synthetic future data)
        future_limit = int(time.time() * 1000) + (365 * 24 * 3600 * 1000)
        df = flag_invalid(col("timestamp") > future_limit, "Future timestamp")

    # 6. Numeric ranges
    for field, (lo, hi) in rules.get("numeric_range_checks", {}).items():
        if field in df.columns:
            df = flag_invalid(
                col(field).isNotNull() & ((col(field).cast(DoubleType()) < lo) | (col(field).cast(DoubleType()) > hi)),
                f"Out of range: {field}"
            )

    # 7. Cross field checks (with buffer)
    for field_a, op, field_b, buffer in rules.get("cross_field_checks", []):
        if field_a in df.columns and field_b in df.columns:
            val_a = col(field_a).cast(DoubleType())
            val_b = col(field_b).cast(DoubleType())
            if op == ">":
                df = flag_invalid(val_a.isNotNull() & val_b.isNotNull() & (val_a + buffer <= val_b), f"Cross field error: {field_a} {op} {field_b}")
                
    # 8. Health status
    if "health_status" in df.columns:
        df = flag_invalid(col("health_status").isNotNull() & ~col("health_status").isin("Healthy", "Warning", "Critical"), "Invalid health_status")

    return df

def process_staging_to_silver(input_dir, output_dir, start_date=None, end_date=None):
    components = ["compressor", "condenser", "evaporator", "expansion_valve"]
    ensure_dir(output_dir)
    
    dlq_dir = os.path.join(output_dir, "dlq")
    ensure_dir(dlq_dir)

    spark = get_spark_session("HVAC_Staging_to_Silver")
        
    try:
        for component in components:
            staging_path = os.path.join(input_dir, component)
            silver_path = os.path.join(output_dir, component)
            dlq_path = os.path.join(dlq_dir, component)
            
            if not path_exists(spark, staging_path) or not DeltaTable.isDeltaTable(spark, staging_path):
                print(f"Skipping {component} - Staging data not found.")
                continue

            print(f"Processing Staging -> Silver for {component}...")
            df = spark.read.format("delta").load(staging_path)
            
            if start_date:
                df = df.filter(to_date(col("date")) >= to_date(lit(start_date)))
            if end_date:
                df = df.filter(to_date(col("date")) <= to_date(lit(end_date)))
                
            rules = QUALITY_RULES.get(component, {})
            df_checked = apply_quality_rules(df, rules)
            df_checked.cache()
            
            if df_checked.rdd.isEmpty():
                print(f"No records to process for {component}.")
                df_checked.unpersist()
                continue
                
            total_records = df_checked.count()
            df_valid = df_checked.filter(col("is_valid") == True).drop("is_valid", "failure_reason")
            df_invalid = df_checked.filter(col("is_valid") == False).withColumn("quarantine_timestamp", current_timestamp())
            
            valid_count = df_valid.count()
            invalid_count = df_invalid.count()
            
            print(f"  Total records: {total_records}")
            print(f"  Valid records: {valid_count}")
            print(f"  Invalid records (DLQ): {invalid_count}")
            
            # Write valid records to Silver
            if valid_count > 0:
                if DeltaTable.isDeltaTable(spark, silver_path):
                    target_table = DeltaTable.forPath(spark, silver_path)
                    target_table.alias("t").merge(
                        df_valid.alias("s"),
                        "t.event_id = s.event_id"
                    ).whenNotMatchedInsertAll().execute()
                else:
                    df_valid.write.format("delta").mode("append").partitionBy("date").save(silver_path)
                    
            # Write invalid records to DLQ
            if invalid_count > 0:
                if DeltaTable.isDeltaTable(spark, dlq_path):
                    dlq_table = DeltaTable.forPath(spark, dlq_path)
                    dlq_table.alias("t").merge(
                        df_invalid.alias("s"),
                        "t.event_id = s.event_id"
                    ).whenNotMatchedInsertAll().execute()
                else:
                    df_invalid.write.format("delta").mode("append").partitionBy("date").save(dlq_path)

            df_checked.unpersist()
            
    except Exception as e:
        print(f"Error processing Staging to Silver: {e}")
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
    process_staging_to_silver(args.input_dir, args.output_dir, args.start_date, args.end_date)
