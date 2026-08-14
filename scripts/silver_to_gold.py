import os
import argparse
import json
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, year, quarter, month, dayofmonth, dayofweek, hour, 
    sum as _sum, count, max as _max, min as _min, mean as _mean,
    when, countDistinct, from_unixtime, to_date, lag, collect_list,
    struct
)
from delta.tables import DeltaTable
from spark_utils import get_spark_session, ensure_dir, path_exists
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
def cfg_path(*parts):
    return os.path.join(BASE_DIR, '..', *parts)

CRITICAL_CODES_PATH = cfg_path('config', 'critical_reading_codes.json')
def safe_merge(spark, target_path, df, merge_condition, partition_by=None):
    if DeltaTable.isDeltaTable(spark, target_path):
        target_table = DeltaTable.forPath(spark, target_path)
        target_table.alias("t").merge(
            df.alias("s"),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        writer = df.write.format("delta").mode("append")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(target_path)

def process_silver_to_gold(input_dir, output_dir, start_date=None, end_date=None):
    ensure_dir(output_dir)
    spark = get_spark_session("HVAC_Silver_to_Gold")
        
    try:
        # 1. GENERATE STATIC DIMENSION TABLES
        print("Generating Gold Dimension Tables...")
        
        with open(cfg_path('config', 'topology', 'customers.json'), 'r') as f:
            df_company = spark.createDataFrame(json.load(f))
            df_company = df_company.withColumnRenamed("companyId", "company_id") \
                                   .withColumnRenamed("companyName", "company_name") \
                                   .withColumnRenamed("accountType", "account_type")
            safe_merge(spark, os.path.join(output_dir, "dim_company"), df_company, "t.company_id = s.company_id")

        with open(cfg_path('config', 'topology', 'locations.json'), 'r') as f:
            df_location = spark.createDataFrame(json.load(f))
            df_location = df_location.withColumnRenamed("locationId", "location_id") \
                                     .withColumnRenamed("companyId", "company_id") \
                                     .withColumnRenamed("locationName", "location_name")
            safe_merge(spark, os.path.join(output_dir, "dim_location"), df_location, "t.location_id = s.location_id")

        with open(cfg_path('config', 'topology', 'hvac_machines.json'), 'r') as f:
            df_machine = spark.createDataFrame(json.load(f))
            df_machine = df_machine.withColumnRenamed("hvacMachineId", "hvac_machine_id") \
                                   .withColumnRenamed("locationId", "location_id")
            safe_merge(spark, os.path.join(output_dir, "dim_hvac_machine"), df_machine, "t.hvac_machine_id = s.hvac_machine_id")

        try:
            with open(CRITICAL_CODES_PATH, 'r') as f:
                critical_map = json.load(f)
        except Exception as e:
            print(f"Failed to load critical reading codes: {e}")
            return
            
        fault_rows = []
        for code, info in critical_map["codes"].items():
            fault_rows.append({
                'fault_code_key': int(code),
                'fault_code': str(code),
                'component_type': info.get('component_type', ''),
                'fault_description': info.get('label', ''),
                'threshold_description': info.get('description', ''),
                'severity': info.get('severity', ''),
                'recommended_action': info.get('recommended_action', ''),
                'is_active': True
            })
        if fault_rows:
            df_fault = spark.createDataFrame(fault_rows)
            safe_merge(spark, os.path.join(output_dir, "dim_fault_code"), df_fault, "t.fault_code_key = s.fault_code_key")

        # 2. READ SILVER TELEMETRY DATA
        components = ["compressor", "condenser", "evaporator", "expansion_valve"]
        silver_dfs = {}
        for comp in components:
            silver_path = os.path.join(input_dir, comp)
            if path_exists(spark, silver_path) and DeltaTable.isDeltaTable(spark, silver_path):
                df = spark.read.format("delta").load(silver_path)
                if start_date:
                    df = df.filter(to_date(col("date")) >= to_date(lit(start_date)))
                if end_date:
                    df = df.filter(to_date(col("date")) <= to_date(lit(end_date)))
                if not df.rdd.isEmpty():
                    df = df.withColumn("component_type", lit(comp))
                    df = df.withColumn("hour", hour(from_unixtime(col("timestamp") / 1000)))
                    silver_dfs[comp] = df

        if not silver_dfs:
            print("No silver telemetry data found in specified date range.")
            return

        # Create combined DataFrame using common columns
        # To concatenate in PySpark safely, we align columns
        from functools import reduce
        
        # Get all unique columns
        all_cols = set()
        for df in silver_dfs.values():
            all_cols.update(df.columns)
            
        aligned_dfs = []
        for df in silver_dfs.values():
            for c in all_cols:
                if c not in df.columns:
                    df = df.withColumn(c, lit(None))
            # Sort columns so unionByName works cleanly or just use unionByName(allowMissingColumns=True) in newer spark
            aligned_dfs.append(df)
            
        combined_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), aligned_dfs)
        combined_df.cache()

        # 3. DYNAMIC DIMENSIONS
        
        # dim_component
        comp_cols = ['component_id', 'hvac_machine_id', 'component_type', 'component_manufacturer', 'component_model', 'component_serial_number', 'component_installation_date']
        cols_present = [c for c in comp_cols if c in combined_df.columns]
        if cols_present:
            dim_comp = combined_df.select(*cols_present).dropDuplicates(["component_id"])
            # In a real SCD2 we would use a different merge condition, but for now we upsert
            safe_merge(spark, os.path.join(output_dir, "dim_component"), dim_comp, "t.component_id = s.component_id")
            
        # dim_date
        new_dates = combined_df.select("date").dropDuplicates()
        dim_date = new_dates.withColumn("year", year(col("date"))) \
                            .withColumn("quarter", quarter(col("date"))) \
                            .withColumn("month", month(col("date"))) \
                            .withColumn("day", dayofmonth(col("date"))) \
                            .withColumn("day_of_week", dayofweek(col("date"))) \
                            .withColumn("is_weekend", when(dayofweek(col("date")).isin([1, 7]), True).otherwise(False))
        safe_merge(spark, os.path.join(output_dir, "dim_date"), dim_date, "t.date = s.date")

        # 4. CROSS-COMPONENT FACTS
        
        # State transitions for event counts
        windowSpec = Window.partitionBy("component_id").orderBy("timestamp")
        combined_df = combined_df.withColumn("is_faulty", col("health_status").isin("Critical", "Warning"))
        combined_df = combined_df.withColumn("prev_faulty", lag("is_faulty").over(windowSpec))
        combined_df = combined_df.withColumn(
            "is_new_event", 
            when(
                (col("is_faulty")) & 
                (col("prev_faulty").isNull() | (col("prev_faulty") == False)), 
                1
            ).otherwise(0)
        )
        
        # health_daily
        health_daily = combined_df.groupBy("date", "location_id", "hvac_machine_id", "component_id", "component_type").agg(
            count("timestamp").alias("total_readings"),
            _sum(when(col("health_status") == "Critical", 1).otherwise(0)).alias("critical_readings"),
            _sum(when(col("health_status") == "Warning", 1).otherwise(0)).alias("warning_readings"),
            _sum("is_new_event").alias("critical_event_count")
        )
        health_daily = health_daily.withColumn("health_score_pct", 
            when(col("total_readings") > 0, 100.0 - (col("critical_readings") / col("total_readings") * 100.0) - (col("warning_readings") / col("total_readings") * 50.0)).otherwise(100.0)
        )
        health_daily = health_daily.withColumn("health_score_pct", when(col("health_score_pct") < 0, 0.0).otherwise(col("health_score_pct")))
        safe_merge(spark, os.path.join(output_dir, "fact_component_health_daily"), health_daily, 
            "t.date = s.date AND t.component_id = s.component_id", partition_by=["date"])

        # health_hourly
        health_hourly = combined_df.groupBy("date", "hour", "location_id", "hvac_machine_id", "component_id", "component_type").agg(
            count("timestamp").alias("total_readings"),
            _sum(when(col("health_status") == "Critical", 1).otherwise(0)).alias("critical_readings"),
            _sum(when(col("health_status") == "Warning", 1).otherwise(0)).alias("warning_readings")
        )
        health_hourly = health_hourly.withColumn("health_score_pct", 
            when(col("total_readings") > 0, 100.0 - (col("critical_readings") / col("total_readings") * 100.0) - (col("warning_readings") / col("total_readings") * 50.0)).otherwise(100.0)
        )
        health_hourly = health_hourly.withColumn("health_score_pct", when(col("health_score_pct") < 0, 0.0).otherwise(col("health_score_pct")))
        safe_merge(spark, os.path.join(output_dir, "fact_component_health_hourly"), health_hourly, 
            "t.date = s.date AND t.hour = s.hour AND t.component_id = s.component_id", partition_by=["date"])

        # Deriving Interval Dynamically (Bug C1)
        # We calculate the interval in hours by dividing duration by number of records
        energy_df = combined_df.filter(col("power_consumption_kw").isNotNull())
        energy_daily = energy_df.groupBy("date", "location_id", "hvac_machine_id", "component_id", "component_type").agg(
            _mean("power_consumption_kw").alias("avg_power_kw"),
            _max("power_consumption_kw").alias("max_power_kw"),
            _min("power_consumption_kw").alias("min_power_kw"),
            count("power_consumption_kw").alias("total_readings"),
            ((_max("timestamp") - _min("timestamp")) / 1000 / 3600).alias("duration_hours")
        )
        # Handle single record edge case / missing duration
        energy_daily = energy_daily.withColumn("duration_hours", 
            when((col("duration_hours").isNotNull()) & (col("duration_hours") > 0), col("duration_hours")).otherwise(lit(5.0/60.0))
        )
        energy_daily = energy_daily.withColumn("daily_energy_kwh", col("avg_power_kw") * col("duration_hours"))
        safe_merge(spark, os.path.join(output_dir, "fact_energy_consumption_daily"), energy_daily, 
            "t.date = s.date AND t.component_id = s.component_id", partition_by=["date"])

        energy_hourly = energy_df.groupBy("date", "hour", "location_id", "hvac_machine_id", "component_id", "component_type").agg(
            _mean("power_consumption_kw").alias("avg_power_kw"),
            _max("power_consumption_kw").alias("max_power_kw"),
            _min("power_consumption_kw").alias("min_power_kw"),
            count("power_consumption_kw").alias("total_readings"),
            ((_max("timestamp") - _min("timestamp")) / 1000 / 3600).alias("duration_hours")
        )
        energy_hourly = energy_hourly.withColumn("duration_hours", 
            when((col("duration_hours").isNotNull()) & (col("duration_hours") > 0), col("duration_hours")).otherwise(lit(5.0/60.0))
        )
        energy_hourly = energy_hourly.withColumn("hourly_energy_kwh", col("avg_power_kw") * col("duration_hours"))
        safe_merge(spark, os.path.join(output_dir, "fact_energy_consumption_hourly"), energy_hourly, 
            "t.date = s.date AND t.hour = s.hour AND t.component_id = s.component_id", partition_by=["date"])

        alerts_daily = combined_df.groupBy("date", "location_id", "hvac_machine_id").agg(
            _sum(when(col("health_status") == "Critical", 1).otherwise(0)).alias("total_critical_alerts"),
            _sum(when(col("health_status") == "Warning", 1).otherwise(0)).alias("total_warning_alerts"),
            countDistinct("health_active_fault_code").alias("unique_fault_codes")
        )
        safe_merge(spark, os.path.join(output_dir, "fact_machine_alerts_daily"), alerts_daily, 
            "t.date = s.date AND t.hvac_machine_id = s.hvac_machine_id", partition_by=["date"])

        alerts_hourly = combined_df.groupBy("date", "hour", "location_id", "hvac_machine_id").agg(
            _sum(when(col("health_status") == "Critical", 1).otherwise(0)).alias("total_critical_alerts"),
            _sum(when(col("health_status") == "Warning", 1).otherwise(0)).alias("total_warning_alerts"),
            countDistinct("health_active_fault_code").alias("unique_fault_codes")
        )
        safe_merge(spark, os.path.join(output_dir, "fact_machine_alerts_hourly"), alerts_hourly, 
            "t.date = s.date AND t.hour = s.hour AND t.hvac_machine_id = s.hvac_machine_id", partition_by=["date"])

        # Performance metrics
        if "compressor" in silver_dfs:
            comp_df = silver_dfs["compressor"]
            perf_daily = comp_df.groupBy("date", "location_id", "hvac_machine_id").agg(
                _max("run_hours").alias("max_run_hours"),
                _max("start_stop_count").alias("max_start_stops"),
                _mean("cop").alias("avg_cop"),
                _mean("eer").alias("avg_eer")
            )
            safe_merge(spark, os.path.join(output_dir, "fact_machine_performance_daily"), perf_daily, 
                "t.date = s.date AND t.hvac_machine_id = s.hvac_machine_id", partition_by=["date"])

        # 5. COMPONENT SPECIFIC
        def gen_metrics(df, comp_type, metrics):
            valid_metrics = [m for m in metrics if m in df.columns]
            if not valid_metrics: return
            
            aggs = []
            for m in valid_metrics:
                aggs.append(_mean(m).alias(f"avg_{m}"))
                aggs.append(_max(m).alias(f"max_{m}"))
            
            c_daily = df.groupBy("date", "location_id", "hvac_machine_id", "component_id").agg(*aggs)
            safe_merge(spark, os.path.join(output_dir, f"fact_{comp_type}_metrics_daily"), c_daily, 
                "t.date = s.date AND t.component_id = s.component_id", partition_by=["date"])
                
            c_hourly = df.groupBy("date", "hour", "location_id", "hvac_machine_id", "component_id").agg(*aggs)
            safe_merge(spark, os.path.join(output_dir, f"fact_{comp_type}_metrics_hourly"), c_hourly, 
                "t.date = s.date AND t.hour = s.hour AND t.component_id = s.component_id", partition_by=["date"])

        if "compressor" in silver_dfs:
            gen_metrics(silver_dfs["compressor"], "compressor", ['vibration_mm_s', 'suction_temperature_c', 'discharge_temperature_c', 'suction_pressure_kpa', 'discharge_pressure_kpa'])
        if "condenser" in silver_dfs:
            gen_metrics(silver_dfs["condenser"], "condenser", ['water_inlet_temperature_c', 'water_outlet_temperature_c', 'fan_speed_rpm', 'heat_rejection_efficiency_pct', 'approach_temperature_c'])
        if "evaporator" in silver_dfs:
            gen_metrics(silver_dfs["evaporator"], "evaporator", ['cooling_capacity_tr', 'entering_chilled_water_temperature_c', 'leaving_chilled_water_temperature_c', 'heat_transfer_efficiency_pct'])
        if "expansion_valve" in silver_dfs:
            gen_metrics(silver_dfs["expansion_valve"], "expansion_valve", ['valve_opening_pct', 'superheat_c', 'subcooling_c', 'refrigerant_flow_rate_kg_min'])

        # 6. KPI ROLLUPS
        
        # Bug C3: machines_online based on run_status > 0
        if "compressor" in silver_dfs:
            online_df = silver_dfs["compressor"].groupBy("date", "hvac_machine_id").agg(_max("run_status").alias("is_online"))
            machines_online = online_df.filter(col("is_online") > 0).groupBy("date").agg(count("hvac_machine_id").alias("machines_online"))
        else:
            machines_online = combined_df.select("date", "hvac_machine_id").dropDuplicates().groupBy("date").agg(count("hvac_machine_id").alias("machines_online"))
            
        machine_health = health_daily.groupBy("date", "location_id", "hvac_machine_id").agg(
            _sum("total_readings").alias("total_readings"),
            _sum("critical_readings").alias("critical_readings"),
            _sum("warning_readings").alias("warning_readings")
        )
        machine_health = machine_health.withColumn("machine_health_score", 
            when(col("total_readings") > 0, 100.0 - (col("critical_readings") / col("total_readings") * 100.0) - (col("warning_readings") / col("total_readings") * 50.0)).otherwise(100.0)
        )
        machine_health = machine_health.withColumn("machine_health_score", when(col("machine_health_score") < 0, 0.0).otherwise(col("machine_health_score")))

        fleet_summary = combined_df.groupBy("date").agg(
            countDistinct("location_id").alias("total_locations"),
            countDistinct("hvac_machine_id").alias("total_machines")
        ).join(machines_online, on="date", how="left")
        
        fleet_health_agg = machine_health.groupBy("date").agg(
            _sum(when(col("machine_health_score") >= 90, 1).otherwise(0)).alias("healthy_count"),
            _sum(when((col("machine_health_score") >= 70) & (col("machine_health_score") < 90), 1).otherwise(0)).alias("warning_count"),
            _sum(when(col("machine_health_score") < 70, 1).otherwise(0)).alias("critical_count"),
            _mean("machine_health_score").alias("avg_health_score")
        )
        fleet_summary = fleet_summary.join(fleet_health_agg, on="date", how="left")
        
        fleet_energy_agg = energy_daily.groupBy("date").agg(_sum("daily_energy_kwh").alias("total_energy_kwh"))
        fleet_summary = fleet_summary.join(fleet_energy_agg, on="date", how="left")
        
        if "compressor" in silver_dfs:
            fleet_perf_agg = silver_dfs["compressor"].groupBy("date").agg(_mean("cop").alias("avg_cop"))
            fleet_summary = fleet_summary.join(fleet_perf_agg, on="date", how="left")
            
        fleet_alerts_agg = alerts_daily.groupBy("date").agg((_sum("total_critical_alerts") + _sum("total_warning_alerts")).alias("active_fault_count"))
        fleet_summary = fleet_summary.join(fleet_alerts_agg, on="date", how="left").fillna(0)
        
        safe_merge(spark, os.path.join(output_dir, "KPI_Rollups/kpi_fleet_summary_daily"), fleet_summary, "t.date = s.date", partition_by=["date"])

        combined_df.unpersist()
        print("Successfully generated all Gold tables.")

    except Exception as e:
        print(f"Error in Silver to Gold processing: {e}")
        import traceback
        traceback.print_exc()
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    args = parser.parse_args()
    process_silver_to_gold(args.input_dir, args.output_dir, args.start_date, args.end_date)
