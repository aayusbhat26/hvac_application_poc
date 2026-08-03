"""
scripts/generate_batch_data.py

Generates realistic batch telemetry files for the HVAC fleet matching the schemas in 
`schema/raw/*.json` (compressor, condenser, evaporator, expansion_valve).

This is a batch data generator, not a streaming simulator. It models how edge gateways 
actually dump telemetry in real-world systems:

    - Edge gateways buffer sensor readings locally all day (every 5 mins).
    - At the end of the day, it dumps one batch JSON file per device into S3.
    - The file contains a metadata envelope (ingestionMode=BATCH, sourceFileName, 
      ingestionChannel, batch window, deviceId) plus an array of raw readings.
    - Values are delivered as stringified numbers and 13-digit epoch-millisecond 
      timestamps (as strings), exactly how real PLCs/sensors send them.
    - Includes real-world quirks: occasional dropped fields (missing data), 
      random-walk drifts for continuous values, and multi-day persistent faults 
      so downstream pipeline logic (like consecutive_days_reproduced) can test real scenarios.
    - Time-Variant Metadata: Simulates hardware swaps (SCD2). If you generate data 
      past a specific date, a device ID will change, proving Point-in-Time accuracy.
    - Fleet Manifest: Dumps a metadata file detailing the exact fleet topology 
      (plants, machines, device counts) for that specific day.

Outputs files locally under:
    batch_output/<component_type>/<device_id>/<YYYY-MM-DD>.json
    batch_output/metadata/fleet_manifest_<YYYYMMDD>.json
    
"""

import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

# --------------------------------------------------------------------------
# 1. TIME-VARIANT FLEET TOPOLOGY (Metadata & Asset Registry)
# --------------------------------------------------------------------------
def build_fleet(target_date, num_plants=10, machines_per_plant=3):
    """
    Generates the fleet topology valid for a specific date.
    Simulates hardware swaps so the data warehouse SCD2 logic can be tested.
    """
    fleet = []
    
    # Simulate a historical hardware swap on a specific date
    # E.g., On March 15, 2026, Compressor COMP-0101 was replaced with COMP-9999
    swap_date = datetime(2026, 3, 15).date()

    for p in range(1, num_plants + 1):
        plant_id = f"PLANT-{p:02d}"
        for m in range(1, machines_per_plant + 1):
            hvac_machine_id = f"HVAC-{p:02d}-{m:02d}"
            
            # Default Device IDs
            comp_id = f"COMP-{p:02d}{m:02d}"
            cond_id = f"COND-{p:02d}{m:02d}"
            evap_id = f"EVAP-{p:02d}{m:02d}"
            exv_id = f"EXV-{p:02d}{m:02d}"
            
            # Point-in-Time Logic: Apply the swap if we are generating data after the swap date
            if target_date >= swap_date and hvac_machine_id == "HVAC-01-01":
                comp_id = "COMP-9999"  # The new replacement compressor
                
            fleet.append({
                "plantId": plant_id,
                "hvacMachineId": hvac_machine_id,
                "components": {
                    "compressor": comp_id,
                    "condenser": cond_id,
                    "evaporator": evap_id,
                    "expansion_valve": exv_id,
                },
            })
    return fleet

def export_fleet_manifest(fleet, output_dir, current_date):
    """Exports a static lookup JSON tracking exactly what hardware existed on this day."""
    manifest = {
        "snapshotDate": current_date.strftime("%Y-%m-%d"),
        "totalPlants": len(set(m["plantId"] for m in fleet)),
        "totalHvacMachines": len(fleet),
        "totalDevices": len(fleet) * 4,
        "breakdownByComponent": {
            "compressor": len(fleet),
            "condenser": len(fleet),
            "evaporator": len(fleet),
            "expansion_valve": len(fleet)
        },
        "fleetTopology": fleet
    }
    
    meta_dir = os.path.join(output_dir, "metadata")
    os.makedirs(meta_dir, exist_ok=True)
    manifest_path = os.path.join(meta_dir, f"fleet_manifest_{current_date.strftime('%Y%m%d')}.json")
    
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
        
    return manifest_path

# --------------------------------------------------------------------------
# 2. RAW-SCHEMA FIELD DEFINITIONS & FAULTS
# --------------------------------------------------------------------------

FAULT_CODES = {
    "compressor": ["CMP-HIGH-DISCH-TEMP", "CMP-LOW-OIL-PRESS", "CMP-HIGH-VIBRATION"],
    "condenser": ["CND-FAN-FAULT", "CND-HIGH-APPROACH-TEMP", "CND-LOW-WATER-FLOW"],
    "evaporator": ["EVP-FLOW-SWITCH-OPEN", "EVP-LOW-LWT", "EVP-PUMP-TRIP"],
    "expansion_valve": ["EXV-LOW-SUPERHEAT", "EXV-STUCK-VALVE", "EXV-HIGH-SUBCOOLING"],
}

COMPRESSOR_FIELDS = {
    "suctionPressureKpa": (450, 400, 500, 3.0),
    "dischargePressureKpa": (1200, 1050, 1400, 8.0),
    "oilPressureKpa": (300, 260, 340, 2.0),
    "suctionTemperatureC": (5.0, 1.0, 9.0, 0.3),
    "dischargeTemperatureC": (65.0, 55.0, 80.0, 0.6),
    "oilTemperatureC": (55.0, 45.0, 68.0, 0.4),
    "motorWindingTemperatureC": (78.0, 60.0, 100.0, 0.7),
    "voltageV": (415.0, 400.0, 425.0, 0.8),
    "currentA": (34.0, 26.0, 44.0, 0.5),
    "powerConsumptionKw": (45.0, 32.0, 58.0, 1.0),
    "frequencyHz": (50.0, 49.4, 50.6, 0.05),
    "powerFactor": (0.91, 0.82, 0.97, 0.01),
    "rpm": (2900, 2750, 3000, 12),
    "vibrationMmS": (2.0, 0.8, 3.6, 0.15),
    "bearingTemperatureC": (62.0, 48.0, 82.0, 0.5),
    "eer": (11.0, 8.5, 13.5, 0.1),
    "cop": (3.7, 2.9, 4.6, 0.05),
}

CONDENSER_FIELDS = {
    "condenserPressureKpa": (1250, 1100, 1450, 8.0),
    "pressureDropKpa": (25, 10, 45, 1.0),
    "waterInletTemperatureC": (29.0, 25.0, 34.0, 0.3),
    "waterOutletTemperatureC": (35.0, 30.0, 40.0, 0.3),
    "coolingTowerWaterTemperatureC": (27.0, 22.0, 32.0, 0.3),
    "ambientAirTemperatureC": (32.0, 24.0, 42.0, 0.5),
    "waterFlowRateGpm": (480, 400, 560, 4.0),
    "waterValvePositionPct": (72, 40, 100, 2.0),
    "fanSpeedRpm": (880, 700, 1000, 8.0),
    "fanCurrentA": (12.0, 8.0, 16.0, 0.3),
    "fanPowerKw": (5.5, 3.5, 7.5, 0.2),
    "voltageV": (415.0, 400.0, 425.0, 0.8),
    "frequencyHz": (50.0, 49.4, 50.6, 0.05),
    "powerFactor": (0.90, 0.80, 0.97, 0.01),
    "powerConsumptionKw": (14.0, 9.0, 19.0, 0.4),
    "approachTemperatureC": (3.5, 1.5, 6.5, 0.15),
    "heatRejectionEfficiencyPct": (88.0, 70.0, 96.0, 0.4),
}

EVAPORATOR_FIELDS = {
    "enteringChilledWaterTemperatureC": (12.0, 9.0, 15.0, 0.2),
    "leavingChilledWaterTemperatureC": (7.0, 5.0, 10.0, 0.2),
    "temperatureDifferenceC": (5.0, 3.0, 7.0, 0.15),
    "evaporatorPressureKpa": (380, 330, 430, 3.0),
    "waterFlowRateGpm": (520, 440, 600, 4.0),
    "lwtSetpointC": (7.0, 6.5, 7.5, 0.02),
    "pumpPowerConsumptionKw": (18.0, 12.0, 24.0, 0.4),
    "pumpCurrentA": (28.0, 20.0, 36.0, 0.4),
    "pumpFrequencyHz": (50.0, 49.4, 50.6, 0.05),
    "coolingCapacityTr": (185.0, 140.0, 220.0, 1.5),
    "heatTransferEfficiencyPct": (90.0, 75.0, 97.0, 0.4),
}

EXPANSION_VALVE_FIELDS = {
    "valveOpeningPct": (45, 20, 80, 1.5),
    "liquidLineTemperatureC": (30.0, 25.0, 36.0, 0.3),
    "evaporatorOutletTemperatureC": (8.0, 5.0, 12.0, 0.2),
    "superheatC": (6.0, 3.0, 10.0, 0.25),
    "subcoolingC": (5.0, 2.0, 9.0, 0.2),
    "inletPressureKpa": (1150, 1000, 1350, 6.0),
    "outletPressureKpa": (400, 350, 460, 3.0),
    "pressureDropKpa": (750, 600, 900, 5.0),
    "refrigerantFlowRateKgMin": (18.0, 12.0, 24.0, 0.3),
    "actuatorVoltageV": (24.0, 22.0, 26.0, 0.1),
    "actuatorCurrentA": (0.4, 0.2, 0.8, 0.02),
    "powerConsumptionW": (9.5, 5.0, 14.0, 0.3),
    "targetSuperheatC": (6.0, 6.0, 6.0, 0.0), 
}

FIELD_SETS = {
    "compressor": COMPRESSOR_FIELDS,
    "condenser": CONDENSER_FIELDS,
    "evaporator": EVAPORATOR_FIELDS,
    "expansion_valve": EXPANSION_VALVE_FIELDS,
}

INT_FIELDS = {"rpm", "fanSpeedRpm", "waterValvePositionPct", "valveStepPosition", "responseTimeMs"}

# --------------------------------------------------------------------------
# 3. RANDOM-WALK STATE
# --------------------------------------------------------------------------

def init_state(component_type, rng):
    """Seed a device's starting values + cumulative counters + fault state."""
    fields = FIELD_SETS[component_type]
    values = {name: base for name, (base, _lo, _hi, _std) in fields.items()}
    return {
        "values": values,
        "run_hours": rng.uniform(8000, 15000),
        "start_stop_count": rng.randint(200, 600),
        "valve_step_position": rng.randint(800, 1200),
        "fault_active": False,
        "fault_code": None,
        "fault_days_remaining": 0,
    }

def walk(value, lo, hi, std, rng):
    new_value = value + rng.gauss(0, std)
    return max(lo, min(hi, new_value))

def maybe_start_fault(component_type, state, rng, daily_fault_probability):
    if state["fault_active"]:
        return
    if rng.random() < daily_fault_probability:
        state["fault_active"] = True
        state["fault_code"] = rng.choice(FAULT_CODES[component_type])
        state["fault_days_remaining"] = rng.randint(2, 5)

def tick_fault_day(state):
    if state["fault_active"]:
        state["fault_days_remaining"] -= 1
        if state["fault_days_remaining"] <= 0:
            state["fault_active"] = False
            state["fault_code"] = None

def apply_fault_bias(component_type, state, values):
    if not state["fault_active"]:
        return
    if component_type == "compressor":
        values["vibrationMmS"] = min(6.0, values["vibrationMmS"] * 1.6)
        values["dischargeTemperatureC"] = min(95.0, values["dischargeTemperatureC"] + 8)
    elif component_type == "condenser":
        values["approachTemperatureC"] = min(9.0, values["approachTemperatureC"] + 3)
        values["heatRejectionEfficiencyPct"] = max(55.0, values["heatRejectionEfficiencyPct"] - 12)
    elif component_type == "evaporator":
        values["temperatureDifferenceC"] = max(1.0, values["temperatureDifferenceC"] - 2)
        values["heatTransferEfficiencyPct"] = max(55.0, values["heatTransferEfficiencyPct"] - 10)
    elif component_type == "expansion_valve":
        values["superheatC"] = max(0.5, values["superheatC"] - 3.5)
        values["subcoolingC"] = min(12.0, values["subcoolingC"] + 3)

# --------------------------------------------------------------------------
# 4. READING GENERATION
# --------------------------------------------------------------------------

def stringify(field_name, value):
    # FIXED: Removed 'and False' so ints are properly cast
    if field_name in INT_FIELDS or field_name in ("startStopCount", "runHours"):
        return str(int(round(value)))
    if isinstance(value, int):
        return str(value)
    return str(round(value, 6))

def status_fields(component_type, rng, fault_active):
    if component_type == "compressor":
        return {"compressorStatus": "OFF" if rng.random() < 0.02 else "ON"}
    if component_type == "condenser":
        return {
            "condenserStatus": "ON",
            "fanStatus": "OFF" if (fault_active and rng.random() < 0.3) else "ON",
        }
    if component_type == "evaporator":
        return {
            "evaporatorStatus": "ON",
            "flowSwitchStatus": "OPEN" if (fault_active and rng.random() < 0.2) else "CLOSED",
            "pumpControlMode": "AUTO",
            "pumpRelay1Status": "ON",
            "pumpRelay2Status": "OFF",
        }
    if component_type == "expansion_valve":
        return {"valveStatus": "ACTIVE", "controlMode": "AUTO"}
    return {}

# FIXED: Added interval_minutes to the parameter list
def generate_reading(component_type, state, ts_ms, rng, dropout_probability, interval_minutes):
    fields = FIELD_SETS[component_type]
    values = state["values"]

    for name, (_base, lo, hi, std) in fields.items():
        values[name] = walk(values[name], lo, hi, std, rng)

    apply_fault_bias(component_type, state, values)

    # FIXED: Use actual interval_minutes instead of hardcoded 5
    state["run_hours"] += interval_minutes / 60.0 
    if rng.random() < 0.001:
        state["start_stop_count"] += 1
    if component_type == "expansion_valve":
        state["valve_step_position"] += rng.randint(-2, 2)

    reading = {
        "eventId": str(uuid.uuid4()),
        "timestamp": str(ts_ms),
    }
    reading.update(status_fields(component_type, rng, state["fault_active"]))
    reading["runHours"] = stringify("runHours", state["run_hours"])
    
    if component_type == "compressor":
        reading["startStopCount"] = str(state["start_stop_count"])
    if component_type == "expansion_valve":
        reading["valveStepPosition"] = str(state["valve_step_position"])
        reading["targetSuperheatC"] = stringify("targetSuperheatC", values["targetSuperheatC"])
        reading["responseTimeMs"] = str(rng.randint(180, 420))

    for name in fields:
        reading[name] = stringify(name, values[name])

    reading["healthActiveFaultCode"] = state["fault_code"]
    reading["healthMaintenanceFlag"] = state["fault_active"]
    reading["healthStatus"] = (
        "Critical" if state["fault_active"] and state["fault_days_remaining"] <= 1
        else "Warning" if state["fault_active"]
        else "Healthy"
    )
    reading["dataQuality"] = "GOOD"

    droppable = list(fields.keys())
    for name in droppable:
        if rng.random() < dropout_probability:
            reading[name] = None
            reading["dataQuality"] = "PARTIAL"

    return reading

# --------------------------------------------------------------------------
# 5. BATCH FILE ASSEMBLY
# --------------------------------------------------------------------------

def generate_device_day_batch(component_type, device_id, plant_id, hvac_machine_id,
                               day, state, rng, interval_minutes, dropout_probability,
                               daily_fault_probability):
    maybe_start_fault(component_type, state, rng, daily_fault_probability)

    day_start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    ticks = (24 * 60) // interval_minutes

    records = []
    for i in range(ticks):
        ts = day_start + timedelta(minutes=i * interval_minutes)
        ts_ms = int(ts.timestamp() * 1000)
        records.append(
            # FIXED: Passing interval_minutes to generate_reading
            generate_reading(component_type, state, ts_ms, rng, dropout_probability, interval_minutes)
        )

    tick_fault_day(state) 

    batch_id = f"batch-{device_id}-{day.strftime('%Y%m%d')}"
    batch = {
        "ingestionMode": "BATCH",
        "uploadedBy": f"edge-gateway-{plant_id.lower()}",
        "uploadTimestamp": str(int((day_start + timedelta(days=1)).timestamp() * 1000)),
        "sourceFileName": f"{component_type}_{device_id}_{day.strftime('%Y%m%d')}.json",
        "sourceSystem": "BMS-EdgeCollector-v2.3",
        "ingestionChannel": "S3_UPLOAD",
        "batchId": batch_id,
        "componentId": device_id,
        "componentType": component_type,
        "hvacMachineId": hvac_machine_id,
        "plantId": plant_id,
        "batchStartTime": str(int(day_start.timestamp() * 1000)),
        "batchEndTime": str(int((day_start + timedelta(days=1)).timestamp() * 1000)),
        "readingIntervalSeconds": interval_minutes * 60,
        "recordCount": len(records),
        "records": records,
    }
    return batch

# --------------------------------------------------------------------------
# 6. DRIVER
# --------------------------------------------------------------------------

def run(output_dir, start_date, num_days, interval_minutes, dropout_probability,
        daily_fault_probability, seed, num_plants=10, machines_per_plant=3):
    rng = random.Random(seed)
    
    device_states = {}
    files_written = []
    manifests_written = []

    for day_offset in range(num_days):
        day = start_date + timedelta(days=day_offset)
        
        # Get the accurate topology for THIS specific day (handles mid-run hardware swaps)
        daily_fleet = build_fleet(day.date(), num_plants, machines_per_plant)
        
        # Dump the manifest metadata for this day
        manifest_path = export_fleet_manifest(daily_fleet, output_dir, day.date())
        manifests_written.append(manifest_path)

        for machine in daily_fleet:
            plant_id = machine["plantId"]
            hvac_machine_id = machine["hvacMachineId"]
            
            for component_type, device_id in machine["components"].items():
                
                # If a device ID was swapped mid-run, it won't be in state yet; initialize it
                if device_id not in device_states:
                    device_states[device_id] = init_state(component_type, rng)
                
                state = device_states[device_id]
                batch = generate_device_day_batch(
                    component_type, device_id, plant_id, hvac_machine_id,
                    day, state, rng, interval_minutes, dropout_probability,
                    daily_fault_probability,
                )

                out_path = os.path.join(
                    output_dir, component_type, device_id, f"{day.strftime('%Y-%m-%d')}.json"
                )
                os.makedirs(os.path.dirname(out_path), exist_ok=True)
                with open(out_path, "w") as f:
                    json.dump(batch, f, indent=2)
                files_written.append(out_path)

    return files_written, manifests_written

def main():
    parser = argparse.ArgumentParser(description="Generate realistic HVAC batch telemetry files.")
    parser.add_argument("--output-dir", default="batch_output")
    parser.add_argument("--start-date", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                         help="YYYY-MM-DD, first day of the batch window")
    parser.add_argument("--days", type=int, default=3, help="number of days to generate")
    parser.add_argument("--interval-minutes", type=int, default=5,
                         help="reading frequency within each day's batch")
    parser.add_argument("--dropout-probability", type=float, default=0.015,
                         help="chance any given sensor field is missing on a reading")
    parser.add_argument("--daily-fault-probability", type=float, default=0.10,
                         help="chance a healthy device develops a new fault on a given day")
    parser.add_argument("--plants", type=int, default=10, help="number of plants in the fleet")
    parser.add_argument("--machines-per-plant", type=int, default=3,
                         help="HVAC machines per plant; also sets device counts.")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    
    files, manifests = run(
        output_dir=args.output_dir,
        start_date=start_date,
        num_days=args.days,
        interval_minutes=args.interval_minutes,
        dropout_probability=args.dropout_probability,
        daily_fault_probability=args.daily_fault_probability,
        seed=args.seed,
        num_plants=args.plants,
        machines_per_plant=args.machines_per_plant,
    )

    total_devices = args.plants * args.machines_per_plant * 4
    print(f"Fleet: {args.plants} plants x {args.machines_per_plant} machines/plant "
          f"= {args.plants * args.machines_per_plant} machines, {total_devices} devices total.")
    
    print(f"\nWrote {len(manifests)} metadata manifest files to '{args.output_dir}/metadata/':")
    for path in manifests[:3]:
        print(f"  {path}")
        
    print(f"\nWrote {len(files)} raw batch telemetry files to '{args.output_dir}/':")
    for path in files[:5]:
        print(f"  {path}")
    if len(files) > 5:
        print(f"  ... and {len(files) - 5} more")

if __name__ == "__main__":
    main()