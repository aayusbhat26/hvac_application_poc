import os
import sys
import json
import argparse
import subprocess
from datetime import datetime, timedelta, timezone
from huggingface_hub import hf_hub_download, HfApi
from huggingface_hub.utils import EntryNotFoundError, RepositoryNotFoundError, HfHubHTTPError

WATERMARK_FILENAME = "watermark.json"
LOCAL_WATERMARK_PATH = os.path.join("data", WATERMARK_FILENAME)

# ─── Helpers ────────────────────────────────────────────────────────────────────

def _empty_watermark():
    """Return a blank watermark structure."""
    return {
        "latest_date": None,
        "generated_dates": [],      # every individual date that has data
        "run_history": []           # audit trail of every workflow run
    }


def _download_watermark(bucket_id):
    """Try to download the watermark JSON from HF. Return dict or None."""
    try:
        path = hf_hub_download(
            repo_id=bucket_id,
            filename=WATERMARK_FILENAME,
            repo_type="bucket",
            force_download=True
        )
        with open(path, "r") as f:
            return json.load(f)
    except (EntryNotFoundError, RepositoryNotFoundError, HfHubHTTPError):
        # Try legacy watermark.txt (old format, single-line date)
        try:
            path = hf_hub_download(
                repo_id=bucket_id,
                filename="watermark.txt",
                repo_type="bucket",
                force_download=True
            )
            with open(path, "r") as f:
                date_str = f.readline().strip()
            if date_str:
                print(f"Found legacy watermark.txt with date: {date_str}")
                wm = _empty_watermark()
                wm["latest_date"] = date_str
                wm["generated_dates"] = [date_str]
                wm["run_history"] = [{"start": date_str, "end": date_str, "days": 1, "note": "migrated from legacy watermark.txt"}]
                return wm
        except Exception:
            pass
        return None
    except Exception as e:
        print(f"Warning: Unexpected error fetching watermark: {e}")
        return None


def _scan_bucket_for_dates(bucket_id):
    """
    Scan the HF bucket to discover which dates actually have data uploaded.
    This is the safety net: even if the watermark was never updated, we can
    reconstruct the true state from the raw data directories on HF.
    
    Directory structure: batch/raw/location_XX/YYYY/month/dayN/
    """
    discovered_dates = set()
    try:
        api = HfApi()
        # List top-level items under batch/raw/ to find location dirs
        entries = api.list_repo_tree(
            repo_id=bucket_id,
            path_in_repo="batch/raw",
            repo_type="bucket",
        )
        location_dirs = [e.path for e in entries if e.path.startswith("batch/raw/location_")]

        # For each location, list year dirs -> month dirs -> day dirs
        # We only need one location to discover all dates
        if location_dirs:
            loc = location_dirs[0]
            year_entries = api.list_repo_tree(repo_id=bucket_id, path_in_repo=loc, repo_type="bucket")
            for year_entry in year_entries:
                year_str = os.path.basename(year_entry.path)
                if not year_str.isdigit():
                    continue
                month_entries = api.list_repo_tree(repo_id=bucket_id, path_in_repo=year_entry.path, repo_type="bucket")
                for month_entry in month_entries:
                    month_name = os.path.basename(month_entry.path).lower()
                    month_map = {
                        "january": 1, "february": 2, "march": 3, "april": 4,
                        "may": 5, "june": 6, "july": 7, "august": 8,
                        "september": 9, "october": 10, "november": 11, "december": 12
                    }
                    month_num = month_map.get(month_name)
                    if month_num is None:
                        continue
                    day_entries = api.list_repo_tree(repo_id=bucket_id, path_in_repo=month_entry.path, repo_type="bucket")
                    for day_entry in day_entries:
                        day_name = os.path.basename(day_entry.path)  # e.g., "day1"
                        if day_name.startswith("day"):
                            try:
                                day_num = int(day_name.replace("day", ""))
                                dt = datetime(int(year_str), month_num, day_num)
                                discovered_dates.add(dt.strftime("%Y-%m-%d"))
                            except (ValueError, OverflowError):
                                continue
    except Exception as e:
        print(f"Warning: Could not scan bucket for existing dates: {e}")
    
    return sorted(discovered_dates)


def _upload_watermark(bucket_id):
    """Upload the local watermark JSON to the HF bucket. Non-fatal on failure."""
    try:
        result = subprocess.run(
            ["hf", "upload", bucket_id,
             LOCAL_WATERMARK_PATH, WATERMARK_FILENAME, "--repo-type", "bucket"],
            capture_output=True, text=True, timeout=60
        )
        if result.returncode == 0:
            print(f"Successfully uploaded {WATERMARK_FILENAME} to HF Bucket.")
            return True
        else:
            print(f"hf CLI upload failed: {result.stderr.strip()}")
            # Fallback to HfApi
            api = HfApi()
            api.upload_file(
                path_or_fileobj=LOCAL_WATERMARK_PATH,
                path_in_repo=WATERMARK_FILENAME,
                repo_id=bucket_id,
                repo_type="bucket"
            )
            print(f"Successfully uploaded {WATERMARK_FILENAME} via HfApi fallback.")
            return True
    except Exception as e:
        print(f"WARNING: Failed to upload watermark to HF: {e}")
        print("The data was synced successfully, but the watermark was not updated.")
        print("On the next run, the bucket will be scanned to recover the true state.")
        return False


# ─── Actions ────────────────────────────────────────────────────────────────────

def check_watermark(input_date_str, bucket_id, days):
    """Determine the next date range to generate, avoiding duplicates."""
    
    # 1. Try to load watermark from HF
    wm = _download_watermark(bucket_id)
    
    # 2. If watermark is missing or stale, scan the bucket for actual data
    if wm is None:
        print("No watermark found. Scanning bucket to discover existing data...")
        wm = _empty_watermark()
        bucket_dates = _scan_bucket_for_dates(bucket_id)
        if bucket_dates:
            wm["generated_dates"] = bucket_dates
            wm["latest_date"] = bucket_dates[-1]
            wm["run_history"].append({
                "start": bucket_dates[0], "end": bucket_dates[-1],
                "days": len(bucket_dates), "note": "reconstructed from bucket scan"
            })
            print(f"Discovered {len(bucket_dates)} dates in bucket. Latest: {bucket_dates[-1]}")
    else:
        # Even with a watermark, do a quick sanity check by scanning bucket
        # to catch the case where data was uploaded but watermark update failed
        print("Watermark found. Cross-checking with bucket data...")
        bucket_dates = _scan_bucket_for_dates(bucket_id)
        if bucket_dates:
            # Merge: add any dates found in bucket but missing from watermark
            existing_set = set(wm.get("generated_dates", []))
            new_dates = [d for d in bucket_dates if d not in existing_set]
            if new_dates:
                print(f"Found {len(new_dates)} date(s) in bucket not tracked by watermark: {new_dates}")
                wm["generated_dates"] = sorted(existing_set | set(bucket_dates))
                # Update latest_date if bucket has newer data
                bucket_latest = bucket_dates[-1]
                if wm["latest_date"] is None or bucket_latest > wm["latest_date"]:
                    wm["latest_date"] = bucket_latest
                    print(f"Updated latest_date to {bucket_latest} based on bucket scan.")

    watermark_date_str = wm.get("latest_date")
    watermark_date = None
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if watermark_date_str:
        print(f"Effective watermark: {watermark_date_str}")
        print(f"Total dates with data: {len(wm.get('generated_dates', []))}")
        try:
            watermark_date = datetime.strptime(watermark_date_str, "%Y-%m-%d")
        except ValueError:
            print("Warning: Watermark date is not in YYYY-MM-DD format. Ignoring.")

    # Determine the target date
    if input_date_str:
        input_date = datetime.strptime(input_date_str, "%Y-%m-%d")
        # Check if ALL requested dates already exist
        requested_dates = set()
        for i in range(int(days)):
            d = input_date + timedelta(days=i)
            requested_dates.add(d.strftime("%Y-%m-%d"))
        
        already_generated = requested_dates & set(wm.get("generated_dates", []))
        if already_generated == requested_dates:
            print(f"::notice::Duplicate Generation Prevented. All requested dates {sorted(requested_dates)} already have data.")
            sys.exit(0)
        elif already_generated:
            print(f"::warning::Partial overlap detected. These dates already have data: {sorted(already_generated)}")
            # Still proceed — the MERGE in the pipeline will handle dedup
        
        target_date_str = input_date_str
    else:
        if watermark_date:
            target_date = watermark_date + timedelta(days=1)
            target_date_str = target_date.strftime("%Y-%m-%d")
        else:
            target_date_str = today_str

    # Calculate end date
    start_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    end_date = start_date + timedelta(days=int(days) - 1)
    end_date_str = end_date.strftime("%Y-%m-%d")

    print(f"Target date for generation: {target_date_str}")
    print(f"End date after {days} day(s): {end_date_str}")

    # Write to GITHUB_ENV
    env_file = os.getenv('GITHUB_ENV')
    if env_file:
        with open(env_file, "a") as f:
            f.write(f"TARGET_DATE={target_date_str}\n")
            f.write(f"END_DATE={end_date_str}\n")
            f.write(f"GENERATED_DAYS={days}\n")
    return target_date_str


def update_watermark(end_date_str, start_date_str, days, bucket_id):
    """Append the new run to the watermark history and upload."""
    os.makedirs("data", exist_ok=True)

    # Load existing watermark if available locally or from HF
    wm = None
    if os.path.exists(LOCAL_WATERMARK_PATH):
        try:
            with open(LOCAL_WATERMARK_PATH, "r") as f:
                wm = json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    
    if wm is None:
        wm = _download_watermark(bucket_id)
    if wm is None:
        wm = _empty_watermark()

    # Add all dates from this run
    start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    new_dates = []
    for i in range(int(days)):
        d = start_dt + timedelta(days=i)
        new_dates.append(d.strftime("%Y-%m-%d"))

    existing_set = set(wm.get("generated_dates", []))
    existing_set.update(new_dates)
    wm["generated_dates"] = sorted(existing_set)
    wm["latest_date"] = wm["generated_dates"][-1]

    # Append to run history
    run_entry = {
        "start": start_date_str,
        "end": end_date_str,
        "days": int(days),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dates_added": new_dates
    }
    wm.setdefault("run_history", []).append(run_entry)

    # Write locally
    with open(LOCAL_WATERMARK_PATH, "w") as f:
        json.dump(wm, f, indent=2)
    print(f"Locally updated {LOCAL_WATERMARK_PATH}.")
    print(f"  Latest date : {wm['latest_date']}")
    print(f"  Total dates : {len(wm['generated_dates'])}")
    print(f"  This run    : {start_date_str} -> {end_date_str} ({days} day(s))")

    # Upload — non-fatal if it fails
    success = _upload_watermark(bucket_id)
    if not success:
        # Don't exit(1) — the data is already on HF, and the next run
        # will recover via bucket scan.
        print("::warning::Watermark upload failed. The next run will auto-recover by scanning the bucket.")
        sys.exit(0)


# ─── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage data generation watermark on Hugging Face.")
    parser.add_argument("--action", choices=["check", "update"], required=True)
    parser.add_argument("--input-date", help="User requested start date (YYYY-MM-DD)", default=None)
    parser.add_argument("--date", help="Date to update watermark to (YYYY-MM-DD)", default=None)
    parser.add_argument("--start-date", help="The initial target date", default=None)
    parser.add_argument("--days", help="Number of days generated", default="1")
    parser.add_argument("--bucket", default="aayushbhat26/hvac_application_poc_bucket")

    args = parser.parse_args()

    if args.action == "check":
        check_watermark(args.input_date, args.bucket, args.days)
    elif args.action == "update":
        if not args.date:
            print("Error: --date is required for update action.")
            sys.exit(1)
        update_watermark(args.date, args.start_date, args.days, args.bucket)
