import os
import sys
import argparse
from datetime import datetime, timedelta, timezone
from huggingface_hub import hf_hub_download, HfApi
from huggingface_hub.utils import EntryNotFoundError, RepositoryNotFoundError, HfHubHTTPError

def check_watermark(input_date_str, bucket_id, days):
    watermark_date_str = None
    
    print(f"Checking for existing watermark in {bucket_id} ...")
    try:
        downloaded_path = hf_hub_download(
            repo_id=bucket_id, 
            filename="watermark.txt", 
            repo_type="bucket",
            force_download=True 
        )
        with open(downloaded_path, "r") as f:
            watermark_date_str = f.readline().strip() # Read only the first line for the date
    except (EntryNotFoundError, RepositoryNotFoundError, HfHubHTTPError) as e:
        print(f"No existing watermark found (or accessible) on Hugging Face: {e}")
    except Exception as e:
        print(f"Warning: Unexpected error while fetching watermark: {e}")

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if watermark_date_str:
        print(f"Current watermark found: {watermark_date_str}")
        try:
            watermark_date = datetime.strptime(watermark_date_str, "%Y-%m-%d")
        except ValueError:
            print("Warning: Watermark is not in YYYY-MM-DD format. Ignoring.")
            watermark_date = None
    else:
        watermark_date = None

    # Determine the target date
    if input_date_str:
        input_date = datetime.strptime(input_date_str, "%Y-%m-%d")
        if watermark_date and input_date <= watermark_date:
            print(f"::notice::Duplicate Generation Prevented. Requested start date {input_date_str} is <= watermark {watermark_date_str}.")
            sys.exit(0)
        target_date_str = input_date_str
    else:
        if watermark_date:
            target_date = watermark_date + timedelta(days=1)
            target_date_str = target_date.strftime("%Y-%m-%d")
        else:
            target_date_str = today_str
            
    # Calculate end date based on days generated
    start_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    end_date = start_date + timedelta(days=int(days) - 1)
    end_date_str = end_date.strftime("%Y-%m-%d")
            
    print(f"Target date for generation: {target_date_str}")
    print(f"End date after {days} days: {end_date_str}")
    
    # Write to GITHUB_ENV so subsequent steps can use it
    env_file = os.getenv('GITHUB_ENV')
    if env_file:
        with open(env_file, "a") as f:
            f.write(f"TARGET_DATE={target_date_str}\n")
            f.write(f"END_DATE={end_date_str}\n")
            f.write(f"GENERATED_DAYS={days}\n")
    return target_date_str

def update_watermark(end_date_str, start_date_str, days):
    os.makedirs("data", exist_ok=True)
    with open("data/watermark.txt", "w") as f:
        f.write(f"{end_date_str}\n")
        f.write(f"This dataset contains data up to {end_date_str}.\n")
        f.write(f"Last workflow run generated {days} day(s) of data starting from {start_date_str}.\n")
    print(f"Locally updated data/watermark.txt to {end_date_str}.")
    
    # Upload to HF Bucket using the hf CLI (buckets are NOT standard datasets)
    import subprocess
    try:
        result = subprocess.run(
            ["hf", "upload", "aayushbhat26/hvac_application_poc_bucket", 
             "data/watermark.txt", "watermark.txt", "--repo-type", "bucket"],
            capture_output=True, text=True, timeout=60
        )
        if result.returncode == 0:
            print("Successfully uploaded watermark.txt to Hugging Face Bucket.")
        else:
            print(f"hf CLI upload failed: {result.stderr}")
            # Fallback: try HfApi with repo_type=None for buckets
            api = HfApi()
            api.upload_file(
                path_or_fileobj="data/watermark.txt",
                path_in_repo="watermark.txt",
                repo_id="aayushbhat26/hvac_application_poc_bucket",
                repo_type="bucket"
            )
            print("Successfully uploaded watermark.txt via HfApi fallback.")
    except Exception as e:
        print(f"Error uploading watermark to Hugging Face: {e}")
        sys.exit(1)

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
        update_watermark(args.date, args.start_date, args.days)
