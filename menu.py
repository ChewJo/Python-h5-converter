import boto3
from botocore import UNSIGNED
from botocore.client import Config
import os
from main import process_radar_file

s3 = boto3.client('s3', region_name='eu-west-2', config=Config(signature_version=UNSIGNED))
bucket_name = 'met-office-radar-obs-data'

def list_prefixes(prefix):
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix,
        Delimiter='/'
    )
    return sorted(
        [p['Prefix'] for p in response.get('CommonPrefixes', [])],
        reverse=True
    )

def list_files(prefix):
    response = s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=prefix
    )

    files = response.get('Contents', [])
    return sorted(
        [f['Key'] for f in files if f['Key'].endswith('.h5')],
        reverse=True
    )

def processed_file_exists(key, output_folder):
    base_name = os.path.splitext(os.path.basename(key))[0]

    # may delete if we decide to remove colured option
    grey = os.path.join(output_folder, f"{base_name}_greyscale.tif")
    colour = os.path.join(output_folder, f"{base_name}_coloured.tif")

    return os.path.exists(grey) or os.path.exists(colour)

def process_until_caught_up(output_folder, colour=False, stop_on_catchup=True):
    print("Starting catch-up processor...")

    os.makedirs('temp_h5', exist_ok=True)

    any_new_files = False

    years = list_prefixes("radar/")

    for year in years:
        months = list_prefixes(year)

        for month in months:
            days = list_prefixes(month)

            for day in days:
                print(f"Checking: {day}")

                files = list_files(day)

                for key in files:

                    if processed_file_exists(key, output_folder):
                        print(f"Already processed: {key}")

                        if stop_on_catchup:
                            print("Caught up with existing data. Stopping.")
                            return

                        continue

                    any_new_files = True

                    print(f"New file detected: {key}")

                    local_path = os.path.join('temp_h5', os.path.basename(key))
                    print(f"Downloading {key}...")

                    s3.download_file(bucket_name, key, local_path)

                    base_name = os.path.splitext(os.path.basename(key))[0]

                    output_filename = (
                        f"{base_name}_coloured.tif"
                        if colour else
                        f"{base_name}_greyscale.tif"
                    )

                    print(f"Processing {key} => {output_filename}")

                    process_radar_file(
                        local_path,
                        output_folder,
                        colour=colour,
                        output_filename=output_filename
                    )

    if not any_new_files:
        print("No unprocessed files found at all – fully up to date.")
    else:
        print("Reached end of available historical data.")

if __name__ == "__main__":
    output_folder = "output/cog"

    process_until_caught_up(
        output_folder,
        colour=False,
        stop_on_catchup=False
    )
