import requests
import sys
import json
import time
import os
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger


def get_time_buckets(server_url, key, face_id, size="MONTH", verbose=False):
    url = f"{server_url}/api/timeline/buckets"
    headers = {"x-api-key": key, "Accept": "application/json"}
    params = {"personId": face_id, "size": size}

    if verbose:
        print(f"Fetching time buckets from {url} with params: {params}")

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        if verbose:
            print(f"Time buckets fetched: {response.json()}")
        return response.json()
    else:
        print(f"Failed to fetch time buckets. Status code: {response.status_code}, Response text: {response.text}")
        exit(1)


def get_assets_for_time_bucket(
    server_url, key, face_id, time_bucket, size="MONTH", verbose=False
):
    url = f"{server_url}/api/timeline/bucket"
    headers = {"x-api-key": key, "Accept": "application/json"}
    params = {
        "isArchived": "false",
        "personId": face_id,
        "size": size,
        "timeBucket": time_bucket,
    }

    if verbose:
        print(f"Fetching assets for time bucket {time_bucket} from {url} with params: {params}")

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        if verbose:
            print(f"Assets fetched: {response.json()}")
        return response.json()
    else:
        print(f"Failed to fetch assets for time bucket {time_bucket}. Status code: {response.status_code}, Response text: {response.text}")
        exit(1)


def add_assets_to_album(server_url, key, album_id, asset_ids, verbose=False):
    url = f"{server_url}/api/albums/{album_id}/assets"
    headers = {
        "x-api-key": key,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = json.dumps({"ids": asset_ids})

    if verbose:
        print(f"Adding assets to album {album_id} with payload: {payload}")

    response = requests.put(url, headers=headers, data=payload)

    if response.status_code == 200:
        if verbose:
            print(f"Assets added to album: {asset_ids}")
        return True
    else:
        if verbose:
            print(f"Error response: Status code: {response.status_code}, Response text: {response.text}")
            try:
                error_response = response.json()
                print(f"Full error JSON: {json.dumps(error_response, indent=2)}")
            except json.JSONDecodeError:
                print(f"Failed to decode JSON response. Response text: {response.text}")
        else:
            try:
                error_response = response.json()
                print(f"Error adding assets to album: {error_response.get('error', 'Unknown error')}")
            except json.JSONDecodeError:
                print(f"Failed to decode JSON response. Status code: {response.status_code}, Response text: {response.text}")
        return False


def chunker(seq, size):
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))


def face_to_album():
    # Read environment variables
    key = os.environ.get("IMMICH_API_KEY")
    server = os.environ.get("IMMICH_SERVER_URL")
    config = os.environ.get("CONFIG_PATH", "/app/config.json")
    album = os.environ.get("IMMICH_ALBUM_ID")
    timebucket = os.environ.get("TIME_BUCKET", "MONTH")
    verbose = os.environ.get("VERBOSE", "false").lower() == "true"
    run_every_seconds = int(os.environ.get("RUN_EVERY_SECONDS", "0"))
    # Faces and skip faces can be comma-separated lists
    face = os.environ.get("IMMICH_FACE_IDS", "")
    face = [f for f in face.split(",") if f]
    skip_face = os.environ.get("IMMICH_SKIP_FACE_IDS", "")
    skip_face = [f for f in skip_face.split(",") if f]
    """
    If --config is provided, load mappings from config file and process each mapping.
    Otherwise, fallback to CLI options for backward compatibility.
    """
    def process_mapping(mapping):
        # mapping: {"faceIds": [...], "albumId": ...}
        unique_asset_ids = set()
        print(f"Processing mapping: {mapping}")
        album_id = mapping["albumId"]
        face_ids = mapping["faceIds"]
        skip_face_ids = mapping.get("skipFaceIds", [])
        print(f"Album ID: {album_id}, Face IDs: {face_ids}, Skip Face IDs: {skip_face_ids}")
        for face_id in face_ids:
            if verbose:
                print(f"Processing face ID: {face_id} for album {album_id}")
            time_buckets = get_time_buckets(server, key, face_id, timebucket, verbose)
            for bucket in time_buckets:
                bucket_time = bucket.get("timeBucket")
                bucket_assets = get_assets_for_time_bucket(server, key, face_id, bucket_time, timebucket, verbose)
                unique_asset_ids.update(bucket_assets["id"])
        # Exclude assets for skip faces
        if skip_face_ids:
            skip_asset_ids = set()
            for s_face in skip_face_ids:
                if verbose:
                    print(f"Collecting assets to skip for face ID: {s_face}")
                time_buckets = get_time_buckets(server, key, s_face, timebucket, verbose)
                for bucket in time_buckets:
                    bucket_time = bucket.get("timeBucket")
                    bucket_assets = get_assets_for_time_bucket(server, key, s_face, bucket_time, timebucket, verbose)
                    skip_asset_ids.update(bucket_assets["id"])
            before = len(unique_asset_ids)
            unique_asset_ids.difference_update(skip_asset_ids)
            removed = before - len(unique_asset_ids)
            print(f"Excluded {removed} asset(s) belonging to skipped face(s)")
        print(f"Total unique assets to add to album {album_id}: {len(unique_asset_ids)}")
        asset_ids_list = list(unique_asset_ids)
        for asset_chunk in chunker(asset_ids_list, 500):
            if verbose:
                print(f"Adding chunk of {len(asset_chunk)} assets to album {album_id}")
            success = add_assets_to_album(server, key, album_id, asset_chunk, verbose)
            if success:
                print(f"Added {len(asset_chunk)} asset(s) to the album {album_id}")

    def run_once():
        if config and os.path.exists(config):
            with open(config, "r") as f:
                config_data = json.load(f)
            print(f"Loaded config: {config_data}")
            mappings = config_data.get("mappings", [])
            for mapping in mappings:
                process_mapping(mapping)
        else:
            # fallback to env options
            mapping = {
                "faceIds": face,
                "albumId": album,
                "skipFaceIds": skip_face
            }
            process_mapping(mapping)

    cron_expr = os.environ.get("CRON_EXPRESSION")
    if cron_expr:
        print(f"Scheduling sync with CRON_EXPRESSION: {cron_expr}")
        scheduler = BlockingScheduler()
        scheduler.add_job(run_once, CronTrigger.from_crontab(cron_expr))
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            print("Scheduler stopped.")
    elif run_every_seconds and run_every_seconds > 0:
        try:
            while True:
                run_once()
                print(f"Waiting {run_every_seconds} second(s) before next execution...")
                time.sleep(run_every_seconds)
        except KeyboardInterrupt:
            print("Stop requested (Ctrl+C). Ending repeated execution.")
    else:
        run_once()


def main(args=None):
    face_to_album()


if __name__ == "__main__":
    face_to_album()
