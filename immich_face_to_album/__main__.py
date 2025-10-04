import requests
import click
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
        click.echo(f"Fetching time buckets from {url} with params: {params}")

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        if verbose:
            click.echo(f"Time buckets fetched: {response.json()}")
        return response.json()
    else:
        click.echo(
            click.style(
                f"Failed to fetch time buckets. Status code: {response.status_code}, Response text: {response.text}",
                fg="red",
            )
        )
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
        click.echo(
            f"Fetching assets for time bucket {time_bucket} from {url} with params: {params}"
        )

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        if verbose:
            click.echo(f"Assets fetched: {response.json()}")
        return response.json()
    else:
        click.echo(
            click.style(
                f"Failed to fetch assets for time bucket {time_bucket}. Status code: {response.status_code}, Response text: {response.text}",
                fg="red",
            )
        )
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
        click.echo(f"Adding assets to album {album_id} with payload: {payload}")

    response = requests.put(url, headers=headers, data=payload)

    if response.status_code == 200:
        if verbose:
            click.echo(f"Assets added to album: {asset_ids}")
        return True
    else:
        if verbose:
            click.echo(
                f"Error response: Status code: {response.status_code}, Response text: {response.text}"
            )
            try:
                error_response = response.json()
                click.echo(f"Full error JSON: {json.dumps(error_response, indent=2)}")
            except json.JSONDecodeError:
                click.echo(
                    f"Failed to decode JSON response. Response text: {response.text}"
                )
        else:
            try:
                error_response = response.json()
                click.echo(
                    f"Error adding assets to album: {error_response.get('error', 'Unknown error')}"
                )
            except json.JSONDecodeError:
                click.echo(
                    f"Failed to decode JSON response. Status code: {response.status_code}, Response text: {response.text}"
                )
        return False


def chunker(seq, size):
    return (seq[pos : pos + size] for pos in range(0, len(seq), size))



@click.command()
@click.option("--key", help="Your Immich API Key")
@click.option("--server", help="Your Immich server URL")
@click.option("--face", help="ID of the face you want to copy from. Can be used multiple times.", multiple=True)
@click.option("--skip-face", help="ID of a face to exclude (can be used multiple times).", multiple=True)
@click.option("--album", help="ID of the album you want to copy to")
@click.option("--timebucket", help="Time bucket size (e.g., MONTH, WEEK)", default="MONTH")
@click.option("--verbose", is_flag=True, help="Enable verbose output for debugging")
@click.option("--run-every-seconds", type=int, default=0, show_default=True, help="Automatically rerun synchronization every N seconds (0 = run once).")
@click.option("--config", type=click.Path(exists=True), help="Path to config file for face-to-album mappings.")
def face_to_album(key, server, face, skip_face, album, timebucket, verbose, run_every_seconds, config):
    """
    If --config is provided, load mappings from config file and process each mapping.
    Otherwise, fallback to CLI options for backward compatibility.
    """
    def process_mapping(mapping):
        # mapping: {"faceIds": [...], "albumId": ...}
        unique_asset_ids = set()
        album_id = mapping["albumId"]
        face_ids = mapping["faceIds"]
        skip_face_ids = mapping.get("skipFaceIds", [])
        for face_id in face_ids:
            if verbose:
                click.echo(f"Processing face ID: {face_id} for album {album_id}")
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
                    click.echo(f"Collecting assets to skip for face ID: {s_face}")
                time_buckets = get_time_buckets(server, key, s_face, timebucket, verbose)
                for bucket in time_buckets:
                    bucket_time = bucket.get("timeBucket")
                    bucket_assets = get_assets_for_time_bucket(server, key, s_face, bucket_time, timebucket, verbose)
                    skip_asset_ids.update(bucket_assets["id"])
            before = len(unique_asset_ids)
            unique_asset_ids.difference_update(skip_asset_ids)
            removed = before - len(unique_asset_ids)
            click.echo(f"Excluded {removed} asset(s) belonging to skipped face(s)")
        click.echo(f"Total unique assets to add to album {album_id}: {len(unique_asset_ids)}")
        asset_ids_list = list(unique_asset_ids)
        for asset_chunk in chunker(asset_ids_list, 500):
            if verbose:
                click.echo(f"Adding chunk of {len(asset_chunk)} assets to album {album_id}")
            success = add_assets_to_album(server, key, album_id, asset_chunk, verbose)
            if success:
                click.echo(click.style(f"Added {len(asset_chunk)} asset(s) to the album {album_id}", fg="green"))

    def run_once():
        if config:
            with open(config, "r") as f:
                config_data = json.load(f)
            mappings = config_data.get("mappings", [])
            for mapping in mappings:
                process_mapping(mapping)
        else:
            # fallback to CLI options
            mapping = {
                "faceIds": list(face),
                "albumId": album,
                "skipFaceIds": list(skip_face)
            }
            process_mapping(mapping)

    cron_expr = os.environ.get("CRON_EXPRESSION")
    if cron_expr:
        click.echo(f"Scheduling sync with CRON_EXPRESSION: {cron_expr}")
        scheduler = BlockingScheduler()
        scheduler.add_job(run_once, CronTrigger.from_crontab(cron_expr))
        try:
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            click.echo(click.style("Scheduler stopped.", fg="yellow"))
    elif run_every_seconds and run_every_seconds > 0:
        try:
            while True:
                run_once()
                click.echo(f"Waiting {run_every_seconds} second(s) before next execution...")
                time.sleep(run_every_seconds)
        except KeyboardInterrupt:
            click.echo(click.style("Stop requested (Ctrl+C). Ending repeated execution.", fg="yellow"))
    else:
        run_once()


def main(args=None):
    face_to_album()


if __name__ == "__main__":
    face_to_album()
