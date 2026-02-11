#.venv\Scripts\activate
#$env:GOOGLE_APPLICATION_CREDENTIALS="C:\...\nba-gcp-sa.json"
# $env:GOOGLE_APPLICATION_CREDENTIALS="C:\Users\gustavo.nersissian\OneDrive - Andrade Gutierrez\Gustavo AG\10 Outros\gcp-secrets\nba-gcp-sa.json"


# etl/01_pull_raw.py
print(">>> BOOT 01_pull_raw.py", flush=True)

import os
import sys
import time
import threading
import tempfile
import argparse
from datetime import date
from pathlib import Path

import faulthandler
import pandas as pd
from google.cloud import storage

#################################
# Debug (opcional): dump se travar
#################################
faulthandler.enable()

def dump_later(seconds: int = 60) -> None:
    time.sleep(seconds)
    print(f"\n>>> DUMPING STACKS after {seconds}s\n", flush=True)
    faulthandler.dump_traceback(file=sys.stdout, all_threads=True)

# deixe ligado enquanto estiver backfillando; depois você pode comentar
threading.Thread(target=dump_later, daemon=True).start()

#################################
# SSL (corporate) - precisa vir cedo
#################################
CA_PATH = Path(__file__).resolve().parents[1] / "certs" / "combined_ca.pem"
if CA_PATH.exists():
    os.environ["REQUESTS_CA_BUNDLE"] = str(CA_PATH)
    os.environ["SSL_CERT_FILE"] = str(CA_PATH)
    print(f">>> SSL bundle set: {CA_PATH}", flush=True)
else:
    print(f">>> WARNING: combined_ca.pem not found at {CA_PATH}", flush=True)

#################################
# NBA API headers (GLOBAL)
#################################
from nba_api.stats.library.http import NBAStatsHTTP

NBAStatsHTTP.headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
    "Connection": "keep-alive",
}
print(">>> NBAStatsHTTP headers configured", flush=True)

from nba_api.stats.endpoints import leaguegamelog, leaguestandingsv3

#################################
# GCS helpers
#################################
def parse_gs_uri(gs_uri: str) -> tuple[str, str]:
    if not gs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {gs_uri}")
    parts = gs_uri.split("/", 3)  # ["gs:", "", "bucket", "path..."]
    bucket_name = parts[2]
    blob_path = parts[3] if len(parts) > 3 else ""
    return bucket_name, blob_path


def upload_file_to_gcs(local_path: str, gs_uri: str) -> None:
    bucket_name, blob_path = parse_gs_uri(gs_uri)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_path)
    print(f">>> Uploaded: {gs_uri}", flush=True)


def write_parquet_to_gcs(df: pd.DataFrame, gs_uri: str, local_name: str) -> None:
    tmp_dir = Path(tempfile.gettempdir())
    local_path = tmp_dir / local_name

    print(f">>> Writing parquet locally: {local_path}", flush=True)
    df.to_parquet(local_path, index=False, engine="pyarrow")

    print(f">>> Uploading to GCS: {gs_uri}", flush=True)
    upload_file_to_gcs(str(local_path), gs_uri)

    try:
        local_path.unlink()
    except Exception:
        pass


#################################
# Main
#################################
def main(season: str, asof: str, bucket: str) -> None:
    raw_games_gcs = (
        f"gs://{bucket}/raw/season={season}/endpoint=leaguegamelog/asof={asof}/data.parquet"
    )
    raw_stand_gcs = (
        f"gs://{bucket}/raw/season={season}/endpoint=leaguestandingsv3/asof={asof}/data.parquet"
    )

    print(f">>> season={season} asof={asof} bucket={bucket}", flush=True)

    # 1) LeagueGameLog
    print(">>> Fetching LeagueGameLog...", flush=True)
    lg = leaguegamelog.LeagueGameLog(season=season, timeout=30)
    df_games = lg.get_data_frames()[0]
    print(f">>> LeagueGameLog rows={len(df_games)} cols={df_games.shape[1]}", flush=True)

    write_parquet_to_gcs(df_games, raw_games_gcs, f"nba_raw_leaguegamelog_{season}_{asof}.parquet")

    # 2) Standings
    print(">>> Fetching LeagueStandingsV3...", flush=True)
    st = leaguestandingsv3.LeagueStandingsV3(timeout=30)
    df_stand = st.get_data_frames()[0]
    print(f">>> Standings rows={len(df_stand)} cols={df_stand.shape[1]}", flush=True)

    write_parquet_to_gcs(df_stand, raw_stand_gcs, f"nba_raw_standings_{season}_{asof}.parquet")

    print("\n>>> RAW salvo no GCS:", flush=True)
    print(raw_games_gcs, flush=True)
    print(raw_stand_gcs, flush=True)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--season", required=True, help='Ex: "2024-25"')
    p.add_argument("--asof", required=True, help='Ex: "2026-02-10"')
    p.add_argument("--bucket", default="nba-data-gustavo")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(season=args.season, asof=args.asof, bucket=args.bucket)
