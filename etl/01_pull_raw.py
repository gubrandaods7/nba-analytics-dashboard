# etl/01_pull_raw.py

print(">>> BOOT 01_pull_raw.py", flush=True)

import os
import sys
import time
import threading
import tempfile
from datetime import date
from pathlib import Path

import faulthandler
import pandas as pd
from google.cloud import storage

#################################
# Debug: dump de stack se travar
#################################
faulthandler.enable()


def dump_later(seconds: int = 30) -> None:
    time.sleep(seconds)
    print(f"\n>>> DUMPING STACKS after {seconds}s\n", flush=True)
    faulthandler.dump_traceback(file=sys.stdout, all_threads=True)


threading.Thread(target=dump_later, daemon=True).start()

#################################
# CERTS (corporate SSL)
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
# Config
#################################
SEASON = "2025-26"
BUCKET = "nba-data-gustavo"
ASOF = date.today().strftime("%Y-%m-%d")

raw_games_gcs = f"gs://{BUCKET}/raw/season={SEASON}/endpoint=leaguegamelog/asof={ASOF}/data.parquet"
raw_stand_gcs = f"gs://{BUCKET}/raw/season={SEASON}/endpoint=leaguestandingsv3/asof={ASOF}/data.parquet"


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
    # 1) escreve local
    tmp_dir = Path(tempfile.gettempdir())
    local_path = tmp_dir / local_name

    print(f">>> Writing parquet locally: {local_path}", flush=True)
    df.to_parquet(local_path, index=False, engine="pyarrow")

    # 2) upload GCS
    print(f">>> Uploading to GCS: {gs_uri}", flush=True)
    upload_file_to_gcs(str(local_path), gs_uri)

    # 3) cleanup
    try:
        local_path.unlink()
    except Exception:
        pass


def main() -> None:
    print(f">>> season={SEASON} asof={ASOF}", flush=True)

    # 1) LeagueGameLog
    print(">>> Fetching LeagueGameLog...", flush=True)
    lg = leaguegamelog.LeagueGameLog(season=SEASON, timeout=30)
    df_games = lg.get_data_frames()[0]
    print(f">>> LeagueGameLog rows={len(df_games)} cols={df_games.shape[1]}", flush=True)

    write_parquet_to_gcs(df_games, raw_games_gcs, f"nba_raw_leaguegamelog_{SEASON}_{ASOF}.parquet")

    # 2) Standings
    print(">>> Fetching LeagueStandingsV3...", flush=True)
    st = leaguestandingsv3.LeagueStandingsV3(timeout=30)
    df_stand = st.get_data_frames()[0]
    print(f">>> Standings rows={len(df_stand)} cols={df_stand.shape[1]}", flush=True)

    write_parquet_to_gcs(df_stand, raw_stand_gcs, f"nba_raw_standings_{SEASON}_{ASOF}.parquet")

    print("\n>>> RAW salvo no GCS:", flush=True)
    print(raw_games_gcs, flush=True)
    print(raw_stand_gcs, flush=True)
    print(">>> Colunas LeagueGameLog:", list(df_games.columns), flush=True)


if __name__ == "__main__":
    main()
