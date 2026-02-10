# etl/02_build_gold.py
print(">>> BOOT 02_build_gold.py", flush=True)

import os
import sys
import time
import threading
import tempfile
import argparse
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
# GCS helpers
#################################
def parse_gs_uri(gs_uri: str) -> tuple[str, str]:
    if not gs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {gs_uri}")
    parts = gs_uri.split("/", 3)  # ["gs:", "", "bucket", "path..."]
    bucket_name = parts[2]
    blob_path = parts[3] if len(parts) > 3 else ""
    return bucket_name, blob_path


def download_gcs_to_local(gs_uri: str, local_path: str) -> None:
    bucket_name, blob_path = parse_gs_uri(gs_uri)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    Path(local_path).parent.mkdir(parents=True, exist_ok=True)
    blob.download_to_filename(local_path)
    print(f">>> Downloaded: {gs_uri} -> {local_path}", flush=True)


def upload_file_to_gcs(local_path: str, gs_uri: str) -> None:
    bucket_name, blob_path = parse_gs_uri(gs_uri)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    blob.upload_from_filename(local_path)
    print(f">>> Uploaded: {gs_uri}", flush=True)


def read_parquet_from_gcs(gs_uri: str, local_name: str) -> pd.DataFrame:
    tmp_dir = Path(tempfile.gettempdir())
    local_path = tmp_dir / local_name

    print(f">>> Reading from GCS via local temp: {gs_uri}", flush=True)
    download_gcs_to_local(gs_uri, str(local_path))

    df = pd.read_parquet(local_path, engine="pyarrow")

    try:
        local_path.unlink()
    except Exception:
        pass

    return df


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
    print(f">>> season={season} asof={asof} bucket={bucket}", flush=True)

    raw_games_gcs = f"gs://{bucket}/raw/season={season}/endpoint=leaguegamelog/asof={asof}/data.parquet"
    raw_stand_gcs = f"gs://{bucket}/raw/season={season}/endpoint=leaguestandingsv3/asof={asof}/data.parquet"

    gold_kpis_gcs = f"gs://{bucket}/gold/season={season}/kpis.parquet"
    gold_team_totals_gcs = f"gs://{bucket}/gold/season={season}/team_totals.parquet"
    gold_standings_gcs = f"gs://{bucket}/gold/season={season}/standings.parquet"

    # 1) Read RAW
    df_games = read_parquet_from_gcs(raw_games_gcs, f"nba_raw_leaguegamelog_{season}_{asof}.parquet")
    print(f">>> RAW games rows={len(df_games)} cols={df_games.shape[1]}", flush=True)

    df_stand = read_parquet_from_gcs(raw_stand_gcs, f"nba_raw_standings_{season}_{asof}.parquet")
    print(f">>> RAW standings rows={len(df_stand)} cols={df_stand.shape[1]}", flush=True)

    # 2) Team totals
    numeric_cols = [
        "PTS", "AST", "REB", "OREB", "DREB", "STL", "BLK", "TOV", "PF",
        "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA"
    ]
    for c in numeric_cols:
        if c in df_games.columns:
            df_games[c] = pd.to_numeric(df_games[c], errors="coerce")

    group_keys = [c for c in ["TEAM_ID", "TEAM_ABBREVIATION", "TEAM_NAME"] if c in df_games.columns]
    agg_map = {c: "sum" for c in numeric_cols if c in df_games.columns}

    df_team_totals = df_games.groupby(group_keys, as_index=False).agg(agg_map)
    df_team_totals["ASOF"] = asof
    df_team_totals["SEASON"] = season

    # 3) KPIs da liga
    kpi_fields = [c for c in ["PTS", "AST", "REB", "STL", "BLK", "TOV"] if c in df_games.columns]
    kpis = {f"TOTAL_{c}": float(df_games[c].sum(skipna=True)) for c in kpi_fields}
    kpis["GAMES_ROWS"] = int(len(df_games))
    kpis["ASOF"] = asof
    kpis["SEASON"] = season
    df_kpis = pd.DataFrame([kpis])

    # 4) Standings gold
    df_standings = df_stand.copy()
    df_standings["ASOF"] = asof
    df_standings["SEASON"] = season

    # 5) Write GOLD
    write_parquet_to_gcs(df_kpis, gold_kpis_gcs, f"nba_gold_kpis_{season}_{asof}.parquet")
    write_parquet_to_gcs(df_team_totals, gold_team_totals_gcs, f"nba_gold_team_totals_{season}_{asof}.parquet")
    write_parquet_to_gcs(df_standings, gold_standings_gcs, f"nba_gold_standings_{season}_{asof}.parquet")

    print("\n>>> GOLD salvo no GCS:", flush=True)
    print(gold_kpis_gcs, flush=True)
    print(gold_team_totals_gcs, flush=True)
    print(gold_standings_gcs, flush=True)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--season", required=True, help='Ex: "2024-25"')
    p.add_argument("--asof", required=True, help='Ex: "2026-02-10"')
    p.add_argument("--bucket", default="nba-data-gustavo")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(season=args.season, asof=args.asof, bucket=args.bucket)
