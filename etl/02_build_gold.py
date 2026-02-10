# etl/02_build_gold.py

print(">>> BOOT 02_build_gold.py", flush=True)

import os
from pathlib import Path

CA_PATH = Path(__file__).resolve().parents[1] / "certs" / "combined_ca.pem"
if CA_PATH.exists():
    os.environ["REQUESTS_CA_BUNDLE"] = str(CA_PATH)
    os.environ["SSL_CERT_FILE"] = str(CA_PATH)
    print(f">>> SSL bundle set: {CA_PATH}", flush=True)
else:
    print(f">>> WARNING: combined_ca.pem not found at {CA_PATH}", flush=True)


print(">>> BOOT 02_build_gold.py", flush=True)

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

def dump_later(seconds=30):
    time.sleep(seconds)
    print(f"\n>>> DUMPING STACKS after {seconds}s\n", flush=True)
    faulthandler.dump_traceback(file=sys.stdout, all_threads=True)

threading.Thread(target=dump_later, daemon=True).start()

#################################
# Config
#################################
SEASON = "2025-26"
BUCKET = "nba-data-gustavo"
ASOF = date.today().strftime("%Y-%m-%d")

RAW_GAMES_GCS = f"gs://{BUCKET}/raw/season={SEASON}/endpoint=leaguegamelog/asof={ASOF}/data.parquet"
RAW_STAND_GCS = f"gs://{BUCKET}/raw/season={SEASON}/endpoint=leaguestandingsv3/asof={ASOF}/data.parquet"

GOLD_KPIS_GCS = f"gs://{BUCKET}/gold/season={SEASON}/kpis.parquet"
GOLD_TEAM_TOTALS_GCS = f"gs://{BUCKET}/gold/season={SEASON}/team_totals.parquet"
GOLD_STANDINGS_GCS = f"gs://{BUCKET}/gold/season={SEASON}/standings.parquet"


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

    # Garante pasta local
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


def main() -> None:
    print(f">>> season={SEASON} asof={ASOF}", flush=True)

    # 1) Ler RAW (via download local)
    df_games = read_parquet_from_gcs(
        RAW_GAMES_GCS,
        f"nba_raw_leaguegamelog_{SEASON}_{ASOF}.parquet"
    )
    print(f">>> RAW games rows={len(df_games)} cols={df_games.shape[1]}", flush=True)

    df_stand = read_parquet_from_gcs(
        RAW_STAND_GCS,
        f"nba_raw_standings_{SEASON}_{ASOF}.parquet"
    )
    print(f">>> RAW standings rows={len(df_stand)} cols={df_stand.shape[1]}", flush=True)

    # 2) TEAM TOTALS (soma por time)
    numeric_cols = ["PTS", "AST", "REB", "OREB", "DREB", "STL", "BLK", "TOV", "PF", "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA"]
    for c in numeric_cols:
        if c in df_games.columns:
            df_games[c] = pd.to_numeric(df_games[c], errors="coerce")

    group_keys = [c for c in ["TEAM_ID", "TEAM_ABBREVIATION", "TEAM_NAME"] if c in df_games.columns]
    agg_map = {c: "sum" for c in numeric_cols if c in df_games.columns}
    df_team_totals = df_games.groupby(group_keys, as_index=False).agg(agg_map)
    df_team_totals["ASOF"] = ASOF
    df_team_totals["SEASON"] = SEASON

    # 3) KPIs da liga (totais simples)
    kpi_fields = [c for c in ["PTS", "AST", "REB", "STL", "BLK", "TOV"] if c in df_games.columns]
    kpis = {f"TOTAL_{c}": float(df_games[c].sum(skipna=True)) for c in kpi_fields}
    kpis["GAMES_ROWS"] = int(len(df_games))
    kpis["ASOF"] = ASOF
    kpis["SEASON"] = SEASON
    df_kpis = pd.DataFrame([kpis])

    # 4) Standings (salvar “como está” + metadados)
    df_standings = df_stand.copy()
    df_standings["ASOF"] = ASOF
    df_standings["SEASON"] = SEASON

    # 5) Salvar GOLD (via local + upload)
    write_parquet_to_gcs(df_kpis, GOLD_KPIS_GCS, f"nba_gold_kpis_{SEASON}_{ASOF}.parquet")
    write_parquet_to_gcs(df_team_totals, GOLD_TEAM_TOTALS_GCS, f"nba_gold_team_totals_{SEASON}_{ASOF}.parquet")
    write_parquet_to_gcs(df_standings, GOLD_STANDINGS_GCS, f"nba_gold_standings_{SEASON}_{ASOF}.parquet")

    print("\n>>> GOLD salvo no GCS:", flush=True)
    print(GOLD_KPIS_GCS, flush=True)
    print(GOLD_TEAM_TOTALS_GCS, flush=True)
    print(GOLD_STANDINGS_GCS, flush=True)


if __name__ == "__main__":
    main()
