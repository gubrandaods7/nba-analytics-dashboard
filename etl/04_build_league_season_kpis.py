# etl/04_build_league_season_kpis.py
print(">>> BOOT 04_build_league_season_kpis.py", flush=True)

import os
import sys
import re
import time
import threading
import tempfile
from pathlib import Path
from datetime import date

import faulthandler
import pandas as pd
from google.cloud import storage

# --------------------------------
# Debug: dump stacks if hang
# --------------------------------
faulthandler.enable()

def dump_later(seconds: int = 120) -> None:
    time.sleep(seconds)
    print(f"\n>>> DUMPING STACKS after {seconds}s\n", flush=True)
    faulthandler.dump_traceback(file=sys.stdout, all_threads=True)

# keep enabled while debugging corporate network hangs
threading.Thread(target=dump_later, args=(180,), daemon=True).start()

# --------------------------------
# SSL corporate bundle (early)
# --------------------------------
CA_PATH = Path(__file__).resolve().parents[1] / "certs" / "combined_ca.pem"
if CA_PATH.exists():
    os.environ["REQUESTS_CA_BUNDLE"] = str(CA_PATH)
    os.environ["SSL_CERT_FILE"] = str(CA_PATH)
    print(f">>> SSL bundle set: {CA_PATH}", flush=True)
else:
    print(f">>> WARNING: combined_ca.pem not found at {CA_PATH}", flush=True)

DEFAULT_BUCKET = "nba-data-gustavo"

# --------------------------------
# GCS helpers (robust: explicit download/upload)
# --------------------------------
def get_client() -> storage.Client:
    return storage.Client()

def assert_bucket_access(bucket_name: str) -> None:
    print(f">>> Validating access to bucket objects: {bucket_name}", flush=True)
    client = get_client()
    bucket = client.bucket(bucket_name)

    # bucket.exists() requires storage.buckets.get, which may be blocked.
    # Try listing 1 object instead (requires storage.objects.list).
    try:
        _ = next(bucket.list_blobs(max_results=1), None)
    except Exception as e:
        raise RuntimeError(
            f"Cannot list objects in gs://{bucket_name}. "
            f"Likely missing permissions or wrong credentials. Error: {e}"
        )
    print(f">>> Bucket list OK (objects): gs://{bucket_name}", flush=True)

def list_gold_seasons(bucket_name: str) -> list[str]:
    """
    Discover seasons from gold/season=YYYY-YY/ prefixes.
    (We use gold because you already have backfilled gold for 2020-2025.)
    """
    client = get_client()
    bucket = client.bucket(bucket_name)

    it = bucket.list_blobs(prefix="gold/", delimiter="/")
    _ = list(it)  # populate prefixes
    seasons = []
    for p in it.prefixes:
        m = re.search(r"gold/season=([^/]+)/", p)
        if m:
            seasons.append(m.group(1))
    return sorted(seasons)

def list_raw_asof_dates_for_endpoint(bucket_name: str, season: str, endpoint: str) -> list[str]:
    """
    Return available ASOF dates for a RAW endpoint (newest first).
    Example:
      raw/season=2025-26/endpoint=leaguegamelog/asof=2026-02-10/data.parquet
    """
    client = get_client()
    bucket = client.bucket(bucket_name)

    prefix = f"raw/season={season}/endpoint={endpoint}/"
    blobs = bucket.list_blobs(prefix=prefix)

    dates = set()
    for b in blobs:
        m = re.search(r"/asof=(\d{4}-\d{2}-\d{2})/", b.name)
        if m:
            dates.add(m.group(1))
    return sorted(dates, reverse=True)

def download_blob_to_temp(bucket_name: str, blob_path: str) -> Path:
    client = get_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    if not blob.exists():
        raise FileNotFoundError(f"Blob not found: gs://{bucket_name}/{blob_path}")

    tmp_dir = Path(tempfile.gettempdir())
    local_path = tmp_dir / Path(blob_path).name

    print(f">>> Downloading gs://{bucket_name}/{blob_path} -> {local_path}", flush=True)
    blob.download_to_filename(str(local_path))
    return local_path

def read_parquet_gcs(bucket_name: str, blob_path: str) -> pd.DataFrame:
    local = download_blob_to_temp(bucket_name, blob_path)
    print(f">>> Reading parquet locally: {local}", flush=True)
    df = pd.read_parquet(local, engine="pyarrow")
    try:
        local.unlink()
    except Exception:
        pass
    return df

def upload_file(bucket_name: str, local_path: Path, blob_path: str) -> None:
    client = get_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    print(f">>> Uploading {local_path} -> gs://{bucket_name}/{blob_path}", flush=True)
    blob.upload_from_filename(str(local_path))
    print(f">>> Uploaded: gs://{bucket_name}/{blob_path}", flush=True)

# --------------------------------
# Aggregation from RAW leaguegamelog
# --------------------------------
def compute_league_season_totals_from_leaguegamelog(df_games: pd.DataFrame) -> dict:
    """
    df_games is expected at team-game granularity (LeagueGameLog).
    We'll sum across all rows (teams) for totals,
    and compute number of unique games by GAME_ID.
    """
    # Column canonicalization: sometimes the api uses uppercase
    cols = {c.upper(): c for c in df_games.columns}

    def col(name: str) -> str | None:
        return cols.get(name.upper())

    def sum_numeric(cname: str):
        c = col(cname)
        if not c:
            return None
        return pd.to_numeric(df_games[c], errors="coerce").fillna(0).sum()

    game_id_col = col("GAME_ID")
    games = None
    if game_id_col:
        games = int(df_games[game_id_col].astype(str).nunique())

    totals = {
        "games": games,
        "total_pts": sum_numeric("PTS"),
        "total_ast": sum_numeric("AST"),
        "total_reb": sum_numeric("REB"),
        "total_stl": sum_numeric("STL"),
        "total_blk": sum_numeric("BLK"),
        "total_tov": sum_numeric("TOV"),
        "total_fg3m": sum_numeric("FG3M"),
        "total_fg3a": sum_numeric("FG3A"),
    }
    return totals

def add_per_game_metrics(row: dict) -> dict:
    g = row.get("games")
    if not g or pd.isna(g) or g == 0:
        # leave per-game as None
        row["pts_per_game"] = None
        row["ast_per_game"] = None
        row["reb_per_game"] = None
        row["stl_per_game"] = None
        row["blk_per_game"] = None
        row["tov_per_game"] = None
        row["fg3m_per_game"] = None
        row["fg3a_per_game"] = None
        return row

    def per(total_key: str):
        v = row.get(total_key)
        if v is None or pd.isna(v):
            return None
        return float(v) / float(g)

    row["pts_per_game"] = per("total_pts")
    row["ast_per_game"] = per("total_ast")
    row["reb_per_game"] = per("total_reb")
    row["stl_per_game"] = per("total_stl")
    row["blk_per_game"] = per("total_blk")
    row["tov_per_game"] = per("total_tov")
    row["fg3m_per_game"] = per("total_fg3m")
    row["fg3a_per_game"] = per("total_fg3a")
    return row

def season_in_range(season: str, season_min: str | None, season_max: str | None) -> bool:
    if season_min and season < season_min:
        return False
    if season_max and season > season_max:
        return False
    return True

# --------------------------------
# Main
# --------------------------------
def main(bucket: str, season_min: str | None, season_max: str | None, endpoint: str, asof: str | None) -> None:
    assert_bucket_access(bucket)

    seasons = list_gold_seasons(bucket)
    if not seasons:
        raise RuntimeError(f"No seasons found under gs://{bucket}/gold/season=...")

    seasons = [s for s in seasons if season_in_range(s, season_min, season_max)]
    print(f">>> Seasons to process: {len(seasons)} -> {seasons}", flush=True)

    rows = []
    errors = []

    for s in seasons:
        print(f"\n>>> Processing season={s}", flush=True)

        try:
            # Choose ASOF:
            # - if user passed --asof, use it
            # - else pick latest available in raw for this endpoint
            if asof:
                chosen_asof = asof
            else:
                asofs = list_raw_asof_dates_for_endpoint(bucket, s, endpoint=endpoint)
                if not asofs:
                    raise FileNotFoundError(f"No RAW snapshots found for season={s} endpoint={endpoint}")
                chosen_asof = asofs[0]

            blob_path = f"raw/season={s}/endpoint={endpoint}/asof={chosen_asof}/data.parquet"

            df_games = read_parquet_gcs(bucket, blob_path)
            print(f">>> RAW {endpoint} rows={len(df_games)} cols={df_games.shape[1]} asof={chosen_asof}", flush=True)

            totals = compute_league_season_totals_from_leaguegamelog(df_games)
            row = {"season": s, "asof": chosen_asof}
            row.update(totals)
            row = add_per_game_metrics(row)

            print(
                f">>> OK season={s} games={row.get('games')} PTS={row.get('total_pts')} "
                f"FG3M={row.get('total_fg3m')} FG3A={row.get('total_fg3a')}",
                flush=True
            )

            rows.append(row)

        except Exception as e:
            print(f">>> ERROR season={s}: {e}", flush=True)
            errors.append((s, str(e)))
            continue

    if not rows:
        raise RuntimeError("No rows produced. Check errors above and bucket contents.")

    df = pd.DataFrame(rows)

    # Coerce numeric columns
    num_cols = [
        "games",
        "total_pts","total_ast","total_reb","total_stl","total_blk","total_tov","total_fg3m","total_fg3a",
        "pts_per_game","ast_per_game","reb_per_game","stl_per_game","blk_per_game","tov_per_game","fg3m_per_game","fg3a_per_game",
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.sort_values("season").reset_index(drop=True)

    out_local = Path(tempfile.gettempdir()) / "league_season_kpis.parquet"
    print(f"\n>>> Writing output parquet locally: {out_local}", flush=True)
    df.to_parquet(out_local, index=False, engine="pyarrow")

    out_blob = "gold/league_season_kpis.parquet"
    upload_file(bucket, out_local, out_blob)

    try:
        out_local.unlink()
    except Exception:
        pass

    print(f"\n✅ DONE: gs://{bucket}/{out_blob}", flush=True)
    print(f">>> seasons_written={len(df)} cols={df.shape[1]}", flush=True)

    if errors:
        print("\n⚠️ Seasons with errors:", flush=True)
        for s, msg in errors:
            print(f" - {s}: {msg}", flush=True)

def parse_args():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", default=DEFAULT_BUCKET)
    p.add_argument("--season-min", default=None, help='Ex: "2020-21" (optional)')
    p.add_argument("--season-max", default=None, help='Ex: "2025-26" (optional)')
    p.add_argument("--endpoint", default="leaguegamelog", help='RAW endpoint folder name (default: leaguegamelog)')
    p.add_argument("--asof", default=None, help='Force a specific ASOF date (YYYY-MM-DD). Default: use latest per season.')
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    print(
        f">>> bucket={args.bucket} season_min={args.season_min} season_max={args.season_max} "
        f"endpoint={args.endpoint} asof={args.asof}",
        flush=True
    )
    main(
        bucket=args.bucket,
        season_min=args.season_min,
        season_max=args.season_max,
        endpoint=args.endpoint,
        asof=args.asof,
    )
