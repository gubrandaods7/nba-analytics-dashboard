# app/app.py
import os
import re
import tempfile
from pathlib import Path
from datetime import date

import pandas as pd
import streamlit as st
from google.cloud import storage

# -----------------------------
# Page config
# -----------------------------
st.set_page_config(
    page_title="NBA Analytics Dashboard",
    page_icon="🏀",
    layout="wide",
)

# -----------------------------
# SSL (corporate) - optional but recommended
# -----------------------------
CA_PATH = Path(__file__).resolve().parents[1] / "certs" / "combined_ca.pem"
if CA_PATH.exists():
    os.environ["REQUESTS_CA_BUNDLE"] = str(CA_PATH)
    os.environ["SSL_CERT_FILE"] = str(CA_PATH)

# -----------------------------
# Defaults
# -----------------------------
DEFAULT_BUCKET = "nba-data-gustavo"
DEFAULT_SEASON = "2025-26"
DEFAULT_ASOF = date.today().strftime("%Y-%m-%d")

# -----------------------------
# Helpers
# -----------------------------
def parse_gs_uri(gs_uri: str) -> tuple[str, str]:
    if not gs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {gs_uri}")
    parts = gs_uri.split("/", 3)
    bucket_name = parts[2]
    blob_path = parts[3] if len(parts) > 3 else ""
    return bucket_name, blob_path


@st.cache_resource(show_spinner=False)
def get_gcs_client() -> storage.Client:
    return storage.Client()


def _list_prefixes(bucket: storage.Bucket, prefix: str) -> list[str]:
    """
    Return 'subfolders' immediately under prefix using delimiter.
    """
    it = bucket.list_blobs(prefix=prefix, delimiter="/")
    _ = list(it)  # exhaust to populate .prefixes
    return sorted([p for p in it.prefixes])


@st.cache_data(ttl=3600, show_spinner=False)
def list_gold_seasons(bucket_name: str) -> list[str]:
    """
    Seasons available under gold/season=YYYY-YY/
    """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)

    prefixes = _list_prefixes(bucket, prefix="gold/")
    seasons = []
    for p in prefixes:
        # p looks like "gold/season=2025-26/"
        m = re.search(r"gold/season=([^/]+)/", p)
        if m:
            seasons.append(m.group(1))
    seasons = sorted(seasons, reverse=True)
    return seasons


@st.cache_data(ttl=3600, show_spinner=False)
def list_raw_asof_dates(bucket_name: str, season: str) -> list[str]:
    """
    ASOF snapshots available in raw for at least one endpoint.
    We scan raw/season=YYYY-YY/ and collect asof=YYYY-MM-DD from blob paths.
    """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)

    # list blobs under raw/season=... (bounded)
    prefix = f"raw/season={season}/"
    blobs = bucket.list_blobs(prefix=prefix)

    dates = set()
    # Example path:
    # raw/season=2025-26/endpoint=leaguegamelog/asof=2026-02-10/data.parquet
    for b in blobs:
        m = re.search(r"/asof=(\d{4}-\d{2}-\d{2})/", b.name)
        if m:
            dates.add(m.group(1))

    # Return newest first
    return sorted(dates, reverse=True)


@st.cache_data(ttl=3600, show_spinner=False)
def read_parquet_from_gcs(gs_uri: str) -> pd.DataFrame:
    client = get_gcs_client()
    bucket_name, blob_path = parse_gs_uri(gs_uri)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    tmp_dir = Path(tempfile.gettempdir())
    local_path = tmp_dir / (Path(blob_path).name or "data.parquet")

    blob.download_to_filename(str(local_path))
    df = pd.read_parquet(local_path, engine="pyarrow")

    try:
        local_path.unlink()
    except Exception:
        pass

    return df


def safe_metric(df_kpis: pd.DataFrame, col: str):
    if df_kpis is None or df_kpis.empty or col not in df_kpis.columns:
        return None
    val = df_kpis.iloc[0][col]
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    return val


def fmt_int(x):
    if x is None:
        return "—"
    try:
        return f"{int(x):,}".replace(",", ".")
    except Exception:
        return str(x)


# -----------------------------
# Sidebar (controls)
# -----------------------------
st.sidebar.header("Configurações")

bucket = st.sidebar.text_input("GCS bucket", value=DEFAULT_BUCKET)

# Seasons dropdown (auto-discovery) with fallback to manual input
seasons = []
try:
    seasons = list_gold_seasons(bucket)
except Exception:
    seasons = []

if seasons:
    default_idx = seasons.index(DEFAULT_SEASON) if DEFAULT_SEASON in seasons else 0
    season = st.sidebar.selectbox("Season", options=seasons, index=default_idx)
else:
    st.sidebar.warning("Não consegui listar seasons automaticamente em gold/. Você pode digitar manualmente.")
    season = st.sidebar.text_input("Season (manual)", value=DEFAULT_SEASON)

# ASOF dropdown from RAW (auto) + "Hoje"
asof_options = []
try:
    asof_options = list_raw_asof_dates(bucket, season)
except Exception:
    asof_options = []

# Always allow today as a choice (even if not ingested yet)
today_str = DEFAULT_ASOF
if today_str not in asof_options:
    asof_options = [today_str] + asof_options

asof = st.sidebar.selectbox("ASOF (snapshot)", options=asof_options, index=0)

col_a, col_b = st.sidebar.columns(2)
with col_a:
    ttl_hours = st.number_input("Cache (h)", min_value=0, max_value=24, value=1, step=1)
with col_b:
    refresh = st.button("🔄 Recarregar")

st.sidebar.caption(
    "Season lista do GOLD. ASOF lista do RAW (snapshots). "
    "O GOLD é 'latest' por season (sobrescrito). O ASOF é usado para filtrar se existir coluna ASOF."
)

# Clear cache if requested
if refresh:
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()

# -----------------------------
# Paths (gold)
# -----------------------------
kpis_uri = f"gs://{bucket}/gold/season={season}/kpis.parquet"
team_uri = f"gs://{bucket}/gold/season={season}/team_totals.parquet"
stand_uri = f"gs://{bucket}/gold/season={season}/standings.parquet"

# -----------------------------
# Load data
# -----------------------------
st.title("🏀 NBA Analytics Dashboard")
st.caption("Dados pré-processados (GOLD) no GCS. Sem chamadas em tempo real para a NBA API.")

with st.expander("Ver fontes (GCS URIs)"):
    st.code("\n".join([kpis_uri, team_uri, stand_uri]), language="text")

load_error = None
df_kpis = df_team = df_stand = None

# (Optional) Make cache TTL configurable by user
# Streamlit cache TTL is defined at decorator time, so we emulate by including ttl_hours in function key.
@st.cache_data(ttl=3600, show_spinner=False)
def _load_gold(kpis_uri: str, team_uri: str, stand_uri: str, ttl_key: int):
    dk = read_parquet_from_gcs(kpis_uri)
    dt = read_parquet_from_gcs(team_uri)
    ds = read_parquet_from_gcs(stand_uri)
    return dk, dt, ds

with st.spinner("Carregando dados do GCS..."):
    try:
        df_kpis, df_team, df_stand = _load_gold(kpis_uri, team_uri, stand_uri, int(ttl_hours))
    except Exception as e:
        load_error = e

if load_error:
    st.error("Falha ao carregar dados do GCS.")
    st.exception(load_error)
    st.stop()

# Filter by ASOF if columns exist (future-proof)
if df_kpis is not None and "ASOF" in df_kpis.columns:
    df_kpis = df_kpis[df_kpis["ASOF"].astype(str) == str(asof)]
if df_team is not None and "ASOF" in df_team.columns:
    df_team = df_team[df_team["ASOF"].astype(str) == str(asof)]
if df_stand is not None and "ASOF" in df_stand.columns:
    df_stand = df_stand[df_stand["ASOF"].astype(str) == str(asof)]

# -----------------------------
# KPI Row
# -----------------------------
st.subheader("KPIs da Liga")

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("PTS (total)", fmt_int(safe_metric(df_kpis, "TOTAL_PTS")))
c2.metric("AST (total)", fmt_int(safe_metric(df_kpis, "TOTAL_AST")))
c3.metric("REB (total)", fmt_int(safe_metric(df_kpis, "TOTAL_REB")))
c4.metric("STL (total)", fmt_int(safe_metric(df_kpis, "TOTAL_STL")))
c5.metric("BLK (total)", fmt_int(safe_metric(df_kpis, "TOTAL_BLK")))
c6.metric("TOV (total)", fmt_int(safe_metric(df_kpis, "TOTAL_TOV")))

st.caption(f"Season: **{season}** | ASOF selecionado: **{asof}** | Bucket: **{bucket}**")
st.divider()

# -----------------------------
# Team Totals + filters
# -----------------------------
st.subheader("Equipe — Totais & Ranking")

if df_team is None or df_team.empty:
    st.warning("team_totals.parquet está vazio.")
else:
    df_team_view = df_team.copy()

    # pick label columns
    label_col = "TEAM_NAME" if "TEAM_NAME" in df_team_view.columns else (
        "TEAM_ABBREVIATION" if "TEAM_ABBREVIATION" in df_team_view.columns else None
    )

    # choose metric for chart
    metric_candidates = [c for c in ["PTS", "AST", "REB", "STL", "BLK", "TOV"] if c in df_team_view.columns]
    if not metric_candidates:
        st.warning("Não encontrei colunas de métricas (PTS/AST/REB/...). Vou mostrar a tabela completa.")
        st.dataframe(df_team_view, use_container_width=True, hide_index=True)
    else:
        left, right = st.columns([2, 1])

        with right:
            metric = st.selectbox("Métrica (ranking)", options=metric_candidates, index=0)
            top_n = st.number_input("Top N", min_value=5, max_value=30, value=15, step=1)
            show_table = st.toggle("Mostrar tabela", value=True)

            # Team filter
            if label_col:
                teams = sorted(df_team_view[label_col].dropna().astype(str).unique().tolist())
                selected_teams = st.multiselect("Filtrar times", options=teams, default=[])
            else:
                selected_teams = []

        # apply team filter
        if label_col and selected_teams:
            df_team_view = df_team_view[df_team_view[label_col].astype(str).isin(selected_teams)]

        # numeric metric
        df_team_view[metric] = pd.to_numeric(df_team_view[metric], errors="coerce").fillna(0)
        df_team_view = df_team_view.sort_values(metric, ascending=False)

        with left:
            df_top = df_team_view.head(int(top_n))
            if label_col:
                st.bar_chart(df_top.set_index(label_col)[metric])
            else:
                st.bar_chart(df_top[metric])

        if show_table:
            show_cols = [c for c in [label_col, "TEAM_ABBREVIATION", "PTS", "AST", "REB", "STL", "BLK", "TOV"] if c and c in df_team_view.columns]
            if show_cols:
                st.dataframe(df_team_view[show_cols], use_container_width=True, hide_index=True)
            else:
                st.dataframe(df_team_view, use_container_width=True, hide_index=True)

st.divider()

# -----------------------------
# Standings + conference filter
# -----------------------------
st.subheader("Standings")

if df_stand is None or df_stand.empty:
    st.warning("standings.parquet está vazio.")
else:
    df_view = df_stand.copy()

    # Conference filter if possible
    if "Conference" in df_view.columns:
        confs = ["All"] + sorted(df_view["Conference"].dropna().astype(str).unique().tolist())
        conf = st.selectbox("Conference", options=confs, index=0)
        if conf != "All":
            df_view = df_view[df_view["Conference"].astype(str) == conf]

    preferred = [
        "TeamCity", "TeamName", "Conference", "PlayoffRank", "Wins", "Losses", "WinPCT",
        "HomeWins", "HomeLosses", "AwayWins", "AwayLosses",
        "LastTenWins", "LastTenLosses", "Streak", "PointsFor", "PointsAgainst"
    ]
    cols = [c for c in preferred if c in df_view.columns]

    # Sorting
    if "Conference" in df_view.columns and "PlayoffRank" in df_view.columns:
        df_view["PlayoffRank"] = pd.to_numeric(df_view["PlayoffRank"], errors="coerce")
        df_view = df_view.sort_values(["Conference", "PlayoffRank"], ascending=[True, True])
    elif "WinPCT" in df_view.columns:
        df_view["WinPCT"] = pd.to_numeric(df_view["WinPCT"], errors="coerce")
        df_view = df_view.sort_values("WinPCT", ascending=False)

    st.dataframe(df_view[cols] if cols else df_view, use_container_width=True, hide_index=True)

# -----------------------------
# Footer / Debug
# -----------------------------
with st.expander("Debug"):
    st.write("kpis shape:", df_kpis.shape if df_kpis is not None else None)
    st.write("team_totals shape:", df_team.shape if df_team is not None else None)
    st.write("standings shape:", df_stand.shape if df_stand is not None else None)
    st.caption(
        "Este app baixa os arquivos do GCS para uma pasta temporária e lê localmente (mais robusto em Windows/proxy corporativo)."
    )
