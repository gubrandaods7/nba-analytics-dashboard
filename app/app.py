# app/app.py
import os
import re
import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st
import altair as alt
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
# Config (fixed)
# -----------------------------
BUCKET = "nba-data-gustavo"
DEFAULT_SEASON = "2025-26"

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
    it = bucket.list_blobs(prefix=prefix, delimiter="/")
    _ = list(it)  # exhaust to populate .prefixes
    return sorted(list(it.prefixes))


def list_gold_seasons(bucket_name: str) -> list[str]:
    """
    Seasons available under gold/season=YYYY-YY/
    """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)

    prefixes = _list_prefixes(bucket, prefix="gold/")
    seasons = []
    for p in prefixes:
        m = re.search(r"gold/season=([^/]+)/", p)
        if m:
            seasons.append(m.group(1))
    return sorted(seasons, reverse=True)


def read_parquet_from_gcs(gs_uri: str) -> pd.DataFrame:
    """
    Robust approach:
      - download parquet from GCS to local temp file
      - read locally with pyarrow
    Avoids pd.read_parquet("gs://...") issues in some Windows/proxy setups.
    """
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
# Sidebar (only Season)
# -----------------------------
st.sidebar.header("Temporada")

try:
    seasons = list_gold_seasons(BUCKET)
except Exception:
    seasons = []

if seasons:
    default_idx = seasons.index(DEFAULT_SEASON) if DEFAULT_SEASON in seasons else 0
    season = st.sidebar.selectbox("Season", options=seasons, index=default_idx)
else:
    # fallback if listing fails
    season = st.sidebar.text_input("Season", value=DEFAULT_SEASON)

# -----------------------------
# Paths (gold)
# -----------------------------
kpis_uri = f"gs://{BUCKET}/gold/season={season}/kpis.parquet"
team_uri = f"gs://{BUCKET}/gold/season={season}/team_totals.parquet"
stand_uri = f"gs://{BUCKET}/gold/season={season}/standings.parquet"
league_hist_uri = f"gs://{BUCKET}/gold/league_season_kpis.parquet"

# -----------------------------
# Load data
# -----------------------------
st.title("🏀 NBA Analytics Dashboard")
st.caption("Dados pré-processados (GOLD) no GCS. Sem chamadas em tempo real para a NBA API.")

load_error = None
df_kpis = df_team = df_stand = df_hist = None

with st.spinner("Carregando dados..."):
    try:
        df_kpis = read_parquet_from_gcs(kpis_uri)
        df_team = read_parquet_from_gcs(team_uri)
        df_stand = read_parquet_from_gcs(stand_uri)
        # histórico pode não existir em alguns momentos
        try:
            df_hist = read_parquet_from_gcs(league_hist_uri)
        except Exception:
            df_hist = pd.DataFrame()
    except Exception as e:
        load_error = e

if load_error:
    st.error("Falha ao carregar dados do GCS (GOLD).")
    st.exception(load_error)
    st.stop()

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

st.caption(f"Season: **{season}**")
st.divider()

# -----------------------------
# League History (Season-over-Season)
# -----------------------------
st.subheader("📈 Evolução da Liga por Temporada")

if df_hist is None or df_hist.empty or "season" not in df_hist.columns:
    st.info("Histórico por temporada ainda não disponível. Gere com o ETL 04 (league_season_kpis).")
else:
    preferred_metrics = [
        ("pts_per_game", "PTS por jogo"),
        ("ast_per_game", "AST por jogo"),
        ("reb_per_game", "REB por jogo"),
        ("tov_per_game", "TOV por jogo"),
        ("fg3m_per_game", "3PTM por jogo"),
        ("fg3a_per_game", "3PTA por jogo"),
        ("total_pts", "PTS total"),
        ("total_ast", "AST total"),
        ("total_reb", "REB total"),
        ("total_tov", "TOV total"),
        ("total_fg3m", "3PTM total"),
        ("total_fg3a", "3PTA total"),
    ]

    # only show metrics that actually have any valid data
    available = []
    for col, label in preferred_metrics:
        if col in df_hist.columns:
            s = pd.to_numeric(df_hist[col], errors="coerce")
            if s.notna().any() and s.fillna(0).sum() != 0:
                available.append((col, label))

    if not available:
        st.warning("Histórico encontrado, mas sem métricas válidas para plotar.")
    else:
        seasons_all = sorted(df_hist["season"].astype(str).unique().tolist())
        col_to_label = {c: l for c, l in available}

        left, right = st.columns([2, 3])
        with left:
            metric_col = st.selectbox(
                "Métrica",
                options=[c for c, _ in available],
                format_func=lambda c: col_to_label.get(c, c),
                index=0,
                key="hist_metric",
            )
        with right:
            selected_seasons = st.multiselect(
                "Seasons",
                options=seasons_all,
                default=seasons_all,
                key="hist_seasons",
            )

        dfp = df_hist.copy()
        dfp["season"] = dfp["season"].astype(str)
        dfp = dfp[dfp["season"].isin(selected_seasons)]
        dfp[metric_col] = pd.to_numeric(dfp[metric_col], errors="coerce")
        dfp = dfp.dropna(subset=["season", metric_col])

        if dfp.empty:
            st.warning("Sem dados após filtros.")
        else:
            chart = (
                alt.Chart(dfp)
                .mark_line(point=True)
                .encode(
                    x=alt.X("season:N", title="Season", sort=seasons_all),
                    y=alt.Y(f"{metric_col}:Q", title=col_to_label.get(metric_col, metric_col)),
                    tooltip=["season:N", alt.Tooltip(f"{metric_col}:Q", format=",.2f")],
                )
                .interactive()
            )
            st.altair_chart(chart, use_container_width=True)

st.divider()

# -----------------------------
# Teams (Ranking)
# -----------------------------
st.subheader("Times — Ranking")

if df_team is None or df_team.empty:
    st.warning("team_totals.parquet está vazio.")
else:
    df_team_view = df_team.copy()

    label_col = "TEAM_NAME" if "TEAM_NAME" in df_team_view.columns else (
        "TEAM_ABBREVIATION" if "TEAM_ABBREVIATION" in df_team_view.columns else None
    )

    metric_candidates = [c for c in ["PTS", "AST", "REB", "STL", "BLK", "TOV", "FG3M", "FG3A"] if c in df_team_view.columns]

    if not metric_candidates:
        st.warning("Não encontrei colunas de métricas (PTS/AST/REB/...).")
        st.dataframe(df_team_view, use_container_width=True, hide_index=True)
    else:
        left, right = st.columns([2, 1])

        with right:
            metric = st.selectbox("Métrica", options=metric_candidates, index=0, key="team_metric")
            top_n = st.number_input("Top N", min_value=5, max_value=30, value=15, step=1, key="team_topn")

            if label_col:
                teams = sorted(df_team_view[label_col].dropna().astype(str).unique().tolist())
                selected_teams = st.multiselect("Times (opcional)", options=teams, default=[], key="team_filter")
            else:
                selected_teams = []

        if label_col and selected_teams:
            df_team_view = df_team_view[df_team_view[label_col].astype(str).isin(selected_teams)]

        df_team_view[metric] = pd.to_numeric(df_team_view[metric], errors="coerce").fillna(0)
        df_team_view = df_team_view.sort_values(metric, ascending=False)

        with left:
            df_top = df_team_view.head(int(top_n))
            if label_col:
                st.bar_chart(df_top.set_index(label_col)[metric])
            else:
                st.bar_chart(df_top[metric])

        with st.expander("Ver tabela"):
            show_cols = [c for c in [label_col, "TEAM_ABBREVIATION", "PTS", "AST", "REB", "STL", "BLK", "TOV", "FG3M", "FG3A"] if c and c in df_team_view.columns]
            st.dataframe(df_team_view[show_cols] if show_cols else df_team_view, use_container_width=True, hide_index=True)

st.divider()

# -----------------------------
# Standings
# -----------------------------
st.subheader("Standings")

if df_stand is None or df_stand.empty:
    st.warning("standings.parquet está vazio.")
else:
    df_view = df_stand.copy()

    # Keep it simple: show best available columns
    preferred = [
        "TeamCity", "TeamName", "Conference", "PlayoffRank", "Wins", "Losses", "WinPCT",
        "HomeWins", "HomeLosses", "AwayWins", "AwayLosses",
        "LastTenWins", "LastTenLosses", "Streak", "PointsFor", "PointsAgainst"
    ]
    cols = [c for c in preferred if c in df_view.columns]

    if "Conference" in df_view.columns and "PlayoffRank" in df_view.columns:
        df_view["PlayoffRank"] = pd.to_numeric(df_view["PlayoffRank"], errors="coerce")
        df_view = df_view.sort_values(["Conference", "PlayoffRank"], ascending=[True, True])
    elif "WinPCT" in df_view.columns:
        df_view["WinPCT"] = pd.to_numeric(df_view["WinPCT"], errors="coerce")
        df_view = df_view.sort_values("WinPCT", ascending=False)

    st.dataframe(df_view[cols] if cols else df_view, use_container_width=True, hide_index=True)
