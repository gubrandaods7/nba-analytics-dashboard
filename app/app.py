# app/app.py
import os
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
    # Applies to google-auth/requests + any requests usage under the hood
    os.environ["REQUESTS_CA_BUNDLE"] = str(CA_PATH)
    os.environ["SSL_CERT_FILE"] = str(CA_PATH)

# -----------------------------
# Config
# -----------------------------
DEFAULT_BUCKET = "nba-data-gustavo"
DEFAULT_SEASON = "2025-26"

# If you want the app to always read "today's" snapshot, keep this.
# If you prefer a fixed snapshot for demo purposes, set a fixed string like "2026-02-10".
DEFAULT_ASOF = date.today().strftime("%Y-%m-%d")

# -----------------------------
# Helpers
# -----------------------------
def parse_gs_uri(gs_uri: str) -> tuple[str, str]:
    if not gs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {gs_uri}")
    parts = gs_uri.split("/", 3)  # ["gs:", "", "bucket", "path..."]
    bucket_name = parts[2]
    blob_path = parts[3] if len(parts) > 3 else ""
    return bucket_name, blob_path


@st.cache_resource(show_spinner=False)
def get_gcs_client() -> storage.Client:
    # Uses GOOGLE_APPLICATION_CREDENTIALS if set
    return storage.Client()


@st.cache_data(ttl=3600, show_spinner=False)
def read_parquet_from_gcs(gs_uri: str) -> pd.DataFrame:
    """
    Robust approach:
      - download parquet from GCS to local temp file
      - read locally with pyarrow
      - cache result for 1h
    Avoids pd.read_parquet("gs://...") which can hang in some Windows/corporate setups.
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


def safe_metric(df_kpis: pd.DataFrame, col: str) -> float | int | None:
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
season = st.sidebar.text_input("Season", value=DEFAULT_SEASON)
asof = st.sidebar.text_input("ASOF (YYYY-MM-DD)", value=DEFAULT_ASOF)

st.sidebar.caption("Dica: se você quer o snapshot fixo do ETL, use `2026-02-10` (ou a data que você rodou).")

# Paths (gold)
kpis_uri = f"gs://{bucket}/gold/season={season}/kpis.parquet"
team_uri = f"gs://{bucket}/gold/season={season}/team_totals.parquet"
stand_uri = f"gs://{bucket}/gold/season={season}/standings.parquet"

# -----------------------------
# Load data
# -----------------------------
st.title("🏀 NBA Analytics Dashboard")
st.caption("Dados pré-processados (GOLD) no GCS. Sem chamadas em tempo real para a NBA API.")

# Helpful UX: show current URIs
with st.expander("Ver fontes (GCS URIs)"):
    st.code("\n".join([kpis_uri, team_uri, stand_uri]), language="text")

load_error = None
df_kpis = df_team = df_stand = None

with st.spinner("Carregando dados do GCS..."):
    try:
        df_kpis = read_parquet_from_gcs(kpis_uri)
        df_team = read_parquet_from_gcs(team_uri)
        df_stand = read_parquet_from_gcs(stand_uri)
    except Exception as e:
        load_error = e

if load_error:
    st.error("Falha ao carregar dados do GCS.")
    st.exception(load_error)
    st.stop()

# Optional: validate ASOF/SEASON columns if present
if "ASOF" in df_team.columns:
    # filter to current ASOF if multiple snapshots ever exist in same file (future-proof)
    df_team = df_team[df_team["ASOF"].astype(str) == str(asof)]
if "ASOF" in df_stand.columns:
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

st.caption(f"Season: **{season}** | ASOF: **{asof}**")

st.divider()

# -----------------------------
# Team Totals
# -----------------------------
st.subheader("Pontos por Equipe (acumulado)")

# Basic cleaning/sorting
team_cols_needed = [c for c in ["TEAM_NAME", "TEAM_ABBREVIATION", "PTS"] if c in df_team.columns]
if not team_cols_needed or "PTS" not in df_team.columns:
    st.warning("`team_totals.parquet` não contém as colunas esperadas (ex.: PTS).")
else:
    df_team_view = df_team.copy()

    # Ensure numeric
    df_team_view["PTS"] = pd.to_numeric(df_team_view["PTS"], errors="coerce").fillna(0)

    # Sort and select
    df_team_view = df_team_view.sort_values("PTS", ascending=False)

    left, right = st.columns([2, 1])

    with right:
        top_n = st.number_input("Top N", min_value=5, max_value=30, value=15, step=1)
        show_table = st.toggle("Mostrar tabela", value=True)

    with left:
        df_top = df_team_view.head(int(top_n))
        # Use team name when present; fallback to abbreviation
        label_col = "TEAM_NAME" if "TEAM_NAME" in df_top.columns else "TEAM_ABBREVIATION"

        st.bar_chart(df_top.set_index(label_col)["PTS"])

    if show_table:
        show_cols = [c for c in ["TEAM_NAME", "TEAM_ABBREVIATION", "PTS", "AST", "REB", "STL", "BLK", "TOV"] if c in df_team_view.columns]
        st.dataframe(df_team_view[show_cols], use_container_width=True, hide_index=True)

st.divider()

# -----------------------------
# Standings
# -----------------------------
st.subheader("Standings")

if df_stand is None or df_stand.empty:
    st.warning("standings.parquet está vazio.")
else:
    # Try to pick useful columns without assuming exact schema
    preferred = [
        "TeamCity", "TeamName", "Conference", "PlayoffRank", "Wins", "Losses", "WinPCT",
        "HomeWins", "HomeLosses", "AwayWins", "AwayLosses",
        "LastTenWins", "LastTenLosses", "Streak", "PointsFor", "PointsAgainst"
    ]
    cols = [c for c in preferred if c in df_stand.columns]

    # Fallback: show everything if schema differs
    if not cols:
        st.dataframe(df_stand, use_container_width=True, hide_index=True)
    else:
        # Sorting: if PlayoffRank exists, sort by conference + rank
        df_view = df_stand.copy()

        if "Conference" in df_view.columns and "PlayoffRank" in df_view.columns:
            # Some datasets store rank as string; coerce numeric
            df_view["PlayoffRank"] = pd.to_numeric(df_view["PlayoffRank"], errors="coerce")
            df_view = df_view.sort_values(["Conference", "PlayoffRank"], ascending=[True, True])
        elif "WinPCT" in df_view.columns:
            df_view["WinPCT"] = pd.to_numeric(df_view["WinPCT"], errors="coerce")
            df_view = df_view.sort_values("WinPCT", ascending=False)

        st.dataframe(df_view[cols], use_container_width=True, hide_index=True)

# -----------------------------
# Footer / Debug
# -----------------------------
with st.expander("Debug"):
    st.write("kpis shape:", df_kpis.shape if df_kpis is not None else None)
    st.write("team_totals shape:", df_team.shape if df_team is not None else None)
    st.write("standings shape:", df_stand.shape if df_stand is not None else None)
    st.caption("Se o load demorar, quase sempre é rede/autenticação. Este app baixa os arquivos para temp e lê localmente (mais robusto).")
