import pandas as pd
import streamlit as st

BUCKET = "SEU_BUCKET_AQUI"
SEASON = "2025-26"

kpis_path  = f"gs://{BUCKET}/gold/season={SEASON}/kpis.parquet"
team_path  = f"gs://{BUCKET}/gold/season={SEASON}/team_totals.parquet"
stand_path = f"gs://{BUCKET}/gold/season={SEASON}/standings.parquet"

st.set_page_config(page_title="NBA Dashboard", layout="wide")
st.title(f"NBA Dashboard — Temporada {SEASON}")

@st.cache_data(ttl=600)
def load_data():
    kpis = pd.read_parquet(kpis_path)
    team = pd.read_parquet(team_path)
    stand = pd.read_parquet(stand_path)
    return kpis, team, stand

kpis, team, stand = load_data()

# Cards
row = kpis.iloc[0].to_dict()
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Pontos (liga)",  f"{int(row['pts']):,}" if row.get("pts") else "—")
c2.metric("Assists (liga)", f"{int(row['ast']):,}" if row.get("ast") else "—")
c3.metric("Rebotes (liga)", f"{int(row['reb']):,}" if row.get("reb") else "—")
c4.metric("Blocks (liga)",  f"{int(row['blk']):,}" if row.get("blk") else "—")
c5.metric("Steals (liga)",  f"{int(row['stl']):,}" if row.get("stl") else "—")

st.caption(f"Snapshot (asof): {row.get('asof', '—')}  |  Jogos: {row.get('games', '—')}")

# Gráfico: pontos por time
st.subheader("Pontos por time (acumulado na temporada)")
team_cols = list(team.columns)
team_key = team_cols[0]
if "PTS" in team.columns:
    chart_df = team[[team_key, "PTS"]].set_index(team_key).sort_values("PTS", ascending=False)
    st.bar_chart(chart_df)
else:
    st.info("Coluna PTS não encontrada no team_totals. Verifique o LeagueGameLog e as colunas agregadas.")

# Standings
st.subheader("Classificação (Standings)")
st.dataframe(stand, use_container_width=True)
