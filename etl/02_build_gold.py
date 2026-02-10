import pandas as pd

SEASON = "2025-26"
BUCKET = "SEU_BUCKET_AQUI"

# Escolha qual snapshot usar
ASOF = pd.Timestamp.today().strftime("%Y-%m-%d")

raw_games_path = f"gs://{BUCKET}/raw/season={SEASON}/endpoint=leaguegamelog/asof={ASOF}/data.parquet"
raw_stand_path = f"gs://{BUCKET}/raw/season={SEASON}/endpoint=leaguestandingsv3/asof={ASOF}/data.parquet"

gold_kpis_path = f"gs://{BUCKET}/gold/season={SEASON}/kpis.parquet"
gold_team_path = f"gs://{BUCKET}/gold/season={SEASON}/team_totals.parquet"
gold_stand_path = f"gs://{BUCKET}/gold/season={SEASON}/standings.parquet"

def safe_sum(df: pd.DataFrame, col: str):
    return df[col].sum() if col in df.columns else None

def main():
    df_games = pd.read_parquet(raw_games_path)
    df_stand = pd.read_parquet(raw_stand_path)

    # KPIs da liga
    # Atenção: LeagueGameLog costuma ter stats no nível "time por jogo".
    # Somar PTS/AST/REB aqui dá "total da liga" (somando todos os times em todos os jogos).
    kpis = pd.DataFrame([{
        "season": SEASON,
        "asof": ASOF,
        "games": df_games["GAME_ID"].nunique() if "GAME_ID" in df_games.columns else None,
        "pts": safe_sum(df_games, "PTS"),
        "ast": safe_sum(df_games, "AST"),
        "reb": safe_sum(df_games, "REB"),
        "stl": safe_sum(df_games, "STL"),
        "blk": safe_sum(df_games, "BLK"),
        "tov": safe_sum(df_games, "TOV"),
    }])

    # Totais por time
    team_key = "TEAM_ABBREVIATION" if "TEAM_ABBREVIATION" in df_games.columns else "TEAM_ID"
    agg_cols = {}
    for c in ["PTS", "AST", "REB", "STL", "BLK", "TOV"]:
        if c in df_games.columns:
            agg_cols[c] = "sum"

    team_totals = (
        df_games
        .groupby(team_key, as_index=False)
        .agg(agg_cols)
        .sort_values("PTS", ascending=False) if "PTS" in agg_cols else df_games.groupby(team_key, as_index=False).size()
    )

    # Standings ordenado (quando tiver Conference e PlayoffRank)
    standings = df_stand.copy()
    if "Conference" in standings.columns and "PlayoffRank" in standings.columns:
        standings = standings.sort_values(["Conference", "PlayoffRank"])

    # Salvar gold
    kpis.to_parquet(gold_kpis_path, index=False)
    team_totals.to_parquet(gold_team_path, index=False)
    standings.to_parquet(gold_stand_path, index=False)

    print("GOLD salvo no GCS:")
    print(gold_kpis_path)
    print(gold_team_path)
    print(gold_stand_path)

if __name__ == "__main__":
    main()
