from datetime import date
import pandas as pd

from nba_api.stats.endpoints import leaguegamelog, leaguestandingsv3

SEASON = "2025-26"
BUCKET = "SEU_BUCKET_AQUI"
ASOF = date.today().strftime("%Y-%m-%d")

raw_games_path = f"gs://{BUCKET}/raw/season={SEASON}/endpoint=leaguegamelog/asof={ASOF}/data.parquet"
raw_stand_path = f"gs://{BUCKET}/raw/season={SEASON}/endpoint=leaguestandingsv3/asof={ASOF}/data.parquet"

def main():
    # 1) LeagueGameLog (base para métricas por time + totais da liga)
    lg = leaguegamelog.LeagueGameLog(season=SEASON)
    df_games = lg.get_data_frames()[0]
    df_games.to_parquet(raw_games_path, index=False)

    # 2) Standings (tabela atual)
    st = leaguestandingsv3.LeagueStandingsV3()
    df_stand = st.get_data_frames()[0]
    df_stand.to_parquet(raw_stand_path, index=False)

    print("RAW salvo no GCS:")
    print(raw_games_path)
    print(raw_stand_path)
    print("Colunas LeagueGameLog:", list(df_games.columns))

if __name__ == "__main__":
    main()
