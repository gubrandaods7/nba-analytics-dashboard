# etl/03_backfill_seasons.py
import argparse
import subprocess
import sys
from datetime import date

CURRENT_SEASON_SAFETY = "2025-26"  # não mexer por padrão

def season_str(year_start: int) -> str:
    # 2015 -> "2015-16"
    return f"{year_start}-{str(year_start + 1)[-2:]}"

def run(cmd: list[str]) -> int:
    print("\n>>> " + " ".join(cmd), flush=True)
    return subprocess.call(cmd)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--from_year", type=int, required=True, help="Ex: 2015 para season 2015-16")
    p.add_argument("--to_year", type=int, required=True, help="Ex: 2024 para season 2024-25")
    p.add_argument("--asof", type=str, default=date.today().strftime("%Y-%m-%d"))
    p.add_argument("--bucket", default="nba-data-gustavo")
    p.add_argument("--include_current", action="store_true", help="Se passar, permite rodar também a season atual.")
    args = p.parse_args()

    failures = []

    for y in range(args.from_year, args.to_year + 1):
        season = season_str(y)

        if (season == CURRENT_SEASON_SAFETY) and (not args.include_current):
            print(f">>> Skipping current season {CURRENT_SEASON_SAFETY} for safety", flush=True)
            continue

        print(f"\n===== SEASON {season} =====", flush=True)

        rc1 = run([sys.executable, "-u", "etl/01_pull_raw.py",
                   "--season", season, "--asof", args.asof, "--bucket", args.bucket])
        if rc1 != 0:
            failures.append((season, "01_pull_raw", rc1))
            continue

        rc2 = run([sys.executable, "-u", "etl/02_build_gold.py",
                   "--season", season, "--asof", args.asof, "--bucket", args.bucket])
        if rc2 != 0:
            failures.append((season, "02_build_gold", rc2))
            continue

    print("\n===== DONE =====", flush=True)
    if failures:
        print("Failures:", flush=True)
        for season, step, rc in failures:
            print(f"- {season}: {step} (exit={rc})", flush=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
