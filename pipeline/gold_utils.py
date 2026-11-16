"""
gold_utils.py
Functions used only by the Gold layer (NHL skater game stats).

Silver is expected to provide these columns (snake_case, one row per skater per game):
  game_id, player_id, skater_name, team, opponent, home_road, is_home,
  position, position_group, shoots, game_date, game_dow,
  goals, assists, points, plus_minus, pim, shots, shooting_pct,
  ev_goals, ev_points, pp_goals, pp_points, sh_goals, sh_points,
  ot_goals, gw_goals, faceoff_win_pct, toi_min, points_per_60, shots_per_60, load_ts
"""

from datetime import timedelta

import pandas as pd

import config
import utils

log = utils.get_logger("gold")

# Columns kept in the row-level Gold table (one row per skater per game)
GAME_COLUMNS = [
    "game_id", "game_date", "game_dow", "player_id", "skater_name", "team", "opponent",
    "home_road", "is_home", "position", "position_group", "shoots",
    "goals", "assists", "points", "plus_minus", "pim", "shots", "shooting_pct",
    "ev_points", "pp_goals", "pp_points", "sh_points", "gw_goals", "ot_goals",
    "faceoff_win_pct", "toi_min", "points_per_60", "shots_per_60", "load_ts",
]


def days_to_rebuild(fs):
    """Today and the previous GOLD_REBUILD_DAYS-1 days, as 'YYYY-MM-DD' strings.
    Three days covers the one-day Bronze overlap plus late stat corrections.

    Exception: if Gold has no skater_games yet (first run, or after you delete
    Gold to rebuild it), build every day Silver has. Otherwise player_form's
    season totals would only ever contain games from the first 3 days onward."""
    if not fs.glob(f"{config.GOLD_PATH}/skater_games/game_date=*"):
        days = sorted(p.split("game_date=")[1][:10]
                      for p in fs.glob(f"{config.SILVER_PATH}/game_date=*"))
        log.info("Gold is empty: backfilling all %d Silver days", len(days))
        return days
    today = utils.now_utc().date()
    days = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(config.GOLD_REBUILD_DAYS)]
    log.info("Rebuilding Gold for %s", days)
    return days


def read_silver(fs, days):
    """Every Silver partition for the given days, as one DataFrame.
    The NHL has days with no games (off nights, holidays, All-Star break),
    so a missing partition is normal and is skipped."""
    frames = []
    for day in days:
        df = utils.read_parquet_folder(fs, f"{config.SILVER_PATH}/game_date={day}/*.parquet")
        if not df.empty:
            df["game_date"] = day                      # partition value, in case it was dropped from the file
            frames.append(df)
    silver = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=GAME_COLUMNS)
    log.info("Read %d Silver rows", len(silver))
    return silver


# ---------- Daily tables: one function per table, each takes one day of Silver ----------
def build_skater_games(s):
    """Row-level stat lines for the player detail view and filters."""
    return s[GAME_COLUMNS]


def build_team_daily(s):
    """One row per team per game day (Q3: team usage and scoring trends)."""
    return s.groupby(["game_date", "team", "opponent", "home_road"]).agg(
        skaters=("player_id", "nunique"),
        goals=("goals", "sum"),
        points=("points", "sum"),
        shots=("shots", "sum"),
        pp_goals=("pp_goals", "sum"),
        pp_points=("pp_points", "sum"),
        pim=("pim", "sum"),
        avg_toi_min=("toi_min", "mean"),
    ).reset_index()


def build_opponent_vs_position(s):
    """Points each opponent allowed to each position on each day (Q2: matchups).
    Kept at daily grain so the dashboard's date filter can sum any range."""
    return s.groupby(["game_date", "opponent", "position"]).agg(
        skater_games=("player_id", "count"),
        goals_allowed=("goals", "sum"),
        points_allowed=("points", "sum"),
        shots_allowed=("shots", "sum"),
        pp_points_allowed=("pp_points", "sum"),
    ).reset_index()


BUILDERS = {
    "skater_games": build_skater_games,
    "team_daily": build_team_daily,
    "opponent_vs_position": build_opponent_vs_position,
}


def write_day(fs, day, s):
    """Build every daily Gold table for one day and overwrite that day's partition.
    Days with no games are skipped, so no empty files are written."""
    if s.empty:
        log.info("No games on %s, nothing to write", day)
        return
    for table in config.GOLD_TABLES:
        df = BUILDERS[table](s)
        path = f"{config.GOLD_PATH}/{table}/game_date={day}/part-0.parquet"
        utils.write_parquet(fs, path, df)
        log.info("Wrote %d rows to %s", len(df), path)


# ---------- Snapshot table: rebuilt whole every run from Gold, not Silver ----------
def build_player_form(fs):
    """One row per player: last FORM_DAYS vs. season-to-date (Q1: who's hot or cold).

    This needs the whole season, not just the last 3 days, so it reads every
    partition of gold/skater_games instead of Silver. That keeps the rule that
    Gold reads only the last GOLD_REBUILD_DAYS of Silver. The table is small
    (~one row per skater), so it is written as one file and fully replaced."""
    games = utils.read_parquet_folder(fs, f"{config.GOLD_PATH}/skater_games/game_date=*/*.parquet")
    if games.empty:                                    # very first runs, before any game is in Gold
        log.info("No skater_games in Gold yet, skipping player_form")
        return
    games = games.reset_index(drop=True)
    games["game_date"] = pd.to_datetime(games["game_date"])
    latest = games["game_date"].max()
    recent = games[games["game_date"] > latest - pd.Timedelta(days=config.FORM_DAYS)]

    def summarize(df, prefix):
        out = df.groupby("player_id").agg(
            games=("game_id", "nunique"),
            goals=("goals", "sum"),
            points=("points", "sum"),
            shots=("shots", "sum"),
            pp_points=("pp_points", "sum"),
            avg_toi_min=("toi_min", "mean"),
        )
        out["points_per_game"] = out["points"] / out["games"]
        return out.add_prefix(prefix)

    info = (games.sort_values("game_date")
                 .groupby("player_id")[["skater_name", "team", "position", "position_group"]]
                 .last())                                    # latest team, in case of a trade
    form = info.join(summarize(games, "season_")).join(summarize(recent, "last7_"))
    form = form.fillna({c: 0 for c in form.columns if c.startswith("last7_")})
    form["ppg_change"] = form["last7_points_per_game"] - form["season_points_per_game"]
    form["form"] = pd.cut(form["ppg_change"], [-float("inf"), -0.25, 0.25, float("inf")],
                          labels=["cold", "steady", "hot"]).astype(str)
    form["as_of_date"] = latest.strftime("%Y-%m-%d")
    form = form[form["season_games"] >= config.FORM_MIN_GAMES].reset_index()

    path = f"{config.GOLD_PATH}/player_form/player_form.parquet"
    utils.write_parquet(fs, path, form)
    log.info("Wrote %d rows to %s", len(form), path)


def export_csvs(fs):
    """Concatenate every day partition of each daily table, plus the snapshot
    table, into one flat CSV each for the dashboard tool."""
    for table in config.GOLD_TABLES:
        full = utils.read_parquet_folder(fs, f"{config.GOLD_PATH}/{table}/game_date=*/*.parquet")
        path = f"{config.GOLD_PATH}/tableau/{table}.csv"
        utils.write_csv(fs, path, full)
        log.info("Exported %d rows to %s", len(full), path)

    form = utils.read_parquet_folder(fs, f"{config.GOLD_PATH}/player_form/*.parquet")
    if form.empty:
        return
    utils.write_csv(fs, f"{config.GOLD_PATH}/tableau/player_form.csv", form)
    log.info("Exported %d rows to %s/tableau/player_form.csv", len(form), config.GOLD_PATH)