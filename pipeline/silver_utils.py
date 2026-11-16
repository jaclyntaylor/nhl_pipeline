"""
silver_utils.py
Functions used only by the Silver layer (NHL skater game stats).
"""

from datetime import datetime, timezone

import pandas as pd

import config
import utils

log = utils.get_logger("silver")

# API field name -> Silver column name. Anything the API sends that is not in
# this map is dropped (gamesPlayed is always 1 per game, pointsPerGame = points).
RENAME = {
    "gameId": "game_id",
    "playerId": "player_id",
    "skaterFullName": "skater_name",
    "lastName": "last_name",
    "teamAbbrev": "team",
    "opponentTeamAbbrev": "opponent",
    "homeRoad": "home_road",
    "positionCode": "position",
    "shootsCatches": "shoots",
    "gameDate": "game_date",
    "goals": "goals",
    "assists": "assists",
    "points": "points",
    "plusMinus": "plus_minus",
    "penaltyMinutes": "pim",
    "shots": "shots",
    "shootingPct": "shooting_pct",
    "evGoals": "ev_goals",
    "evPoints": "ev_points",
    "ppGoals": "pp_goals",
    "ppPoints": "pp_points",
    "shGoals": "sh_goals",
    "shPoints": "sh_points",
    "otGoals": "ot_goals",
    "gameWinningGoals": "gw_goals",
    "faceoffWinPct": "faceoff_win_pct",
    "timeOnIcePerGame": "toi_sec",
    "load_ts": "load_ts",
}

COUNT_COLUMNS = ["goals", "assists", "points", "plus_minus", "pim", "shots",
                 "ev_goals", "ev_points", "pp_goals", "pp_points", "sh_goals",
                 "sh_points", "ot_goals", "gw_goals"]
DECIMAL_COLUMNS = ["shooting_pct", "faceoff_win_pct", "toi_sec"]
TEXT_COLUMNS = ["skater_name", "last_name", "team", "opponent", "home_road",
                "position", "shoots"]


def get_watermark(fs):
    """Last Bronze load_ts that Silver already processed. First run: take everything."""
    watermark = utils.read_watermark(fs, config.SILVER_WATERMARK)
    if watermark is None:
        watermark = datetime(2000, 1, 1, tzinfo=timezone.utc)
    log.info("Processing Bronze lines with load_ts > %s", watermark)
    return watermark


def read_new_bronze(fs, watermark):
    """Read only the Bronze lines newer than the watermark.
    Two filters: skip whole partition folders that are too old (partition
    pruning), then keep only rows whose load_ts is newer than the watermark."""
    watermark_hour = watermark.replace(minute=0, second=0, microsecond=0)
    rows = []
    for path in fs.glob(f"{config.BRONZE_PATH}/load_date=*/load_hr=*/*.jsonl"):
        date_part = path.split("load_date=")[1][:10]            # 2026-11-16
        hr_part = path.split("load_hr=")[1][:2]                 # 13
        folder_time = datetime.strptime(f"{date_part}T{hr_part}", "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)
        if folder_time < watermark_hour:
            continue                                             # too old, skip the whole folder
        rows.extend(utils.read_jsonl(fs, path))

    raw = pd.DataFrame(rows)
    if raw.empty:
        return raw
    raw["load_ts"] = pd.to_datetime(raw["load_ts"], utc=True)
    raw = raw[raw["load_ts"] > watermark].reset_index(drop=True)
    log.info("Read %d new Bronze lines", len(raw))
    return raw


def flatten(raw):
    """NHL rows are already flat, so this step renames camelCase fields to
    snake_case, keeps only the columns we know, and fixes the types.

    reindex() guarantees every expected column exists even if the API leaves
    one out of a small batch. Counts become nullable integers; percentages
    and ice time become floats. faceoff_win_pct stays null for players who
    took no faceoffs: 0% would be wrong, it is 'not applicable'."""
    df = raw.rename(columns=RENAME).reindex(columns=list(RENAME.values()))

    for col in COUNT_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in DECIMAL_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    for col in TEXT_COLUMNS:
        df[col] = df[col].astype("string").str.strip()
    df["game_id"] = pd.to_numeric(df["game_id"], errors="coerce").astype("Int64")
    df["player_id"] = pd.to_numeric(df["player_id"], errors="coerce").astype("Int64")
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    bad = df["game_id"].isna() | df["player_id"].isna() | df["game_date"].isna()
    if bad.any():
        log.warning("Dropping %d rows with no game_id, player_id or game_date", int(bad.sum()))
    return df[~bad].reset_index(drop=True)


def dedupe(df):
    """Keep only the newest version of each skater-game line.
    The API has no 'updated' timestamp, so the newest load_ts wins: if the
    NHL corrected a stat, the later pull carries the corrected number."""
    return (df.sort_values("load_ts")
              .drop_duplicates(config.SILVER_KEY, keep="last")
              .reset_index(drop=True))


def add_columns(df):
    """Derived columns the dashboard needs. The partition key, game_date, is
    already set in flatten(); it is the date the game was played, not loaded."""
    dates = pd.to_datetime(df["game_date"])
    df["game_dow"] = dates.dt.day_name()
    df["is_home"] = df["home_road"] == "H"
    df["position_group"] = df["position"].map({"D": "D"}).fillna("F")   # C, L, R are forwards

    # Ice time arrives in seconds; minutes are easier to read on a chart
    df["toi_min"] = (df["toi_sec"] / 60).round(2)

    # Per-60-minute rates let a 12-minute 4th-liner be compared with a 22-minute 1st-liner.
    # Rows with 0 ice time get null, not infinity.
    hours = (df["toi_sec"] / 3600).where(df["toi_sec"] > 0)
    df["points_per_60"] = (df["points"].astype("float64") / hours).round(2)
    df["shots_per_60"] = (df["shots"].astype("float64") / hours).round(2)
    return df


def write_partitions(fs, df):
    """For every game_date in df: merge with the rows already in that Silver
    partition, keep the newest version of each (game_id, player_id), and
    OVERWRITE the file. Dates not in this batch are left alone."""
    for game_date, new_part in df.groupby("game_date"):
        path = f"{config.SILVER_PATH}/game_date={game_date}/part-0.parquet"
        if fs.exists(path):
            merged = dedupe(pd.concat([utils.read_parquet(fs, path), new_part], ignore_index=True))
        else:
            merged = new_part
        utils.write_parquet(fs, path, merged)
        log.info("Wrote %d rows to %s", len(merged), path)