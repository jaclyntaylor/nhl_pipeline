"""
config.py
Every setting in one place. Secrets come from environment variables
(copy .env.example to pipeline/.env on your laptop; the DAG sets them on the VM).
"""

import os
from dotenv import load_dotenv

load_dotenv()

### GENERIC CONFIG ###########################################################

# Azure storage account with hierarchical namespace ON, and one of its access keys
STORAGE_ACCOUNT = os.environ["STORAGE_ACCOUNT"]
STORAGE_KEY = os.environ["STORAGE_KEY"]

# Where watermark files live. Format for every path is "container/folder".
WATERMARK_PATH = "meta/watermarks"

# One timestamp format for the whole pipeline, always UTC: 2026-11-01T13:00:08Z
TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


### BRONZE CONFIG ############################################################

NHL_URL = "https://api.nhle.com/stats/rest/en/skater/summary"
BRONZE_PATH = "bronze/nhl"
BRONZE_WATERMARK = "nhl_to_bronze"
FIRST_RUN_LOOKBACK_DAYS = 7         # no watermark yet? look back this many days
OVERLAP_DAYS = 1                    # re-pull the previous day to catch late games / stat fixes
PAGE_SIZE = 100                     # rows per API page
GAME_TYPE_ID = 2                    # 2 = regular season, 3 = playoffs
HTTP_ATTEMPTS = 3                   # tries per page before failing the run


### SILVER CONFIG ############################################################

SILVER_PATH = "silver/skater_games"
SILVER_WATERMARK = "bronze_to_silver"
SILVER_KEY = ["game_id", "player_id"]   # one row per skater per game; newest load_ts wins (names after Silver's rename)


### GOLD CONFIG ##############################################################

GOLD_PATH = "gold"
GOLD_TABLES = ["skater_games", "team_daily", "opponent_vs_position"]   # daily tables, partitioned by game_date
GOLD_REBUILD_DAYS = 3               # Gold rebuilds the last N days every run
FORM_DAYS = 7                       # "recent form" window for the player_form snapshot table
FORM_MIN_GAMES = 5                  # leave out players with too few games to judge