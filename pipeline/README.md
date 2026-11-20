# nhl_medallion_pipeline

A Medallion (Bronze -> Silver -> Gold) data pipeline in plain Python, reading the NHL Stats API (skater game logs) into an Azure Data Lake (ADLS Gen2), ending in a Tableau dashboard. Built for DATA 5330 / 6330 at Utah State University.

## Stakeholder

A fantasy hockey manager deciding which skaters to start or pick up each week. The dashboard answers:

1. Which skaters are hot or cold: points per game over the last 7 days compared with their season average?
2. Which teams give up the most points to each position (C / L / R / D)?
3. How do ice time and power-play usage trend for a player or team, and do they move with points?

## Data source

`https://api.nhle.com/stats/rest/en/skater/summary?isAggregate=false&isGame=true`

* One row per skater per game, about 30 fields (goals, assists, shots, ice time, power-play points, faceoff %, team, opponent, position, game date, ...)
* Public, no key, no signup
* Filtered with `cayenneExp=gameDate>="YYYY-MM-DD"`; results come in pages of 100 (`start`, `limit`, sorted by `gameId`, `playerId`)

**Watermark:** the API filters on `gameDate`, a date rather than a timestamp. Each Bronze run asks for every game on or after (watermark date - 1 day). The one-day overlap catches late-finishing games and stat corrections. Silver removes the resulting duplicates on (`game_id`, `player_id`), keeping the newest `load_ts`. A missed run is covered by the next one because its window starts from the last successful watermark.

## What each layer does

### Bronze (`bronze.py`)

* Reads: NHL Stats API, every skater-game row since the watermark date minus one day (all pages)
* Writes: `bronze/nhl/load_date=YYYY-MM-DD/load_hr=HH/skater_games_HHMMSS.jsonl` (raw JSON, one stat line per line, plus `load_ts`)
* Partitioned by: load time
* Strategy: append only, watermark `nhl_to_bronze`

### Silver (`silver.py`)

* Reads: new Bronze partitions
* Writes: `silver/skater_games/game_date=YYYY-MM-DD/part-0.parquet` (renamed to snake_case, typed, deduped)
* Partitioned by: game date (the API has no game start time, so there is no hour level)
* Strategy: overwrite touched partitions, newest `load_ts` wins per (`game_id`, `player_id`), watermark `bronze_to_silver`
* Derived columns: `toi_min`, `points_per_60`, `shots_per_60`, `position_group` (F / D), `is_home`, `game_dow`

### Gold (`gold.py`)

* Reads: Silver, last 3 days (every Silver day on the very first run, to backfill)
* Writes: `gold/<table>/game_date=YYYY-MM-DD/part-0.parquet` plus `gold/tableau/<table>.csv`
* Partitioned by: game date
* Strategy: rebuild the last 3 days every run, no watermark

Gold tables:

| Table | One row per | Answers |
|---|---|---|
| `skater_games` | skater per game | player detail view |
| `team_daily` | team per game day | Q3: team scoring, power-play and ice-time trends |
| `opponent_vs_position` | opponent x position x day | Q2: matchups |
| `player_form` | player (snapshot, not partitioned) | Q1: last 7 days vs. season, labeled hot / steady / cold |

`player_form` needs the whole season, so it is rebuilt every run from all of `gold/skater_games`. Silver is still only read for the last 3 days.

## Files

```
pipeline/
  config.py          all settings, split into GENERIC / BRONZE / SILVER / GOLD sections
  utils.py           generic helpers: logger, lake connection, watermarks, read/write files, run wrapper
  bronze_utils.py    functions used only by Bronze: watermark -> paged API call -> write file
  silver_utils.py    functions used only by Silver: read new Bronze, rename/type, dedupe, derived columns
  gold_utils.py      functions used only by Gold: one build_* function per table, CSV export
  bronze.py          entry point, step by step
  silver.py          entry point, step by step
  gold.py            entry point, step by step
requirements.txt
.env
```

## Run it on your laptop

1. In the Azure portal create a Storage Account with hierarchical namespace enabled and four containers: `bronze`, `silver`, `gold`, `meta`.
2. Copy `.env.example` to `pipeline/.env` and fill in the account name and an access key.
3. Install and run:

```
pip install -r requirements.txt
cd pipeline
python bronze.py
python silver.py
python gold.py
```

The first Bronze run has no watermark, so it looks back 7 days. Run `bronze.py` a second time a minute later: the log shows a start time taken from the watermark instead of the 7-day lookback. Because of the one-day overlap, the second run returns roughly the same rows again (yesterday's and today's games). That is expected, and Silver removes the duplicates. Run `silver.py` again and it should report only the new lines.

## Run it on Airflow

Each layer has its own DAG in the `airflow-dags` repo. Each DAG creates a VM, runs `pip install -r requirements.txt && cd pipeline && python <layer>.py`, deletes the VM, and triggers the next layer's DAG. Bronze runs every 2 hours; Silver and Gold run only when triggered. The VM gets `STORAGE_ACCOUNT` and `STORAGE_KEY` as environment variables from the DAG.

NHL games are mostly played in the evening (North American time), so new stat lines arrive in a nightly burst. Daytime runs mostly re-pull the overlap day.

## How failures are handled

* Every entry point ends with `utils.run(main, "<layer>")`. On any error it prints the full traceback to the console (which Airflow shows in the task log UI) and exits non-zero, so Airflow marks the task failed and retries it.
* Each API page is tried up to 3 times before the run fails, because the NHL API is unofficial and occasionally times out.
* A watermark is only written after the data write succeeds. A failed run therefore re-reads exactly the same window on retry, and a missed run is backfilled by the next one.
* Days with no games (off nights, All-Star break) produce no Silver or Gold partitions; the pipeline skips them instead of failing.
