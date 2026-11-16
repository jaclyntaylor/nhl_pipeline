"""
bronze_utils.py
Functions used only by the Bronze layer (NHL skater game stats).
"""

import json
import time
from datetime import timedelta

import requests

import config
import utils

log = utils.get_logger("bronze")


def get_start_time(fs):
    """Where the last successful run stopped. On the very first run there is
    no watermark, so look back a few days instead (NHL data is daily)."""
    start = utils.read_watermark(fs, config.BRONZE_WATERMARK)
    if start is None:
        start = utils.now_utc() - timedelta(days=config.FIRST_RUN_LOOKBACK_DAYS)
        log.info("First run, no watermark. Looking back %d days.", config.FIRST_RUN_LOOKBACK_DAYS)
    log.info("Start time: %s", start)
    return start


def fetch_events(start):
    """Ask the NHL stats API for every skater-game row with gameDate on or after
    the watermark's date minus OVERLAP_DAYS.

    The API filters on a date, not a timestamp, so we step back a day to catch
    late-finishing games and stat corrections. The duplicates this creates are
    kept in Bronze and removed in Silver on (gameId, playerId).

    Results come back in pages, so we keep asking (start=0, 100, 200, ...) until
    we have 'total' rows. The sort on gameId + playerId keeps the page order
    stable so no row is skipped or repeated between pages."""
    from_date = (start - timedelta(days=config.OVERLAP_DAYS)).date().isoformat()
    sort = json.dumps([{"property": "gameId", "direction": "ASC"},
                       {"property": "playerId", "direction": "ASC"}])
    rows, offset, total = [], 0, None

    while total is None or offset < total:
        params = {
            "isAggregate": "false",
            "isGame": "true",
            "start": offset,
            "limit": config.PAGE_SIZE,
            "sort": sort,
            "cayenneExp": f'gameDate>="{from_date}" and gameTypeId={config.GAME_TYPE_ID}',
        }
        body = _get_with_retry(params)
        page = body.get("data", [])
        total = body.get("total", 0)
        if not page:                            # safety stop if the API returns an empty page early
            break
        rows.extend(page)
        offset += len(page)

    log.info("NHL API returned %d skater-game rows (gameDate >= %s, total=%s)",
             len(rows), from_date, total)
    return rows


def _get_with_retry(params):
    """One page request. The NHL API is unofficial and sometimes returns a 5xx
    or times out, so try a few times with a short wait before giving up.
    If every attempt fails, the exception reaches utils.run(), the script exits
    non-zero and Airflow's own retries take over. The watermark is not moved."""
    for attempt in range(1, config.HTTP_ATTEMPTS + 1):
        try:
            response = requests.get(config.NHL_URL, params=params, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as err:
            if attempt == config.HTTP_ATTEMPTS:
                raise
            log.warning("Request failed (%s), attempt %d of %d. Retrying.",
                        err, attempt, config.HTTP_ATTEMPTS)
            time.sleep(5 * attempt)


def write_events(fs, events, run_ts):
    """Write the rows, untouched except for a load_ts stamp, into this run's
    partition folder:  bronze/nhl/load_date=YYYY-MM-DD/load_hr=HH/skater_games_HHMMSS.jsonl
    The file name carries the run time so two runs in the same hour never overwrite each other."""
    for event in events:
        event["load_ts"] = utils.to_text(run_ts)
    path = (f"{config.BRONZE_PATH}/load_date={run_ts:%Y-%m-%d}/load_hr={run_ts:%H}"
            f"/skater_games_{run_ts:%H%M%S}.jsonl")
    utils.write_jsonl(fs, path, events)
    log.info("Wrote %d lines to %s", len(events), path)