"""
bronze.py  (Layer 1 of 3)

Pull every NHL skater-game stat line since the last successful run
(minus a one-day overlap) and save it exactly as received, one JSON
record per line, partitioned by the time we LOADED it.
"""

import config
import utils
import bronze_utils

log = utils.get_logger("bronze")


def main():
    fs = utils.get_fs()

    start = bronze_utils.get_start_time(fs)          # 1. where did the last run stop?
    run_ts = utils.now_utc()                          # 2. where will this run stop?
    events = bronze_utils.fetch_events(start)         # 3. call the NHL stats API (all pages)
    bronze_utils.write_events(fs, events, run_ts)     # 4. write this run's partition
    utils.write_watermark(fs, config.BRONZE_WATERMARK, run_ts)   # 5. only after step 4 worked

    log.info("Watermark moved to %s. Bronze done.", run_ts)


if __name__ == "__main__":
    utils.run(main, "bronze")