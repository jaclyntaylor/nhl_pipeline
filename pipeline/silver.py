"""
silver.py  (Layer 2 of 3)

Turn raw Bronze JSON into a clean, typed, deduplicated table of skater
stat lines (one row per player per game), partitioned by the date the
game was PLAYED. Touched partitions are overwritten.
"""

import pipeline.config as config
import pipeline.utils as utils
import pipeline.silver_utils as silver_utils

log = utils.get_logger("silver")


def main():
    fs = utils.get_fs()

    watermark = silver_utils.get_watermark(fs)              # 1. what did we already process?
    raw = silver_utils.read_new_bronze(fs, watermark)       # 2. read only the new Bronze lines
    if raw.empty:
        log.info("No new Bronze data. Silver done.")
        return

    df = silver_utils.flatten(raw)                          # 3. rename + type the columns
    df = silver_utils.dedupe(df)                            #    newest version of each (game_id, player_id)
    df = silver_utils.add_columns(df)                       #    partition key + derived columns
    log.info("Cleaned to %d unique skater-game rows", len(df))

    silver_utils.write_partitions(fs, df)                   # 4. overwrite touched game_date partitions

    newest = raw["load_ts"].max().to_pydatetime()
    utils.write_watermark(fs, config.SILVER_WATERMARK, newest)   # 5. only after step 4 worked
    log.info("Watermark moved to %s. Silver done.", newest)