#!/usr/bin/env python3
"""CLI entry point for the ParkPulse availability collector.

Usage
-----
    # Start the continuous collector (default: data/parkpulse.duckdb)
    python -m parkpulse.run

    # Custom database path
    python -m parkpulse.run --db /tmp/parkpulse.duckdb

    # Run exactly one poll cycle then exit (useful for cron / testing)
    python -m parkpulse.run --once

    # Override polling interval and lookahead via env vars
    POLL_BASE_INTERVAL=60 LOOKAHEAD_DAYS=30 python -m parkpulse.run
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from parkpulse import db
from parkpulse.collector import poll_cycle, run_forever


def main() -> None:
    parser = argparse.ArgumentParser(
        description="ParkPulse campsite availability collector",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="Path to DuckDB database (default: data/parkpulse.duckdb)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single poll cycle and exit",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable DEBUG logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )

    if args.once:
        conn = db.connect(args.db)
        db.init_schema(conn)
        stats = asyncio.run(poll_cycle(conn, db_path=args.db))
        conn.close()
        print(
            f"Poll complete: {stats['n_facilities']} facilities, "
            f"{stats['n_sites']} sites, {stats['n_snapshots']} snapshots, "
            f"{stats['n_transitions']} transitions"
        )
    else:
        asyncio.run(run_forever(db_path=args.db))


if __name__ == "__main__":
    main()
