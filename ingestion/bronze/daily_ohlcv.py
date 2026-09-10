import argparse
import uuid
import logging

import pandas as pd
import yfinance as yf

from ingestion.universe.loader import get_active_universe
from ingestion.bronze.schemas import daily_ohlcv_schema
from ingestion.common.retry import fetch_with_backoff
from ingestion.common.logging_config import configure_logging
from ingestion.common.notifications import notify_slack
from ingestion.common.config import BRONZE_ROOT, SLACK_WEBHOOK_URL

logger = logging.getLogger(__name__)


def fetch_daily_ohlcv(symbol: str, run_id: str, ingestion_ts: pd.Timestamp) -> pd.DataFrame:
    ticker = yf.Ticker(symbol)
    df = ticker.history(period="5d", auto_adjust=False)
    df = df.reset_index()
    df["symbol"] = symbol
    df["source"] = "yfinance"
    df["run_id"] = run_id
    df["ingestion_ts"] = ingestion_ts
    return df


def write_partitioned(df: pd.DataFrame, symbol: str, target_date: str | None = None):
    for run_date, group in df.groupby(df["Date"].dt.date):
        if target_date and str(run_date) != target_date:
            continue  # skip rows outside the requested date when --date is given

        partition_path = BRONZE_ROOT / "daily_ohlcv" / f"dt={run_date}" / f"symbol={symbol}"
        partition_path.mkdir(parents=True, exist_ok=True)
        file_path = partition_path / "data.parquet"

        if file_path.exists():
            existing = pd.read_parquet(file_path)
            combined = pd.concat([existing, group])
            combined = combined.sort_values("ingestion_ts").drop_duplicates(
                subset=["symbol", "Date"], keep="last"
            )
        else:
            combined = group

        combined.to_parquet(file_path, index=False)


def run_daily_ingestion(target_date: str | None = None, symbols: list[str] | None = None):
    run_id = str(uuid.uuid4())
    ingestion_ts = pd.Timestamp.now("UTC")
    universe = symbols if symbols else get_active_universe()

    success_count = 0

    for symbol in universe:
        try:
            df = fetch_with_backoff(fetch_daily_ohlcv, symbol, run_id, ingestion_ts)
            df = daily_ohlcv_schema.validate(df, lazy=True)
            write_partitioned(df, symbol, target_date=target_date)
            logger.info(f"Ingested {symbol}: {len(df)} rows [run_id={run_id}]")
            success_count += 1
        except Exception as e:
            logger.error(f"Failed to ingest {symbol}: {e} [run_id={run_id}]")

    summary = f"Daily ingestion complete: {success_count}/{len(universe)} symbols, run_id={run_id}"
    logger.info(summary)
    if SLACK_WEBHOOK_URL:
        notify_slack(summary, webhook_url=SLACK_WEBHOOK_URL)


def parse_args():
    parser = argparse.ArgumentParser(description="Bronze daily OHLCV ingestion")
    parser.add_argument("--date", type=str, default=None, help="Target date, e.g. 2026-08-15 (re-run a historical day)")
    parser.add_argument("--symbols", type=str, default=None, help="Comma-separated symbols to override the universe, e.g. AAPL,MSFT")
    return parser.parse_args()


if __name__ == "__main__":
    configure_logging()
    args = parse_args()
    symbol_list = args.symbols.split(",") if args.symbols else None
    run_daily_ingestion(target_date=args.date, symbols=symbol_list)
