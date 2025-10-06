# pip install ccxt python-dotenv pandas
import os
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import ccxt
from dotenv import load_dotenv

# -------------------- Config & helpers --------------------
load_dotenv()

EXCHANGE_NAME = os.getenv("EXCHANGE", "binanceusdm")  # "binanceusdm" | "okx"
DATA_DIR = Path(os.getenv("DATA_DIR", "data/raw"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# default params if not provided on CLI
DEFAULT_SYMBOLS = os.getenv("SYMBOLS", "BTC/USDT:USDT,ETH/USDT:USDT").split(",")
DEFAULT_TIMEFRAME = os.getenv("TIMEFRAME", "1m")  # choose "1m","5m","15m","1h","4h","1d"
DEFAULT_SINCE = os.getenv("SINCE", "2021-01-01")  # ISO date or "YYYY-MM-DD"

# Per-API max rows per request (Binance ~1500, OKX ~300)
EXCHANGE_LIMITS = {
    "binanceusdm": 1500,
    "okx": 300,
}

def mk_exchange():
    common = dict(enableRateLimit=True, options={})
    if EXCHANGE_NAME == "binanceusdm":
        ex = ccxt.binanceusdm(common)
    elif EXCHANGE_NAME == "okx":
        ex = ccxt.okx(common)
    else:
        raise ValueError(f"Unsupported exchange: {EXCHANGE_NAME}")
    return ex

def parse_since(s: str, exchange):
    # If CSV exists, we auto-resume; otherwise parse user date
    try:
        # try ISO date
        dt = datetime.fromisoformat(s)
        return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
    except Exception:
        # try ccxt parse8601
        ms = exchange.parse8601(s)
        if ms is None:
            raise ValueError("Provide SINCE as ISO date 'YYYY-MM-DD' or full timestamp")
        return ms

def csv_path(exchange, symbol, timeframe):
    # Normalize filename friendly symbol, e.g., BTCUSDT_perp
    market = exchange.market(symbol)
    base = market.get("base", symbol.split("/")[0])
    quote = market.get("quote", "USDT")
    ttype = "perp"
    fname = f"{base}{quote}_{ttype}_{timeframe}.csv"
    subdir = DATA_DIR / EXCHANGE_NAME / timeframe
    subdir.mkdir(parents=True, exist_ok=True)
    return subdir / fname

def load_existing(path: Path):
    if path.exists():
        df = pd.read_csv(path)
        if "ts" in df.columns:
            df["ts"] = pd.to_datetime(df["ts"], utc=True)
        return df
    return pd.DataFrame(columns=["ts","open","high","low","close","volume"])

def save_csv(df: pd.DataFrame, path: Path):
    df = df.sort_values("ts").drop_duplicates(subset=["ts"])
    df.to_csv(path, index=False)
    print(f"Saved {len(df):,} rows -> {path}")

def human(ms):
    return datetime.utcfromtimestamp(ms/1000).strftime("%Y-%m-%d %H:%M:%S")

# -------------------- Fetch loop --------------------
def fetch_symbol(exchange, symbol, timeframe, since_ms=None, limit=None):
    """Fetch (and incrementally update) OHLCV for one symbol/timeframe."""
    path = csv_path(exchange, symbol, timeframe)
    existing = load_existing(path)

    # Determine start timestamp
    if not existing.empty:
        # start 1 bar after the last timestamp we have
        last_ts = int(existing["ts"].iloc[-1].timestamp() * 1000)
        fetch_since = last_ts + 1
        print(f"[{symbol} {timeframe}] resume from {human(fetch_since)}")
    else:
        fetch_since = since_ms
        print(f"[{symbol} {timeframe}] start from {human(fetch_since)}")

    max_rows = EXCHANGE_LIMITS.get(EXCHANGE_NAME, 1000)
    # Safety: if user passed a limit for total rows to add
    total_added = 0

    all_new = []
    while True:
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=fetch_since, limit=max_rows)
        except ccxt.NetworkError as e:
            print("Network error, retrying in 5s:", e)
            time.sleep(5)
            continue
        except ccxt.ExchangeError as e:
            print("Exchange error, stopping:", e)
            break

        if not ohlcv:
            print("No more data from server.")
            break

        df = pd.DataFrame(ohlcv, columns=["ms","open","high","low","close","volume"])
        df["ts"] = pd.to_datetime(df["ms"], unit="ms", utc=True)
        df = df[["ts","open","high","low","close","volume"]]

        all_new.append(df)
        # Advance since to last ms + 1
        fetch_since = int(ohlcv[-1][0]) + 1

        added_now = len(df)
        total_added += added_now
        print(f"  +{added_now} rows up to {human(fetch_since)}")

        if limit is not None and total_added >= limit:
            print(f"Hit user limit {limit}; stopping.")
            break

        # polite sleep to respect rate limits
        time.sleep(exchange.rateLimit / 1000 + 0.05)

    if all_new:
        new_df = pd.concat(all_new, ignore_index=True)
        out = pd.concat([existing, new_df], ignore_index=True)
        save_csv(out, path)
    else:
        print("Nothing new to save.")

# -------------------- CLI --------------------
def main():
    parser = argparse.ArgumentParser(description="Fetch OHLCV (futures perp) to CSV.")
    parser.add_argument("--symbols", type=str, default=",".join(DEFAULT_SYMBOLS),
                        help="Comma-separated ccxt symbols, e.g. BTC/USDT:USDT,ETH/USDT:USDT")
    parser.add_argument("--timeframe", type=str, default=DEFAULT_TIMEFRAME,
                        help="1m,5m,15m,1h,4h,1d ...")
    parser.add_argument("--since", type=str, default=DEFAULT_SINCE,
                        help="ISO date (YYYY-MM-DD) or full timestamp, ignored if CSV already exists")
    parser.add_argument("--limit", type=int, default=None,
                        help="Optional cap on rows to fetch this run")
    args = parser.parse_args()

    ex = mk_exchange()
    ex.load_markets()

    since_ms = parse_since(args.since, ex)
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    for sym in symbols:
        # validate market & ensure it's a swap/perp if on Binance USDM
        market = ex.market(sym)
        if EXCHANGE_NAME == "binanceusdm" and market.get("type") != "swap":
            print(f"Warning: {sym} is not a USDT-M perpetual on Binance.")
        fetch_symbol(ex, sym, args.timeframe, since_ms=since_ms, limit=args.limit)

if __name__ == "__main__":
    main()