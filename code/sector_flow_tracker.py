#!/Projects/Picker/bin/python3

"""
Sector Flow Tracker
====================
Tracks SPDR Select Sector ETF shares outstanding and AUM over time using
the free daily holdings Excel files published by State Street (SSGA).
Stores history in a local SQLite database.

HOW AUM IS DERIVED
------------------
The SSGA holdings files don't include a fund-level AUM or shares-outstanding
figure. Instead the file contains each holding's weight (%) and shares held.
For any holding:

    holding_value = shares_held × stock_price
    weight        = holding_value / AUM   (as a fraction)

Therefore:
    AUM = holding_value / (weight / 100)

We fetch live prices for the top N holdings and take the MEDIAN estimate
across them — this makes the result robust to stale prices on any one stock.
Then:
    ETF shares outstanding = AUM / ETF_price

FILE STRUCTURE (confirmed from live XLE file, Feb 2026)
-------------------------------------------------------
Row 0:  "Fund Name:"     | "State Street® Energy Select Sector SPDR® ETF"
Row 1:  "Ticker Symbol:" | "XLE"
Row 2:  "Holdings:"      | "As of 26-Feb-2026"
Row 3:  (blank)
Row 4:  column headers: Name | Ticker | Identifier | SEDOL | Weight | Sector | Shares Held | Local Currency
Row 5+: holdings data

USAGE
-----
    pip install requests pandas openpyxl yfinance

    # Collect today's snapshot for all 11 sector ETFs:
    python sector_flow_tracker.py

    # Show flow summary for last N days (default 5):
    python sector_flow_tracker.py --summary --days 10

    # Quiet mode for cron:
    python sector_flow_tracker.py --quiet

    # Debug: print raw file structure for one ticker and exit:
    python sector_flow_tracker.py --debug XLE

CRON EXAMPLE (weekdays at 6 PM)
--------------------------------
    0 18 * * 1-5 /usr/bin/python3 /path/to/sector_flow_tracker.py --quiet
"""

import argparse
import logging
import sqlite3
import statistics
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests
import yfinance as yf

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH = Path("/Projects/Picker/code/sector_flows.db")

# Number of top holdings (by weight) used to estimate AUM via median
AUM_ESTIMATE_TOP_N = 10

SECTORS = {
    "XLC":  "Communication Services",
    "XLY":  "Consumer Discretionary",
    "XLP":  "Consumer Staples",
    "XLE":  "Energy",
    "XLF":  "Financials",
    "XLV":  "Health Care",
    "XLI":  "Industrials",
    "XLB":  "Materials",
    "XLRE": "Real Estate",
    "XLK":  "Technology",
    "XLU":  "Utilities",
}

SSGA_URL = (
    "https://www.ssga.com/us/en/intermediary/etfs/library-content/"
    "products/fund-data/etfs/us/holdings-daily-us-en-{ticker}.xlsx"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,*/*",
    "Referer": "https://www.ssga.com/",
}

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sector_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date            TEXT    NOT NULL,
            ticker          TEXT    NOT NULL,
            sector_name     TEXT    NOT NULL,
            shares_out      REAL,
            etf_price       REAL,
            aum_millions    REAL,
            holdings_count  INTEGER,
            fetched_at      TEXT    NOT NULL,
            UNIQUE(date, ticker)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_date_ticker
        ON sector_snapshots(date, ticker)
    """)
    conn.commit()


def upsert_snapshot(conn: sqlite3.Connection, row: dict) -> None:
    conn.execute("""
        INSERT INTO sector_snapshots
            (date, ticker, sector_name, shares_out, etf_price,
             aum_millions, holdings_count, fetched_at)
        VALUES
            (:date, :ticker, :sector_name, :shares_out, :etf_price,
             :aum_millions, :holdings_count, :fetched_at)
        ON CONFLICT(date, ticker) DO UPDATE SET
            shares_out     = excluded.shares_out,
            etf_price      = excluded.etf_price,
            aum_millions   = excluded.aum_millions,
            holdings_count = excluded.holdings_count,
            fetched_at     = excluded.fetched_at
    """, row)
    conn.commit()
    #print("inserting snapshot",row)

# ---------------------------------------------------------------------------
# SSGA holdings file fetch & parse
# ---------------------------------------------------------------------------

def download_holdings(ticker: str) -> bytes | None:
    url = SSGA_URL.format(ticker=ticker.lower())
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.content
    except requests.RequestException as e:
        logging.warning(f"{ticker}: download failed — {e}")
        return None


def parse_holdings(raw_bytes: bytes) -> tuple[str | None, pd.DataFrame]:
    """
    Parse the SSGA holdings Excel.

    Returns (as_of_date_iso_str, holdings_df).

    holdings_df has columns: Ticker, Weight (%), Shares Held
    Sorted by Weight descending.
    """
    df_raw = pd.read_excel(BytesIO(raw_bytes), header=None, engine="openpyxl")

    # --- Extract the "as of" date from the header rows ---
    # Row 2 looks like: "Holdings:" | "As of 26-Feb-2026"
    as_of_date = None
    for _, row in df_raw.head(5).iterrows():
        cell = str(row.iloc[0]).strip().lower()
        if "holding" in cell or "as of" in cell:
            val = str(row.iloc[1]).strip() if len(row) > 1 else ""
            val = val.lower().replace("as of", "").strip()
            try:
                as_of_date = pd.to_datetime(val).date().isoformat()
                break
            except Exception:
                pass

    # --- Find the holdings header row (the one containing "Weight" and "Shares Held") ---
    # Confirmed at index 4 in the live file, but we scan just to be safe.
    header_row_idx = None
    for i, row in df_raw.iterrows():
        cells = [str(c).strip().lower() for c in row]
        if "weight" in cells and "shares held" in cells:
            header_row_idx = i
            break

    if header_row_idx is None:
        logging.warning("Could not locate holdings header row in file")
        return as_of_date, pd.DataFrame()

    # Re-read using the discovered header row
    df = pd.read_excel(BytesIO(raw_bytes), header=header_row_idx, engine="openpyxl")
    df = df.rename(columns=lambda c: str(c).strip())

    # Keep only the columns we need
    needed = ["Ticker", "Weight", "Shares Held"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        logging.warning(f"Expected columns not found: {missing}  (got: {df.columns.tolist()})")
        return as_of_date, pd.DataFrame()

    df = df[needed].copy()
    df["Ticker"]      = df["Ticker"].astype(str).str.strip()
    df["Weight"]      = pd.to_numeric(df["Weight"],      errors="coerce")
    df["Shares Held"] = pd.to_numeric(df["Shares Held"], errors="coerce")

    # Drop blank/invalid rows and the disclaimer footer rows
    df = df.dropna(subset=["Weight", "Shares Held"])
    df = df[df["Ticker"].str.len() > 0]
    df = df[df["Ticker"] != "nan"]
    df = df[df["Weight"] > 0]

    df = df.sort_values("Weight", ascending=False).reset_index(drop=True)
    return as_of_date, df

# ---------------------------------------------------------------------------
# AUM estimation from holdings × live prices
# ---------------------------------------------------------------------------

def estimate_aum(holdings_df: pd.DataFrame, top_n: int = AUM_ESTIMATE_TOP_N) -> float | None:
    """
    Estimate fund AUM (in dollars) using the top N holdings by weight.

    For each holding:
        AUM = (shares_held × stock_price) / (weight / 100)

    Returns the median estimate across all successfully priced holdings.
    """
    top = holdings_df.head(top_n)
    l = list()
    tickers = top["Ticker"].tolist()
    for t in tickers:
        l.append(t.replace('.','-'))
    tickers = l

    # Batch-fetch closing prices via yfinance (faster than one-by-one)
    prices: dict[str, float] = {}
    try:
        data = yf.download(tickers, period="1d", progress=False, auto_adjust=True)
        if not data.empty:
            close = data["Close"] if "Close" in data.columns else data
            last_row = close.iloc[-1]
            for t in tickers:
                t = t.replace('.','-')
                try:
                    v = float(last_row[t])
                    if not pd.isna(v):
                        prices[t] = v
                except Exception:
                    pass
    except Exception as e:
        logging.debug(f"Batch price fetch failed ({e}), will fall back per-ticker")

    # Per-ticker fallback for any that missed
    for t in tickers:
        t = t.replace('.','-')
        if t not in prices:
            try:
                prices[t] = float(yf.Ticker(t).fast_info["lastPrice"])
            except Exception:
                pass

    if not prices:
        logging.warning("Could not fetch any stock prices for AUM estimation")
        return None

    estimates = []
    for _, row in top.iterrows():
        t = row["Ticker"]
        if t in prices and row["Weight"] > 0:
            holding_value = row["Shares Held"] * prices[t]
            aum = holding_value / (row["Weight"] / 100.0)
            estimates.append(aum)

    if not estimates:
        return None
    if len(estimates) == 1:
        logging.warning("Only one holding priced — AUM estimate may be unreliable")
        return estimates[0]

    return statistics.median(estimates)


def fetch_etf_price(ticker: str) -> float | None:
    ticker = ticker.replace('.','-')
    try:
        return round(float(yf.Ticker(ticker).fast_info["lastPrice"]), 4)
    except Exception:
        return None

# ---------------------------------------------------------------------------
# Core collection loop
# ---------------------------------------------------------------------------

def collect_snapshot(conn: sqlite3.Connection, quiet: bool = False) -> int:
    today      = date.today().isoformat()
    fetched_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    success    = 0

    for ticker, sector_name in SECTORS.items():
        ticker = ticker.replace('.','-')
        if not quiet:
            print(f"  {ticker:<5}  {sector_name:<28}", end="  ", flush=True)

        # 1. Download
        raw = download_holdings(ticker)
        if raw is None:
            if not quiet: print("FAILED (download)")
            continue

        # 2. Parse
        as_of_date, holdings_df = parse_holdings(raw)
        if holdings_df.empty:
            if not quiet: print("FAILED (parse)")
            continue

        snap_date      = as_of_date or today
        holdings_count = len(holdings_df)

        # 3. Estimate AUM from holdings × prices
        aum          = estimate_aum(holdings_df)
        aum_millions = aum / 1_000_000 if aum else None

        # 4. ETF price → shares outstanding
        etf_price  = fetch_etf_price(ticker)
        shares_out = (aum / etf_price) if (aum and etf_price) else None

        upsert_snapshot(conn, {
            "date":           snap_date,
            "ticker":         ticker,
            "sector_name":    sector_name,
            "shares_out":     shares_out,
            "etf_price":      etf_price,
            "aum_millions":   aum_millions,
            "holdings_count": holdings_count,
            "fetched_at":     fetched_at,
        })
        success += 1

        if not quiet:
            aum_s    = f"${aum_millions:>9,.1f}M" if aum_millions else "        n/a"
            price_s  = f"${etf_price:>7.2f}"       if etf_price   else "    n/a"
            shares_s = f"{shares_out:>13,.0f}"      if shares_out  else "          n/a"
            print(f"date={snap_date}  AUM={aum_s}  price={price_s}  shares={shares_s}")

    return success

# ---------------------------------------------------------------------------
# Flow summary report
# ---------------------------------------------------------------------------

def print_flow_summary(conn: sqlite3.Connection, days: int = 5) -> None:
    since = (date.today() - timedelta(days=days)).isoformat()

    df = pd.read_sql_query("""
        SELECT date, ticker, sector_name, shares_out, aum_millions, etf_price
        FROM sector_snapshots
        WHERE date >= ?
        ORDER BY ticker, date
    """, conn, params=(since,))

    if df.empty:
        print("No data found. Run without --summary first to collect some snapshots.")
        return

    results = []
    for ticker, grp in df.groupby("ticker"):
        grp  = grp.sort_values("date")
        last  = grp.iloc[-1]
        first = grp.iloc[0]

        def safe_diff(col):
            a, b = first[col], last[col]
            return (b - a) if (pd.notna(a) and pd.notna(b)) else None

        results.append({
            "Ticker":     ticker,
            "Sector":     last["sector_name"],
            "Date":       last["date"],
            "AUM ($M)":   last["aum_millions"],
            "Price":      last["etf_price"],
            "AUM Chg":    safe_diff("aum_millions"),
            "Shares Chg": safe_diff("shares_out"),
        })

    out = pd.DataFrame(results).set_index("Ticker")
    # Sort: biggest inflows at top, outflows at bottom
    out = out.sort_values("AUM Chg", ascending=False, na_position="last")

    def fmt_chg(v, fmt_str, prefix="", suffix=""):
        if v is None or pd.isna(v): return "n/a"
        sign = "+" if v >= 0 else ""
        return f"{sign}{prefix}{v:{fmt_str}}{suffix}"

    W = 84
    print(f"\n{'='*W}")
    print(f"  SPDR Sector Flow Summary  |  {days}-day window  |  as of {date.today()}")
    print(f"{'='*W}")
    print(f"  {'Ticker':<6}  {'Sector':<28}  {'AUM':>11}  {'AUM Flow':>12}  {'Shares Flow':>15}")
    print(f"  {'-'*6}  {'-'*28}  {'-'*11}  {'-'*12}  {'-'*15}")

    for ticker, row in out.iterrows():
        aum_s    = f"${row['AUM ($M)']:>9,.1f}M" if pd.notna(row['AUM ($M)']) else "       n/a"
        flow_s   = fmt_chg(row['AUM Chg'],   ">9,.1f", prefix="$", suffix="M")
        shares_s = fmt_chg(row['Shares Chg'], ">13,.0f")
        print(f"  {ticker:<6}  {row['Sector']:<28}  {aum_s}  {flow_s:>12}  {shares_s:>15}")

    valid = [r["AUM Chg"] for _, r in out.iterrows()
             if r["AUM Chg"] is not None and pd.notna(r["AUM Chg"])]
    if valid:
        total = sum(valid)
        sign  = "+" if total >= 0 else ""
        print(f"\n  Total sector ETF flow ({days}d): {sign}${total:,.1f}M")

    print(f"{'='*W}\n")

# ---------------------------------------------------------------------------
# Debug mode
# ---------------------------------------------------------------------------

def debug_ticker(ticker: str) -> None:
    print(f"\nDownloading {ticker} holdings file...")
    ticker = ticker.replace('.','-')
    raw = download_holdings(ticker)
    if raw is None:
        print("Download failed.")
        return

    df_raw = pd.read_excel(BytesIO(raw), header=None, engine="openpyxl")
    print(f"\nFirst 10 raw rows (header=None):")
    print(df_raw.head(10).to_string())

    as_of, holdings = parse_holdings(raw)
    print(f"\nParsed as-of date : {as_of}")
    print(f"Holdings rows     : {len(holdings)}")
    if not holdings.empty:
        print(f"\nTop 5 holdings:")
        print(holdings.head(5).to_string(index=False))

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Track SPDR sector ETF flows via SSGA daily holdings files."
    )
    parser.add_argument("--summary", action="store_true",
                        help="Print flow summary (don't collect new data)")
    parser.add_argument("--days",    type=int, default=5,
                        help="Window in days for the flow summary (default: 5)")
    parser.add_argument("--quiet",   action="store_true",
                        help="Suppress per-ticker output (for cron)")
    parser.add_argument("--debug",   metavar="TICKER",
                        help="Print raw file structure for one ticker and exit")
    parser.add_argument("--db",      default=str(DB_PATH),
                        help=f"SQLite DB path (default: {DB_PATH})")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.WARNING,
        format="%(levelname)s: %(message)s",
    )

    conn = sqlite3.connect(Path(args.db))
    init_db(conn)

    if args.debug:
        debug_ticker(args.debug)
        conn.close()
        return

    if args.summary:
        print_flow_summary(conn, days=args.days)
        conn.close()
        return

    if not args.quiet:
        print(f"\nCollecting sector snapshots → {args.db}\n")

    n = collect_snapshot(conn, quiet=args.quiet)

    if not args.quiet:
        print(f"\nDone. {n}/{len(SECTORS)} tickers saved.")
        print("Run with --summary to see flow trends.\n")

    conn.close()


if __name__ == "__main__":
    main()

