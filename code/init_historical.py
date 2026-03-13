#!/Projects/Picker/bin/python3

import pandas as pd
import yfinance as yf
import json
import sys,os
import time
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
sys.path.append('/Projects/Picker/code/python_modules')
from Tools import Files

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH = Path("/Projects/Picker/code/historical.db")




def init_db(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS historical (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            date            TEXT    NOT NULL,
            ticker          TEXT    NOT NULL,
            close           INTEGER,
            UNIQUE(date, ticker)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_date_ticker
        ON historical(date, ticker)
    """)
    conn.commit()


def insert_historical(conn,cur,data) -> None:
    cur.execute('''INSERT OR IGNORE INTO historical
                    (date, ticker, close)
                    VALUES ( ?,?,? )''', (data[0],data[1],data[2]))

    conn.commit()


conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
init_db(conn)

f = Files()
# Uncomment below to use a short list of stock symbols for testing.
#tickers = f.read_file('/Projects/Picker/code/test_tickers.txt')
results = []
results2 = []
results3 = []
results4 = []

conf = os.getenv('ScanType')

if conf == 'sp500':
    config = {
        'minMarketCap' : 50000000000,
        'minVol' : 300000,
        'sleepTime' : 0.5,
        'hist' : '3d',
        'minDays' : 150,
        'slope150d' : 1.02,
        'sharpUptrend' : 1.015,
        'heavyVolMultiplier' : 1.3,
        'near150Under' : 0.02,
        'near150Over' : 0.02,
        'near52Week' : 0.1,
        'datfile' : '/Projects/Picker/code/sp500.dat'
    }   
elif conf == 'r2k':
    config = {
        'minMarketCap' : 300000000,
        'minVol' : 300000,
        'sleepTime' : 0.5,
        'hist' : '3d',
        'minDays' : 150,
        'slope150d' : 1.02,
        'sharpUptrend' : 1.015,
        'heavyVolMultiplier' : 1.3,
        'near150Under' : 0.02,
        'near150Over' : 0.06,
        'near52Week' : 0.7,
        'datfile' : '/Projects/Picker/code/r2k.dat'
    }
elif conf == 'ETF':
    config = {
        'minMarketCap' : 300000000,
        'minVol' : 300000,
        'sleepTime' : 0.5,
        'hist' : '3d',
        'minDays' : 150,
        'slope150d' : 1.02,
        'sharpUptrend' : 1.015,
        'heavyVolMultiplier' : 1.3,
        'near150Under' : 0.02,
        'near150Over' : 0.06,
        'near52Week' : 0.7,
        'datfile' : '/Projects/Picker/code/ETF.dat'
    }
    
else:
    print("No valid ScanType set...")
    sys.exit()


tickers = f.read_file( config['datfile'] ) 

for ticker in tickers:
    try:
        #print(ticker)
        dat = yf.Ticker(ticker)
        info = dat.info
        filename = f"/Projects/Picker/code/data/{ticker}_info.json"
        with open(filename, 'w') as file:
            json.dump(info, file, indent=4)
        
        time.sleep(config['sleepTime'])
        h = dat.history(period=config['hist'])

        for date, row in h.iterrows():
            datadate = date.strftime('%Y-%m-%d')
            close = round(row['Close'],2)
            insert_historical(conn,cur,[datadate,ticker,close])
    except Exception as e:
        print(f"Could not process {ticker}: {e}")

    time.sleep(0.5)
            
