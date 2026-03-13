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
        'minMarketCap' : 10000000000,
        'minVol' : 100000,
        'sleepTime' : 0.5,
        'hist' : '200d',
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
        'hist' : '200d',
        'minDays' : 150,
        'slope150d' : 1.02,
        'sharpUptrend' : 1.015,
        'heavyVolMultiplier' : 1.3,
        'near150Under' : 0.02,
        'near150Over' : 0.06,
        'near52Week' : 0.7,
        'datfile' : '/Projects/Picker/code/r2k.dat'
    }
elif conf == 'test':
    config = {
        'minMarketCap' : 5000000000,
        'minVol' : 300000,
        'sleepTime' : 0.5,
        'hist' : '200d',
        'minDays' : 150,
        'slope150d' : 1.02,
        'sharpUptrend' : 1.015,
        'heavyVolMultiplier' : 1.3,
        'near150Under' : 0.02,
        'near150Over' : 0.06,
        'near52Week' : 0.7,
        'datfile' : '/Projects/Picker/code/test.dat'
    }
else:
    print("No ScanType set...")
    sys.exit()

historicalDB = "/Projects/Picker/code/historical.db"
DataDir = "/Projects/Picker/code/data"
tickers = f.read_file( config['datfile'] ) 

for ticker in tickers:
    try:
        #print(ticker)
        connection = sqlite3.connect(historicalDB)
        sql_query = f"SELECT date,close FROM historical where ticker = \"{ticker}\" order by date" 
        h = pd.read_sql_query(sql_query, connection)

        # 4. close the connection
        connection.close()        



        info_file = f"{DataDir}/{ticker}_info.json"
        try:
            with open(info_file, 'r') as file:
                info = json.load(file)

        except FileNotFoundError:
            print(f"Error: The file {info_file} was not found.")
        except json.JSONDecodeError:
            print("Error: Failed to decode JSON from the file (malformed JSON).")

        ##################################################################
        marketCap = info['marketCap']
        volume = info['volume']
        marketCapBillions = marketCap/1000/1000/1000
        if marketCapBillions > 1000:
            marketCapBillions = marketCapBillions/1000
            marketCapBillions = round(marketCapBillions,2)
            marketCapBillions = str(marketCapBillions) + " Trillion"
        else: 
            marketCapBillions = round(marketCapBillions,2)
            marketCapBillions = str(marketCapBillions) + " Billion"
        
        if marketCap < config['minMarketCap']:
            print(f"{ticker} market cap {marketCapBillions} is too small. Skipping")
            continue
        if volume < config['minVol']:
            print(f"{ticker} Volume {volume} is too low. Skipping")
            continue


        if len(h) < config['minDays']: continue

        # Calculate Moving Averages
        h['SMA_150'] = h['close'].rolling(window=150).mean()
        h['SMA_50'] = h['close'].rolling(window=50).mean()
        h['SMA_21'] = h['close'].rolling(window=21).mean()
        h['SMA_15'] = h['close'].rolling(window=15).mean()

        # Current Values
        price = info['currentPrice']
        m150 = h['SMA_150'].iloc[-1]
        m50 = h['SMA_50'].iloc[-1]
        m21 = h['SMA_21'].iloc[-1]
        m15 = h['SMA_15'].iloc[-1]
        vol = info['volume']

        # Calculate 21 Day EMA
        h['EMA_21'] = h['close'].ewm(span=21, adjust=False).mean()

        # Get today's and yesterday's values to detect the 'cross'
        ema_today = h['EMA_21'].iloc[-1]
        ema_yesterday = h['EMA_21'].iloc[-2]

   
        sma50_today = h['SMA_50'].iloc[-1]
        sma50_yesterday = h['SMA_50'].iloc[-2]

        # Logic: Yesterday EMA was below SMA, but today it is above
        is_bullish_crossover = (ema_yesterday < sma50_yesterday) and (ema_today > sma50_today)

        # Logic: Yesterday EMA was above SMA, but today it is below (Bearish/Exit signal)
        is_bearish_crossover = (ema_yesterday > sma50_yesterday) and (ema_today < sma50_today)

        breakout =  (ema_today > sma50_today) and (ema_today < sma50_today * 1.02) and (sma50_today > m150) and (sma50_today < m150 * 1.07) and (price < ema_today * 1.02) and (price > ema_today) and (h['close'].iloc[-1] > h['close'].iloc[-2])


        if breakout:
            print(f"{ticker}: BREAKOUT")
        if is_bullish_crossover:
            signal_type = "BULLISH CROSS"
            print(f"{ticker}: Bullish Cross")
        elif is_bearish_crossover:
            signal_type = "BEARISH CROSS"
        else:
            signal_type = "NO CROSS"

        crossed21ema =  (h['close'].iloc[-2]  < ema_yesterday) and (h['close'].iloc[-1] > ema_today)
        
        trending_up =   h['close'].iloc[-1] >  h['close'].iloc[-2] >  h['close'].iloc[-3]
        trending_up_sharply = h['close'].iloc[-1] >  h['close'].iloc[-2] * config['sharpUptrend'] >  h['close'].iloc[-3] * config['sharpUptrend']

       
        trend_alignment = m150 < m50 and m150 < m15
        perfect_trend_alignment = m150 < m50  < h['EMA_21'].iloc[-1]

        
        # Trend Logic (Is the 150d MA higher than 1 month ago?)
        m150_prev = h['SMA_150'].iloc[-21]
        is_trending_up = m150 > m150_prev
        slope = ( h['SMA_150'].iloc[-30] * config['slope150d'] ) <= m150
         

        # Proximity Logic (Is price within 2% of the 150d MA?)
        if price - m150 < 0:
            is_near_150 = abs(price - m150) / m150 < config['near150Under']
        else:
            is_near_150 = abs(price - m150) / m150 < config['near150Over']

            

        currentPrice = price
        averageVolume = info['averageVolume']
        averageVolume10days = info['averageVolume10days']
        fiftyTwoWeekHigh = info['fiftyTwoWeekHigh']
        near52week = price >= fiftyTwoWeekHigh * config['near52Week']
        heavy_buying = (vol > int(averageVolume * config['heavyVolMultiplier']) ) and (price > h['close'].iloc[-2])
        name = ticker
        if 'shortName' in info:
            name = info['shortName']
        #print(averageVolume10days,int(averageVolume * 1.2))
        if 'recommendationKey' in info:
            recommendationKey = info['recommendationKey']
        else:
            recommendationKey = 'N/A'
        if 'targetMeanPrice' in info:
            targetMeanPrice = info['targetMeanPrice']
        else:
            targetMeanPrice = currentPrice + 1
        forwardPE = 'N/A'
        if 'forwardPE' in info:
            forwardPE = info['forwardPE']
        trailingPE = 'N/A'
        if 'trailingPE' in info:
            trailingPE = info['trailingPE']

        short_term_confirmation = slope and crossed21ema and trending_up_sharply and 'buy' in recommendationKey.lower() and perfect_trend_alignment

        
        #if "strong" in recommendationKey.lower() and heavy_buying and slope and trending_up:
        if "strong" in recommendationKey.lower() and heavy_buying:
            print(f"Strong buy match, {ticker}, {name}")
            results2.append({
               'Ticker': ticker,
               'Company': name,
               'Reason Included?' : "Strong buy and uptrend",
               'Price': round(price, 2),
               'Target Price': round(targetMeanPrice, 2),
               'SMA_150': round(m150, 2),
               'Dist_from_150': round(((price - m150) / m150) * 100, 2),
               'SMA_50': round(m50, 2),
               'SMA_15': round(m15, 2),
               'Volume': vol,
               'Average Volume': averageVolume,
               'trailingPE': trailingPE,
               'forwardPE': forwardPE,
               'marketCap (Billions)': round(int(marketCap)/1000/1000/1000),
               'Signal' : signal_type,
               'Possible Breakout?' : breakout,
               'Recommendation': recommendationKey,
             })


        #buy_candidate = heavy_buying and is_near_150 and slope and trending_up_sharply
        buy_candidate = heavy_buying and is_near_150 

        #
        #
        if buy_candidate:
                print(f"Match found: {ticker}, {name}")
                results.append({
                  'Ticker': ticker,
                  'Company': name,
                  'Reason Included?' : "150ma bounce and sharp uptrend",
                  'Price': round(price, 2),
                  'Target Price': round(targetMeanPrice, 2),
                  'SMA_150': round(m150, 2),
                  'Dist_from_150': round(((price - m150) / m150) * 100, 2),
                  'SMA_50': round(m50, 2),
                  'SMA_15': round(m15, 2),
                  'Volume': vol,
                  'Average Volume': averageVolume,
                  'trailingPE': trailingPE,
                  'forwardPE': forwardPE,
                  'marketCap (Billions)': round(int(marketCap)/1000/1000/1000),
                  'Signal' : signal_type,
                  'Possible Breakout?' : breakout,
                  'Recommendation': recommendationKey,
                })

        #
        if  short_term_confirmation:
                print(f"Match found: {ticker}, {name}")
                results.append({
                  'Ticker': ticker,
                  'Company': name,
                  'Reason Included?' : "Short term confirmation",
                  'Price': round(price, 2),
                  'Target Price': round(targetMeanPrice, 2),
                  'SMA_150': round(m150, 2),
                  'Dist_from_150': round(((price - m150) / m150) * 100, 2),
                  'SMA_50': round(m50, 2),
                  'SMA_15': round(m15, 2),
                  'Volume': vol,
                  'Average Volume': averageVolume,
                  'trailingPE': trailingPE,
                  'forwardPE': forwardPE,
                  'marketCap (Billions)': round(int(marketCap)/1000/1000/1000),
                  'Signal' : signal_type,
                  'Possible Breakout?' : breakout,
                  'Recommendation': recommendationKey,
                })


        #
        if   (breakout and slope):
                print(f"Match found: {ticker}, {name}")
                results.append({
                  'Ticker': ticker,
                  'Company': name,
                  'Reason Included?' : "Breakout",
                  'Price': round(price, 2),
                  'Target Price': round(targetMeanPrice, 2),
                  'SMA_150': round(m150, 2),
                  'Dist_from_150': round(((price - m150) / m150) * 100, 2),
                  'SMA_50': round(m50, 2),
                  'SMA_15': round(m15, 2),
                  'Volume': vol,
                  'Average Volume': averageVolume,
                  'trailingPE': trailingPE,
                  'forwardPE': forwardPE,
                  'marketCap (Billions)': round(int(marketCap)/1000/1000/1000),
                  'Signal' : signal_type,
                  'Possible Breakout?' : breakout,
                  'Recommendation': recommendationKey,
                })


    except Exception as e:
        print(f"Could not process {ticker}: {e}")
    

outfile = f"/var/www/html/scanner/{conf}_results.csv"
outfile2 = f"/var/www/html/scanner/{conf}_strong_buy_recommendations.csv"

# Create DataFrame from results
final_df = pd.DataFrame(results)
final_df2 = pd.DataFrame(results2)

# Save to CSV
final_df.to_csv(outfile, index=False)
print(f"Scan complete. Results saved to {outfile}")
final_df2.to_csv(outfile2, index=False)
print(f"Strong Buy Recommendations saved to {outfile2}")
