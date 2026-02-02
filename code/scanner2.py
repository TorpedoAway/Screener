#!/Projects/Picker/bin/python3

import pandas as pd
import yfinance as yf
import json
import sys,os
import time
sys.path.append('/Projects/Picker/code/python_modules')
from Tools import Files

f = Files()
# Uncomment below to use a short list of stock symbols for testing.
#tickers = f.read_file('/Projects/Picker/code/test_tickers.txt')
results = []
results2 = []
results3 = []

conf = os.getenv('ScanType')

if conf == 'sp500':
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
        'datfile' : '/Projects/Picker/code/sp500.dat'
    }   
elif conf == 'russell2000':
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
        'datfile' : '/Projects/Picker/code/russell2000.txt'
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


tickers = f.read_file( config['datfile'] ) 


for ticker in tickers:
    try:
        #print(ticker)
        dat = yf.Ticker(ticker)
        info = dat.info
        marketCap = info['marketCap']
        volume = info['volume']
        if marketCap < config['minMarketCap'] or volume < config['minVol']:
            time.sleep(config['sleepTime'])
            continue
        time.sleep(config['sleepTime'])
        # Fetch 200d to ensure we have enough data for the 150d SMA
        h = dat.history(period=config['hist'])
        #cols = h.columns
        #print (cols)
        
        if len(h) < config['minDays']: continue

        # Calculate Moving Averages
        h['SMA_150'] = h['Close'].rolling(window=150).mean()
        h['SMA_50'] = h['Close'].rolling(window=50).mean()
        h['SMA_21'] = h['Close'].rolling(window=21).mean()
        h['SMA_15'] = h['Close'].rolling(window=15).mean()

        # Current Values
        price = info['currentPrice']
        m150 = h['SMA_150'].iloc[-1]
        m50 = h['SMA_50'].iloc[-1]
        m21 = h['SMA_21'].iloc[-1]
        m15 = h['SMA_15'].iloc[-1]
        vol = h['Volume'].iloc[-1]

        # Calculate 21 Day EMA
        h['EMA_21'] = h['Close'].ewm(span=21, adjust=False).mean()

        # Get today's and yesterday's values to detect the 'cross'
        ema_today = h['EMA_21'].iloc[-1]
        ema_yesterday = h['EMA_21'].iloc[-2]

        sma50_today = h['SMA_50'].iloc[-1]
        sma50_yesterday = h['SMA_50'].iloc[-2]

        # Logic: Yesterday EMA was below SMA, but today it is above
        is_bullish_crossover = (ema_yesterday < sma50_yesterday) and (ema_today > sma50_today)

        # Logic: Yesterday EMA was above SMA, but today it is below (Bearish/Exit signal)
        is_bearish_crossover = (ema_yesterday > sma50_yesterday) and (ema_today < sma50_today)

        if is_bullish_crossover:
            signal_type = "BULLISH CROSS"
            print(f"{ticker}: Bullish Cross")
        elif is_bearish_crossover:
            signal_type = "BEARISH CROSS"
        else:
            signal_type = "NO CROSS"

        crossed21ema =  (h['Close'].iloc[-2]  < ema_yesterday) and (h['Close'].iloc[-1] > ema_today)
        if crossed21ema:
            print(f"{ticker}: Crossed 21d EMA")
        
        trending_up =   h['Close'].iloc[-1] >  h['Close'].iloc[-2] >  h['Close'].iloc[-3]
        trending_up_sharply = h['Close'].iloc[-1] >  h['Close'].iloc[-2] * config['sharpUptrend'] >  h['Close'].iloc[-3] * config['sharpUptrend']

        #if trending_up_sharply:
        #    print("Trending up",ticker)
       
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
        heavy_buying = (vol > int(averageVolume * config['heavyVolMultiplier']) ) and (price > h['Close'].iloc[-2])
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

        short_term_confirmation = perfect_trend_alignment and crossed21ema and trending_up_sharply and 'buy' in recommendationKey.lower() and perfect_trend_alignment


        if short_term_confirmation:
            print(f"cwShort term trade buy match, {ticker}, {name}")
            results3.append({
               'Ticker': ticker,
               'Company': name,
               'Price': round(price, 2),
               'Target Price': round(targetMeanPrice, 2),
               'SMA_150': round(m150, 2),
               'Dist_from_150': round(((price - m150) / m150) * 100, 2),
               'SMA_50': round(m50, 2),
               'SMA_15': round(m15, 2),
               'Volume': h['Volume'].iloc[-1],
               'Average Volume': averageVolume,
               'trailingPE': trailingPE,
               'forwardPE': forwardPE,
               'marketCap (Billions)': round(int(marketCap)/1000/1000/1000),
               'Signal' : signal_type,
               'Recommendation': recommendationKey,
             })
        
        if "strong" in recommendationKey.lower() and heavy_buying and slope and trending_up:
            print(f"Strong buy match, {ticker}, {name}")
            results2.append({
               'Ticker': ticker,
               'Company': name,
               'Price': round(price, 2),
               'Target Price': round(targetMeanPrice, 2),
               'SMA_150': round(m150, 2),
               'Dist_from_150': round(((price - m150) / m150) * 100, 2),
               'SMA_50': round(m50, 2),
               'SMA_15': round(m15, 2),
               'Volume': h['Volume'].iloc[-1],
               'Average Volume': averageVolume,
               'trailingPE': trailingPE,
               'forwardPE': forwardPE,
               'marketCap (Billions)': round(int(marketCap)/1000/1000/1000),
               'Signal' : signal_type,
               'Recommendation': recommendationKey,
             })

        # If it meets core buy criteria, add to results
        #buy_candidate = targetMeanPrice > currentPrice and trend_alignment and heavy_buying and near52week and is_near_150 and slope and trending_up_sharply
        buy_candidate = heavy_buying and is_near_150 and slope and trending_up_sharply

        #
        #
        if buy_candidate:
                print(f"Match found: {ticker}, {name}")
                results.append({
                  'Ticker': ticker,
                  'Company': name,
                  'Price': round(price, 2),
                  'Target Price': round(targetMeanPrice, 2),
                  'SMA_150': round(m150, 2),
                  'Dist_from_150': round(((price - m150) / m150) * 100, 2),
                  'SMA_50': round(m50, 2),
                  'SMA_15': round(m15, 2),
                  'Volume': h['Volume'].iloc[-1],
                  'Average Volume': averageVolume,
                  'trailingPE': trailingPE,
                  'forwardPE': forwardPE,
                  'marketCap (Billions)': round(int(marketCap)/1000/1000/1000),
                  'Signal' : signal_type,
                  'Recommendation': recommendationKey,
                })

    except Exception as e:
        print(f"Could not process {ticker}: {e}")
    
    time.sleep(0.5)

outfile = f"/var/www/html/scanner/{conf}_SMA150Bounce_results.csv"
outfile2 = f"/var/www/html/scanner/{conf}_strong_buy_recommendations.csv"
outfile3 = f"/var/www/html/scanner/{conf}_short_term_trade.csv"

# Create DataFrame from results
final_df = pd.DataFrame(results)
final_df2 = pd.DataFrame(results2)
final_df3 = pd.DataFrame(results3)

# Save to CSV
final_df.to_csv(outfile, index=False)
print(f"Scan complete. Results saved to {outfile}")
final_df2.to_csv(outfile2, index=False)
print(f"Strong Buy Recommendations saved to {outfile2}")
final_df3.to_csv(outfile3, index=False)
print(f"Short Term Trade Buy Recommendations saved to {outfile2}")
