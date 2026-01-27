#!/Projects/Picker/bin/python3

import pandas as pd
import yfinance as yf
import json
import sys
import time
sys.path.append('/Projects/Picker/code/python_modules')
from Tools import Files

f = Files()
tickers = f.read_file('/Projects/Picker/code/russell2000.txt')
# Uncomment below to use a short list of stock symbols for testing.
#tickers = f.read_file('/Projects/Picker/code/test_tickers.txt')
results = []
results2 = []

for ticker in tickers:
    try:
        ticker = ticker.replace('.','')
        dat = yf.Ticker(ticker)
        # Fetch 200d to ensure we have enough data for the 150d SMA

        info = dat.info
        marketCap = info['marketCap']
        if marketCap < 1000000000:
            time.sleep(0.5)
            continue

        time.sleep(0.5)
        h = dat.history(period='200d')

        if len(h) < 150: continue

        # Calculate your MAs
        h['SMA_150'] = h['Close'].rolling(window=150).mean()
        h['SMA_50'] = h['Close'].rolling(window=50).mean()
        h['SMA_15'] = h['Close'].rolling(window=15).mean()

        # Current Values
        price = h['Close'].iloc[-1]
        previous_price = h['Close'].iloc[-2]
        m150 = h['SMA_150'].iloc[-1]
        m50 = h['SMA_50'].iloc[-1]
        m15 = h['SMA_15'].iloc[-1]

        trend_alignment = m150 < m50 and m150 < m15
        
        # Trend Logic (Is the 150d MA higher than 1 month ago?)
        m150_prev = h['SMA_150'].iloc[-21]
        is_trending_up = m150 > m150_prev
        
        # Proximity Logic (Is price within 2% of the 150d MA?)
        is_near_150 = abs(price - m150) / m150 < 0.02

        currentPrice = price
        volume = info['volume']
        averageVolume = info['averageVolume']
        averageVolume10days = info['averageVolume10days']
        fiftyTwoWeekHigh = info['fiftyTwoWeekHigh']
        near52week = price >= fiftyTwoWeekHigh * 0.85
        heavy_buying = (averageVolume10days > averageVolume * 1.2) and (price > previous_price)
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
            targetMeanPrice = currentPrice
        forwardPE = 'N/A'
        if 'forwardPE' in info:
            forwardPE = info['forwardPE']
        trailingPE = 'N/A'
        if 'trailingPE' in info:
            trailingPE = info['trailingPE']

        if "strong" in recommendationKey.lower() and heavy_buying:
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
               'Recommendation': recommendationKey,
             })

        # If it meets core buy criteria, add to results
        if is_trending_up and is_near_150:
            #if 'buy' in recommendationKey.lower():
                if targetMeanPrice > currentPrice:
                    if volume > 1000000 and trend_alignment:
                        if marketCap > 5000000000 and heavy_buying and near52week:            
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
                              'Recommendation': recommendationKey,
                            })
                            print(f"Match found: {ticker}")

    except Exception as e:
        print(f"Could not process {ticker}: {e}")
    
    time.sleep(0.5)

outfile = '/var/www/html/scanner/Russell_2000.csv'
outfile2 = '/var/www/html/scanner/strong_buy_r2000.csv'

# Create DataFrame from results
final_df = pd.DataFrame(results)
final_df2 = pd.DataFrame(results2)

# Save to CSV
final_df.to_csv(outfile, index=False)
print(f"Scan complete. Results saved to {outfile}")
final_df2.to_csv(outfile2, index=False)
print(f"Scan complete. Strong Buy Recommendations saved to {outfile2}")
