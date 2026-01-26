#!/Projects/Picker/bin/python3

import yfinance as yf
import sys, os, time
import pprint
sys.path.append('/Projects/Picker/code/python_modules')
from Tools import Files

tickers = ['aapl']

def analyzer(ticker):
    dat = yf.Ticker(ticker)
    h = dat.history(period='200d')
    h['SMA_15'] = h['Close'].rolling(window=15).mean()
    h['SMA_50'] = h['Close'].rolling(window=50).mean()
    h['SMA_150'] = h['Close'].rolling(window=150).mean()

    sma15 = h['SMA_15'].iloc[-1]
    sma50 = h['SMA_50'].iloc[-1]
    sma150 = h['SMA_150'].iloc[-1]

    uptrend_sma150 = False
    if h['SMA_150'].iloc[-30] < sma150:
        uptrend_sma150 = True


    uptrend_sma50 = False
    if h['SMA_50'].iloc[-30] < sma50:
        uptrend_sma50 = True

    uptrend_sma15 = False
    if h['SMA_15'].iloc[-15] < sma15:
        uptrend_sma15 = True
    
    #if uptrend and uptrend_m15:
    #    print(f"{ticker} Uptrend_150d: {uptrend} Uptrend_15d {uptrend_m15}")
    target = sma150 * 1.07
    lower_target = sma150 * .95
    info = dat.info
    #pprint.pp(info)
    if 'currentPrice' not in info:
        #print(ticker, "No current price")
        return
    currentPrice = info['currentPrice']
    volume = info['volume']
    if 'recommendationKey' in info:
        recommendationKey = info['recommendationKey']
    else:
        recommendationKey = 'N/A'
    if 'targetMeanPrice' in info:
        targetMeanPrice = info['targetMeanPrice']
    else:
        targetMeanPrice = currentPrice
    marketCap = info['marketCap']
    forwardPE = 'N/A'
    if 'forwardPE' in info:
        forwardPE = info['forwardPE']
    trailingPE = 'N/A'
    if 'trailingPE' in info:
        trailingPE = info['trailingPE']
    packet = ["Potential Buy",ticker,currentPrice,
              targetMeanPrice,marketCap,trailingPE,forwardPE,recommendationKey]

    # Check if price is within 2% of the 150-day MA
    is_near_ma = abs(currentPrice - sma150) / sma150 < 0.02

    # Check if today's close is higher than yesterday (the bounce)
    is_bouncing = h['Close'].iloc[-1] > h['Close'].iloc[-2]

    # Combined Signal
    #h['Buy_Signal'] = is_near_ma & is_bouncing & (h['SMA_150'] > h['SMA_150'].shift(20))
    #print(h['Buy_Signal'])

    # Calculate the slope over the last month (approx 21 trading days)
    h['SMA_150_Slope'] = h['SMA_150'] - h['SMA_150'].shift(21)
    slope_150 = h['SMA_150_Slope'].iloc[-1]

    # Logical check: Is the 150-day average higher than it was a month ago?
    is_trending_up = slope_150 > 0
    #print(f"Uptrend: {is_trending_up} Near 150ma: {is_near_ma} Bouncing: {is_bouncing}") 
    if is_trending_up and is_near_ma and is_bouncing:
        print(f"Stock, {ticker}, is in an uptrend and is  bouncing off 150 day ma")

    #if currentPrice >= lower_target and currentPrice <= target:
    if is_trending_up and is_near_ma and is_bouncing:
        if 'buy' in recommendationKey.lower():
            if targetMeanPrice > currentPrice:
                if volume > 1000000:
                    if marketCap > 50000000000:
                            return packet


def run_scanner():
    f = Files()
    outfile = '/var/www/html/results.csv'
    heading = [
      "Analysis",'ticker','currentPrice',
      'targetMeanPrice','marketCap','trailingPE','forwardPE',
      'recommendationKey'
     ]
    outlist = list()
    outlist.append(heading)
    data = f.read_file('/Projects/Picker/code/sp500.txt')
    for ticker in data:
        packet = analyzer(ticker)
        if packet is None:
            time.sleep(1)
            continue
        outlist.append(packet)
        print(packet)
        time.sleep(1)
    f.write_csv(outfile,outlist)

# Boiler plate call to main()
if __name__ == '__main__':
  run_scanner()

sys.exit()
