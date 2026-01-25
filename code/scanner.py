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
    h['SMA_150'] = h['Close'].rolling(window=150).mean()
    h['SMA_15'] = h['Close'].rolling(window=15).mean()
    m15 = h['SMA_15'].iloc[-1]
    moving_avg_150 = h['SMA_150'].iloc[-1]

    uptrend = False
    if h['SMA_150'].iloc[-30] < moving_avg_150:
        uptrend = True

    uptrend_m15 = False
    if h['SMA_15'].iloc[-15] < m15:
        uptrend_m15 = True
    
    #if uptrend and uptrend_m15:
    #    print(f"{ticker} Uptrend_150d: {uptrend} Uptrend_15d {uptrend_m15}")
    target = moving_avg_150 * 1.07
    lower_target = moving_avg_150 * .90
    info = dat.info
    #pprint.pp(info)
    if 'currentPrice' not in info:
        print(ticker, "No current price")
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
    if currentPrice >= lower_target and currentPrice <= target:
        if recommendationKey.lower() == 'buy':
            if targetMeanPrice > currentPrice:
                if volume > 1000000:
                    if marketCap > 50000000000:
                        if uptrend and uptrend_m15:
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
            continue
        outlist.append(packet)
        print(packet)
        time.sleep(2)
    f.write_csv(outfile,outlist)

# Boiler plate call to main()
if __name__ == '__main__':
  run_scanner()

sys.exit()
