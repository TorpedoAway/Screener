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
    moving_avg_150 = h['SMA_150'].iloc[-1]
    target = moving_avg_150 * 1.05
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
    if currentPrice > moving_avg_150 and currentPrice < target:
        if recommendationKey.lower() == 'buy':
            if targetMeanPrice > currentPrice:
                if volume > 1000000:
                    if marketCap > 50000000000:
                        print("Potential Buy",ticker,currentPrice,
                            targetMeanPrice,marketCap,recommendationKey)


def run_scanner():
    f = Files()
    data = f.read_file('/Projects/Picker/code/sp500.txt')
    for ticker in data:
        analyzer(ticker)
        time.sleep(1)

# Boiler plate call to main()
if __name__ == '__main__':
  run_scanner()

sys.exit()
