#!/usr/bin/bash
#
log='/var/www/html/scanner/Data_Collection.log'

#export ScanType='SPECIAL'
#echo "`date`: Begin Special Data Collection..." > $log
#/Projects/Picker/Screener/code/init_historical.py
#echo "`date`: End Special Scan..." >> $log
#exit


export ScanType='ETF'
echo "`date`: Begin ETFs Data Collection..." >> $log
/Projects/Picker/Screener/code/init_historical.py
echo "`date`: End ETFs Scan..." >> $log


echo ""  >> $log
export ScanType='sp500'
echo "`date`: Begin SP500 Data Collection..." >> $log
/Projects/Picker/Screener/code/init_historical.py
echo "`date`: End SP500 Scan..." >> $log

echo ""  >> $log
export ScanType='r2k'
echo "`date`: Begin Russell 2000 Data Collection..." >> $log
/Projects/Picker/Screener/code/init_historical.py
echo "`date`: End Russell 2000 Scan..." >> $log
echo "`date`: End Stock Scan..." >> $log
echo "`date`: Data Collection Complete..." >> $log
