#!/usr/bin/bash
#
log='/var/www/html/scanner/Stock_Scan.log'

export ScanType='sp500'
echo "`date`: Begin SP500 Stock Scan..." > $log
/Projects/Picker/Screener/code/scanner2.py
echo "`date`: End SP500 Stock Scan..." >> $log
exit

export ScanType='ETF'
echo "`date`: Begin ETF Stock Scan..." >> $log
/Projects/Picker/Screener/code/scanner2.py
echo "`date`: End ETF Stock Scan..." >> $log


export ScanType='r2k'
echo "`date`: Begin Russell 2000 Stock Scan..." >> $log
/Projects/Picker/Screener/code/scanner2.py
echo "`date`: End Stock Scan..." >> $log
