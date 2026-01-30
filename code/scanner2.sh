#!/usr/bin/bash
#
log='/var/www/html/scanner/scanner2.txt'

echo "`date`: Begin SP500 Stock Scan..." >> $log
/Projects/Picker/Screener/code/scanner2.py
echo "`date`: End SP500 Stock Scan..." >> $log

echo "`date`: Begin Russell 2000 Stock Scan..." >> $log
/Projects/Picker/Screener/code/scanner3.py
echo "`date`: End Stock Scan..." >> $log
