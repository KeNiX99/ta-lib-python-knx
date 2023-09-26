#!/bin/sh
#rm fbroker-99.log
#clear
while [ 1 ] ; do
rm -f /logs/log-*.tar.gz
tar zcvf /logs/log-backup-$(date +%Y%m%d%H%M).tar.gz /logs/*.log 
rm -f /logs/*.log
python  main.py
done 