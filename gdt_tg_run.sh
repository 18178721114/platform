#!/bin/bash
# 1 8,17 * * * php artisan GdtTgReportCommond
searchdate=`date -d'3 days ago' +%Y-%m-%d` # linux
#searchdate=`date -v-3d "+%Y-%m-%d"` # mac

php artisan GdtTgReportCommond $searchdate

echo -e "\n抓取成功日期为:" $searchdate

searchdate=`date -d'2 days ago' +%Y-%m-%d` # linux
#searchdate=`date -v-2d "+%Y-%m-%d"` # mac

php artisan GdtTgReportCommond $searchdate

echo -e "\n抓取成功日期为:" $searchdate

searchdate=`date -d'1 days ago' +%Y-%m-%d` # linux
#searchdate=`date -v-1d "+%Y-%m-%d"` # mac

php artisan GdtTgReportCommond $searchdate

echo -e "\n抓取成功日期为:" $searchdate
