#!/bin/bash
searchdate=`date -d'2 days ago' +%Y-%m-%d` # linux
#searchdate=`date -v-2d "+%Y-%m-%d"` # mac

php artisan AppflurryTjReportCommond $searchdate

echo -e "\n抓取成功日期为:" $searchdate

searchday=`date -d'1 days ago' +%Y-%m-%d` # linux
#searchday=`date -v-1d "+%Y-%m-%d"` # mac
php artisan AppflurryTjReportCommond $searchday

echo -e "\n抓取成功日期为:" $searchday


#!/bin/bash
searchdate=`date -d'3 days ago' +%Y-%m-%d` # linux
#searchdate=`date -v-2d "+%Y-%m-%d"` # mac

php artisan AppflurryTjReportCommond $searchdate

echo -e "\n抓取成功日期为:" $searchdate



#!/bin/bash
searchdate=`date -d'4 days ago' +%Y-%m-%d` # linux
#searchdate=`date -v-2d "+%Y-%m-%d"` # mac

php artisan AppflurryTjReportCommond $searchdate

echo -e "\n抓取成功日期为:" $searchdate
