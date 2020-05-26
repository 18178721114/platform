#!/bin/bash
#新账号取数脚本 每天2,6点 1分执行
searchdate=`date -d'2 days ago' +%Y-%m-%d` # linux
#searchdate=`date -v-2d "+%Y-%m-%d"` # mac

php artisan TalkdataForeignUserTjReportCommond $searchdate

echo -e "\n抓取成功日期为:" $searchdate

searchdate=`date -d'2 days ago' +%Y-%m-%d` # linux
#searchdate=`date -v-2d "+%Y-%m-%d"` # mac

php artisan TalkdataForeignUserSessionTjCommond $searchdate

echo -e "\n抓取成功日期为:" $searchdate

# 新增活跃处理过程
php artisan TdForeignUserTjHandleProcesses $searchdate

echo -e "\n处理成功日期为:" $searchdate

#searchdate=`date -d'2 days ago' +%Y-%m-%d` # linux
##searchdate=`date -v-2d "+%Y-%m-%d"` # mac
#
#php artisan TalkdataForeignTotalTjReportCommond $searchdate
#
#echo -e "\n抓取成功日期为:" $searchdate
#
#searchdate=`date -d'2 days ago' +%Y-%m-%d` # linux
##searchdate=`date -v-2d "+%Y-%m-%d"` # mac
#
#php artisan TalkdataForeignTotalSessionTjCommond $searchdate
#
#echo -e "\n抓取成功日期为:" $searchdate



searchday=`date -d'1 days ago' +%Y-%m-%d` # linux
#searchday=`date -v-1d "+%Y-%m-%d"` # mac

php artisan TalkdataForeignUserTjReportCommond  $searchday

echo -e "\n抓取成功日期为:" $searchday


searchdate=`date -d'1 days ago' +%Y-%m-%d` # linux
#searchdate=`date -v-2d "+%Y-%m-%d"` # mac

php artisan TalkdataForeignUserSessionTjCommond $searchdate

echo -e "\n抓取成功日期为:" $searchdate

# 新增活跃处理过程
php artisan TdForeignUserTjHandleProcesses $searchdate

echo -e "\n处理成功日期为:" $searchdate

#searchdate=`date -d'1 days ago' +%Y-%m-%d` # linux
##searchdate=`date -v-1d "+%Y-%m-%d"` # mac
#
#php artisan TalkdataForeignTotalTjReportCommond $searchdate
#
#echo -e "\n抓取成功日期为:" $searchdate
#
#searchdate=`date -d'1 days ago' +%Y-%m-%d` # linux
##searchdate=`date -v-1d "+%Y-%m-%d"` # mac
#
#php artisan TalkdataForeignTotalSessionTjCommond $searchdate
#
#echo -e "\n抓取成功日期为:" $searchdate

# 新增活跃处理过程
#searchdate=`date -d'1 days ago' +%Y-%m-%d` # linux
#searchdate=`date -v-1d "+%Y-%m-%d"` # mac

