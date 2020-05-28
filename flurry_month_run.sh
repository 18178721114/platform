#!/bin/bash
# 获取flurry 月活用户数据 每月1号执行一次
startdate=`date -d'1 months ago' +%Y-%m-01` # linux
#startdate=`date -v-1m "+%Y-%m-01"` # mac

enddate=`date -d'0 months ago' +%Y-%m-01` # linux
#enddate=`date -v+0m "+%Y-%m-01"` # mac

echo $startdate
echo $enddate
# 获取flurry 月活用户数据
php artisan AppflurryTjMonthReportCommond $startdate $enddate
