#!/bin/bash
# 获取累计数据 上一个月的 每月 7号跑
startdate=`date -d'1 months ago' +%Y-%m` # linux
#startdate=`date -v-1m "+%Y-%m"` # mac


echo $startdate
php artisan TotalCommond $startdate

