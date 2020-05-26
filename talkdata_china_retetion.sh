#!/bin/bash
#新账号取数脚本 每天6点 04分执行
searchdate=`date -d'2 days ago' +%Y-%m-%d` # linux
#searchdate=`date -v-2d "+%Y-%m-%d"` # mac

php artisan TalkdataChinaKeepUserCommond $searchdate

echo -e "\n抓取成功日期为:" $searchdate
