#!/bin/bash
#新账号取数脚本 每天10点10分跑
#searchdate=`date -d'2 days ago' +%Y-%m-%d`

if [ $1 ]
then
        searchday=$1
else
        searchday=`date -d'3 days ago' +%Y-%m-%d` # linux
#        searchday=`date -v-3d "+%Y-%m-%d"` # mac
fi


#php ./talkTetention.php $searchday >>/data/logs/$searchday
php artisan TalkdataForeignUserRetentionTjCommond $searchday

echo -e "\n抓取成功日期为:" $searchday




