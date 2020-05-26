#!/usr/bin/env bash

#开始时间
startdate=`date -d'0 days ago' +"%Y-%m-%d %H:%M:%S"` # linux
#startdate=`date -v-0d "+%Y-%m-%d %H:%M:%S"` # mac
echo -e "\n appsflyer push 数据处理开始时间为:" $startdate

# 日期
if [ "${1}" != "" ] && [ "${2}" != "" ] && [ "${3}" != "" ]
then
        datetime=${1}
        filename=${2}
        hours=${3}
else
        #  todo 时间获取
        datetime=`date -d'0 days ago' +%Y-%m-%d` # linux
        datetimestr=`date -d'0 days ago' +%Y%m%d` # linux
        hours=`date -d'0 days ago' +%H` # linux
#        datetime=`date -v-0d "+%Y-%m-%d"` # mac
#        datetimestr=`date -v-0d "+%Y%m%d"` # mac
#        hours=`date -v-0d "+%H"` # mac

        # 判断时间 如果是0点  处理前一天23点的数据  如果不是0点 处理前一个小时的数据
        if [ $hours -eq 0 ];
        then
            datetime=`date -d'1 days ago' +%Y-%m-%d` # linux
            datetimestr=`date -d'1 days ago' +%Y%m%d` # linux
#            datetime=`date -v-1d "+%Y-%m-%d"` # mac
#            datetimestr=`date -v-1d "+%Y%m%d"` # mac

            hours=23
        else
            hours=`expr ${hours} - 1`
        fi

        # 判断 时间如果小于10点  时间点需要拼接0 主要是兼容文件名称
        if [ $hours -lt 10 ];
        then
            hours="0${hours}";
        fi

        filename=${datetimestr}${hours}

fi



echo ${datetime}
echo ${hours}
echo ${filename}

file_path="/data/af_push/"

python3 appsflyer_push_redis.py "${datetime}" "${filename}" "${hours}" >> /data/appsflyer/push_error.log 2>&1
#mvFile="${file_path}/history/${datetime}/"
#if [ ! -d "${mvFile}" ]; then
#        mkdir -p "${mvFile}"
#fi
#mv "${file}"  "${mvFile}"

# redis数据添加到pgsql
php artisan RedisAppsflyerProcesses "${datetime}" "${hours}"

# 数据匹配处理
php artisan AfPushAnalysisIdfaCommond "${datetime}" "${hours}"


#结束时间
startdate=`date -d'0 days ago' +"%Y-%m-%d %H:%M:%S"` # linux
#startdate=`date -v-0d "+%Y-%m-%d %H:%M:%S"` # mac
echo -e "\n appsflyer push 数据处理结束时间为:" $startdate
