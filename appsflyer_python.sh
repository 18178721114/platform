#!/usr/bin/env bash

#开始时间
startdate=`date -d'1 days ago' +"%Y-%m-%d %H:%M:%S"` # linux
#startdate=`date -v-2d "+%Y-%m-%d %H:%M:%S"` # mac
echo -e "\n appsflyer抓取数据开始时间为:" $startdate

# todo 获取AF appid
php artisan AppsflyerAppID
package=`cat ./appsflyer_appid.txt`
echo $package

#  todo 文件目录
file_path="/data/appsflyer" # 正式
#file_path="/Users/zhenliye/Desktop/appsflury" # 测试

function download_file() {
        local url="https://hq.appsflyer.com/export/${1}/installs_report/v5?api_token=6efa7c19-07c0-4457-bfd4-67e5c19f0997&from=${2}+${3}:00&to=${2}+${3}:59"
        echo $url;
        curl -L "${url}" > "${file_path}/${1}_${2}_${3}.log"
        return ${?}
}

function download_organic_file() {
        local organic_url="https://hq.appsflyer.com/export/${1}/organic_installs_report/v5?api_token=6efa7c19-07c0-4457-bfd4-67e5c19f0997&from=${2}+${3}:00&to=${2}+${3}:59"
        echo $organic_url;
        curl -L "${organic_url}" > "${file_path}/${1}_${2}_${3}_organic.log"
        return ${?}
}


scriptdir=`dirname ${0}`
cd ${scriptdir}

if [ "${1}" != "" ]
then
        package=${1}
fi

if [ "${2}" != "" ]
then
        datetime=${2}
else
        #  todo 时间获取
        datetime=`date -d'1 days ago' +%Y-%m-%d` # linux
#        datetime=`date -v-1d "+%Y-%m-%d"` # mac
fi

for hours in `seq 0 23`
do
    if [ $hours -lt 10 ];
    then
        hours="0${hours}";
    fi
    echo ${datetime}
    echo ${hours}
    echo ${package} | awk 'BEGIN{RS=" "}{print $1}' | while read appkey
    do
            # 非自然量
            download_file "${appkey}" "${datetime}" "${hours}"
            file="${file_path}/${appkey}_${datetime}_${hours}.log"
            echo $file
    #        php artisan AppsflyerTgDetailsReportCommond "${appkey}" "${datetime}" "${appkey}_${datetime}.log" >> /data/appsflyer/error.log 2>&1
            python3 appsflyer_data.py "${appkey}" "${datetime}" "${hours}" "${appkey}_${datetime}_${hours}.log" >> /data/appsflyer/error.log 2>&1
            mvFile="${file_path}/history/${datetime}/"
            if [ ! -d "${mvFile}" ]; then
                    mkdir -p "${mvFile}"
            fi
            mv "${file}"  "${mvFile}"

            # 自然量
            download_organic_file "${appkey}" "${datetime}" "${hours}"
            organic_file="${file_path}/${appkey}_${datetime}_${hours}_organic.log"
            echo $organic_file
    #        php artisan AppsflyerTgDetailsReportCommond "${appkey}" "${datetime}" "${appkey}_${datetime}_organic.log" "organic" >> /data/appsflyer/error.log 2>&1
            python3 appsflyer_data.py "${appkey}" "${datetime}" "${hours}" "${appkey}_${datetime}_${hours}_organic.log" "organic" >> /data/appsflyer/error.log 2>&1
            mvFile="${file_path}/history/${datetime}/"
            if [ ! -d "${mvFile}" ]; then
                    mkdir -p "${mvFile}"
            fi
            mv "${organic_file}"  "${mvFile}"
    done

    # 数据匹配处理
    php artisan AfAnalysisIdfaCommond "${datetime}" "${hours}"
done

#结束时间
startdate=`date -d'1 days ago' +"%Y-%m-%d %H:%M:%S"` # linux
#startdate=`date -v-2d "+%Y-%m-%d %H:%M:%S"` # mac
echo -e "\n appsflyer抓取数据结束时间为:" $startdate
