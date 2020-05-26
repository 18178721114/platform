#!/usr/bin/env bash

# todo 获取AF appid
# php artisan AppsflyerAppID
package=`cat ./appsflyer_appid.txt`
echo $package

#  todo 文件目录
file_path="/data/appsflyer" # 正式
file_path="/Users/zhenliye/Desktop/appsflury" # 测试

function download_file() {
        local url="https://hq.appsflyer.com/export/${1}/organic_installs_report/v5?api_token=6efa7c19-07c0-4457-bfd4-67e5c19f0997&from=${2}&to=${2}"
        echo $url;
        curl -L "${url}" > "${file_path}/${1}_${2}_organic.log"
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
        datetime=`date -v-1d "+%Y-%m-%d"` # mac
fi


echo ${datetime}

echo ${package} | awk 'BEGIN{RS=" "}{print $1}' | while read appkey
do
#       download_file "${appkey}" "${datetime}"
       file="${file_path}/${appkey}_${datetime}_organic.log"
       echo $file
       php artisan AppsflyerTgDetailsReportCommond "${datetime}" "${appkey}_${datetime}_organic.log"
#       mvFile="${file_path}/history/${datetime}/"
#       if [ ! -d "${mvFile}" ]; then
#                mkdir -p "${mvFile}"
#       fi
#       mv "${file}"  "${mvFile}"
done