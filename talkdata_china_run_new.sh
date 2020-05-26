#!/bin/bash
#新账号取数脚本 每天4点 30分执行
#开始时间
startdate=`date -d'2 days ago' +%Y-%m-%d %H:%M:%S` # linux
#startdate=`date -v-2d "+%Y-%m-%d %H:%M:%S"` # mac
echo -e "\n抓取数据开始时间为:" $startdate

searchdate=`date -d'2 days ago' +%Y-%m-%d` # linux
#searchdate=`date -v-2d "+%Y-%m-%d"` # mac

# 第一步 分渠道获取总新增 活跃 启动次数 平均启动时长
php artisan TalkdataChinaSessionNewCommand $searchdate

echo -e "\n抓取成功日期为:" $searchdate

# 第二步 分国家 分渠道 获取新增 活跃 启动次数
php artisan TalkdataForeignUserNewCommond $searchdate

echo -e "\n抓取成功日期为:" $searchdate

# 第三步 分国家 分渠道 获取启动次数
php artisan TalkdataForeignSessionNewCommond $searchdate

echo -e "\n抓取成功日期为:" $searchdate

# 第四步 分国家 新增活跃 启动次数 启动时长 处理过程
php artisan TdForeignUserTjHandleProcesses $searchdate

echo -e "\n处理成功日期为:" $searchdate

# 第五步 分省份 获取 新增 活跃 启动次数 处理数据
php artisan TalkdataChinaUserNewCommand $searchdate

echo -e "\n抓取成功日期为:" $searchdate

# 第六步 处理留存
php artisan TalkdataChinaKeepUserCommond $searchdate

echo -e "\n抓取成功日期为:" $searchdate

#结束时间
startdate=`date -d'2 days ago' +%Y-%m-%d %H:%M:%S` # linux
#startdate=`date -v-2d "+%Y-%m-%d %H:%M:%S"` # mac
echo -e "\n抓取数据结束时间为:" $startdate