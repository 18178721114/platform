#!/bin/bash
#新账号取数脚本 每天4点 30分执行
searchdate=`date -d'2 days ago' +%Y-%m-%d` # linux
#searchdate=`date -v-2d "+%Y-%m-%d"` # mac

# 第一步 分渠道获取总新增 活跃 启动次数 平均启动时长
php artisan TalkdataChinaSessionCommand $searchdate

echo -e "\n抓取成功日期为:" $searchdate

# 第二步 分国家 分渠道 获取新增 活跃 启动次数
php artisan TalkdataForeignUserTjReportCommond $searchdate

echo -e "\n抓取成功日期为:" $searchdate

# 第三步 分国家 分渠道 获取启动次数
php artisan TalkdataForeignUserSessionTjCommond $searchdate

echo -e "\n抓取成功日期为:" $searchdate

# 第四步 分国家 新增活跃 启动次数 启动时长 处理过程
php artisan TdForeignUserTjHandleProcesses $searchdate

echo -e "\n处理成功日期为:" $searchdate

# 第五步 分省份 获取 新增 活跃 启动次数
php artisan TalkdataChinaUserCommand $searchdate

echo -e "\n抓取成功日期为:" $searchdate
