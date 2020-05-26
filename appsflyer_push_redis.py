#!/usr/bin/python
#encoding=utf8

#!/usr/bin/python
#encoding=utf8
import sys
import psycopg2
import time
import datetime
import json
from redis import Redis
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO



def trans_data_to_pair(data,index):
    contents=[
      data[i:i+index]
      for i in range(0,len(data),index)
    ]
    return contents

if __name__ == "__main__":

    if len(sys.argv) == 1:
        # 开始时间
        dayid = time.strftime("%Y-%m-%d", time.localtime(time.time()-86400))
        print("参数有误")
        exit()
    elif len(sys.argv) != 4:
        print("参数有误")
        exit()
    else:
        dayid = sys.argv[1]
        filename = sys.argv[2]
        currhours = sys.argv[3]

    print(dayid,filename,currhours)

    re = Redis(host='r-2zeq68hoz05ij0h7xrpd.redis.rds.aliyuncs.com', port=6379,db=1, password='6be7e20$acA50D5bc@f2aeff56%P56')
    for min_time in range(0,60):
        if min_time < 10:
            min_time = '0'+ str(min_time)
        try:
            # -*- coding:utf-8 -*-
            realfilename = str(filename)+str(min_time)+'.log'
            f = open(r'/data/af_push/'+dayid+'/'+filename+'/'+realfilename,'r')
            # f = open(r'./'+filename,'r')
            a = list(f)
            f.close()
            print(len(a))
            year = time.strftime('%Y',time.localtime(time.mktime(time.strptime(dayid,'%Y-%m-%d'))))
            month = time.strftime('%m',time.localtime(time.mktime(time.strptime(dayid,'%Y-%m-%d'))))
            currTime = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
            print(year)

            af_push_list = trans_data_to_pair(a,2000)
            print(len(af_push_list))

            # #循环写入数据到内存里面， 里面每个字段用制表符\t 隔开，每一行用换行符\n 隔开
            if af_push_list:
                for af_push_arr in af_push_list:
                    print(len(af_push_arr))
                    af_push_arr_json={}
                    if af_push_arr:
                        af_push_arr_json = json.dumps(af_push_arr, ensure_ascii=False)
                        re.rpush('appsfyer_push_data',af_push_arr_json)
        except Exception as e:
            print(str(e))


