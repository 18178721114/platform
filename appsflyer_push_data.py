#!/usr/bin/python
#encoding=utf8

#!/usr/bin/python
#encoding=utf8
import sys
import psycopg2
import time
import datetime
import json
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

    table_name = "appsflyer_push_data.erm_data"


    conn = psycopg2.connect(database="zplay_platform_data_pro", user="data_plat_user",password="yhxkDTlojnbgte-2flkyt", host="gp-2zevvlhg74709d20bo.gpdb.rds.aliyuncs.com", port="3432")
    # conn = psycopg2.connect(database="zplay_platform_data", user="data_plat_user",password="yhxkDTlojnbgte-2flkyt", host="gp-2zevvlhg74709d20bo.gpdb.rds.aliyuncs.com", port="3432")
    cursor = conn.cursor()

    try:
        # -*- coding:utf-8 -*-
        f = open(r'/data/af_push/'+dayid+'/'+filename,'r')
        # f = open(r'./'+filename,'r')
        a = list(f)
        f.close()
        print(len(a))
        year = time.strftime('%Y',time.localtime(time.mktime(time.strptime(dayid,'%Y-%m-%d'))))
        month = time.strftime('%m',time.localtime(time.mktime(time.strptime(dayid,'%Y-%m-%d'))))
        currTime = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
        print(year)

        af_push_list = trans_data_to_pair(a,100)
        print(len(af_push_list))

        # #循环写入数据到内存里面， 里面每个字段用制表符\t 隔开，每一行用换行符\n 隔开
        if af_push_list:
            for af_push_arr in af_push_list:
                print(len(af_push_arr))
                if af_push_arr:
                    s = '';
                    for af_push_data in af_push_arr:
                        value = str.strip(af_push_data)
                        s += dayid+'\t'+value+'\t'+str(year)+'\t'+month+'\t'+str(currhours)+'\t'+str(currTime)+'\n'
                    #最重要的一步，要把f 的游标移到第一位，write 方法后，游标会变成最尾，StringIO(**) 就不会
                    # #
                    cursor.copy_from(StringIO(s), table_name,columns = ('dayid','json_data','year','month','hours','create_time'))  #默认sep和null 都是none
                    print('ok')
                    conn.commit()   #要自己手动提交
    except Exception as e:
        print(str(e))
        conn.rollback()

    # 关闭数据库连接
    conn.close


