#!/usr/bin/python
#encoding=utf8

#!/usr/bin/python
#encoding=utf8
import sys
import psycopg2
import time
import datetime
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO




if __name__ == "__main__":

    if len(sys.argv) == 1:
        # 开始时间
        dayid = time.strftime("%Y-%m-%d", time.localtime(time.time()-86400))
        print("参数有误")
        exit()
    elif len(sys.argv) < 5:
        print("参数有误")
        exit()
    else:
        appkey = sys.argv[1]
        dayid = sys.argv[2]
        currhours = sys.argv[3]
        filename = sys.argv[4]

    organic = ''
    if len(sys.argv) == 6:
        organic = sys.argv[5]

    print(appkey,dayid,currhours,filename,organic)

    table_name = "appsflyer_pull_data.erm_data"
    # table_name = "appsflyer_pull_data.erm_data"


    conn = psycopg2.connect(database="zplay_platform_data_pro", user="data_plat_user",password="yhxkDTlojnbgte-2flkyt", host="gp-2zevvlhg74709d20bo.gpdb.rds.aliyuncs.com", port="3432")
    # conn = psycopg2.connect(database="zplay_platform_data", user="data_plat_user",password="yhxkDTlojnbgte-2flkyt", host="gp-2zevvlhg74709d20bo.gpdb.rds.aliyuncs.com", port="3432")
    cursor = conn.cursor()

    if organic:
        delete_sql = "delete from "+table_name+" where dayid='"+dayid+"' and appid='"+appkey+"' and hours='"+currhours+"' and mediasource = 'organic'"
    else:
        delete_sql = "delete from "+table_name+" where dayid='"+dayid+"' and appid='"+appkey+"' and hours='"+currhours+"' and mediasource != 'organic'"

    cursor.execute(delete_sql)

    try:
        # -*- coding:utf-8 -*-
        f = open(r'/data/appsflyer/'+filename,'r')
        # f = open(r'/Users/zhenliye/Desktop/appsflury/'+filename,'r')
        a = list(f)
        f.close()
        a.pop(0)

        year = time.strftime('%Y',time.localtime(time.mktime(time.strptime(dayid,'%Y-%m-%d'))))
        month = time.strftime('%m',time.localtime(time.mktime(time.strptime(dayid,'%Y-%m-%d'))))
        # #循环写入数据到内存里面， 里面每个字段用制表符\t 隔开，每一行用换行符\n 隔开
        s = '';
        for value in a:
            value_len = value.split(',')
            if len(value_len) == 81:
                value = value.replace('"','')
                value = value.replace(',','\t')
                s += dayid+'\t'+year+'\t'+month+'\t'+currhours+'\t'+value
        #最重要的一步，要把f 的游标移到第一位，write 方法后，游标会变成最尾，StringIO(**) 就不会
        cursor.copy_from(StringIO(s), table_name,columns = ('dayid','year','month','hours','attributedtouchtype','attributedtouchtime','installtime','eventtime','eventname','eventvalue','eventrevenue','eventrevenuecurrency','eventrevenueusd','eventsource','isreceiptvalidated','partner','mediasource','channel','keywords','campaign','campaignid','adset','adsetid','ad','adid','adtype','siteid','subsiteid','subparam1','subparam2','subparam3','subparam4','subparam5','costmodel','costvalue','costcurrency','contributor1partner','contributor1mediasource','contributor1campaign','contributor1touchtype','contributor1touchtime','contributor2partner','contributor2mediasource','contributor2campaign','contributor2touchtype','contributor2touchtime','contributor3partner','contributor3mediasource','contributor3campaign','contributor3touchtype','contributor3touchtime','region','countrycode','state','city','postalcode','dma','ip','wifi','operator','carrier','language','appsflyerid','advertisingid','idfa','androidid','customeruserid','imei','idfv','platform','devicetype','osversion','appversion','sdkversion','appid','appname','bundleid','isretargeting','retargetingconversiontype','attributionlookback','reengagementwindow','isprimaryattribution','useragent','httpreferrer','originalurl'))  #默认sep和null 都是none
        # #

        if organic:
            update_sql = "update "+table_name+" set mediasource = 'organic' where dayid='"+dayid+"' and appid='"+appkey+"' and hours='"+currhours+"' and mediasource = ''"
            cursor.execute(update_sql)
        print('ok')
        conn.commit()   #要自己手动提交
    except Exception as e:
        print(str(e))
        conn.rollback()

    # 关闭数据库连接
    conn.close









