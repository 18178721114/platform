<?php

namespace App\Console\Commands\AdDataProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\Common\CommonFunction;
use App\Common\CurlRequest;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\DataImportLogic;
use Illuminate\Support\Facades\Artisan;
use App\BusinessImp\PlatformImp;
use App\Common\Service;

class YumiReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'YumiReportCommond {dayid?} {appid?}';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Command description';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        set_time_limit(0);
        // 入口方法
        $dayid = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));
        var_dump($dayid);

        //查询pgsql 的数据
        $source_id = 'pad262';
        $source_name = '玉米广告国内安卓渠道';
        try {
        $sql = "SELECT
        platform_app_id
        FROM
        c_app_ad_platform
        WHERE
        platform_id = '$source_id' and platform_app_id is not null and status = 1";
        $app_info_list = DB::select($sql);
        $app_info_list = Service::data($app_info_list);
        $str ='';
        foreach ($app_info_list as $key => $value) {
            $str.="'".$value['platform_app_id']."',";
        }
        $str=trim($str,',');

        //玉米数据库取回来的数据
        $sql ="SELECT
            (case when  app_uuid ='5d075c26fddc0e19d9afae150c38d752' and version ='2.0.0' then 'fb63efca8c295b3336ded8cd92bc490a'
when  app_uuid ='509eb81e61ddbdcfb1a7034534e3e6bc' and version ='2.1.0.1' then '3eb242db97ef2e096232a65da4659391'
when  app_uuid ='5a0f0fbfc3655924c22e64429fa0ebc7' and channel='zy013' then '6350c0b9ab29ef1874d5548b83c0b62d'
else app_uuid end ) AS app_id,-- 玉米id
            app_name,
            date_format(day_id, '%Y%m%d') AS date,
            channel,
            country,
            a.plat_id AS ad_provider,
            t.ad_type,
            sum(round) AS round,
            SUM(request) AS request,
            sum(succreq) AS request_s,
            sum(failreq) AS request_f,
            sum(impopport) AS imp_port,
            sum(video_start) AS imp_begin,
            (
            CASE
            WHEN (
            sum(video_end) > 0
            AND SUM(imp) = 0
            AND t.ad_type = '3'
            ) THEN
            sum(video_end)
            ELSE
            SUM(imp)
            END
            ) AS imp,
            SUM(click) AS click,
            sum(reward) AS reward,
            sum(real_income) AS earning,
            now() as create_time,
            slot_uuid,
            -- 广告位id
            slot_name
            FROM
            slot_income_sdk_stat t
            LEFT JOIN ad_plat a ON a.id = t.ad_plat_id
            WHERE
            cp_id = '1'
            AND a.plat_id = '20001'
            AND app_uuid IN ($str)
        AND day_id ='$dayid'
        GROUP BY
        app_uuid,
        day_id,
        channel,
        country,
        a.plat_id,
        t.ad_type,
        t.request_type,
        ad_plat_key,
        slot_uuid,
        slot_name,
        app_name";
        $info = DB::connection('mysql_yumi')->select($sql);
        $info = Service::data($info);
        var_dump("玉米广告statistics=0数据条数：".count($info));
        if(!$info){
//            $error_msg = $dayid.'号，'.$source_name.'广告平台数据处理程序获取原始数据为空';
//            DataImportImp::saveDataErrorLog(1,$source_id,$source_name,2,$error_msg);
            exit;
        }


        if ($info){

            DB::beginTransaction();
            $sel_sql = "select count(1) as count from yumi_report_data where date = '{$dayid}'";
            $sel_res = DB::select($sel_sql);
            $sel_res = Service::data($sel_res);
            if ($sel_res && $sel_res[0]['count'] > 0) {
                $del_sql = "delete from yumi_report_data where date = '{$dayid}'";
                $del_res = DB::delete($del_sql);
                if (!$del_res){
                    DB::rollBack();
                }
            }

            //拆分批次
            $step = array();
            $i = 0;
            foreach ($info as $kkkk => $insert_data_info) {
                if ($kkkk % 500 == 0) $i++;
                if ($insert_data_info) {
                    $step[$i][] = $insert_data_info;
                }
            }
            $is_success = [];

            if ($step) {
                foreach ($step as $k => $v) {
                    $insert_sql = DB::table('yumi_report_data')->insert($v);
                    if (!$insert_sql) {
                        DB::rollBack();
                    }
                }
            }
            DB::commit();
            sleep(10);

            Artisan::call('YumiHandleProcesses' ,['dayid'=>$dayid]);
        }
            } catch (\Exception $e) {
        $error_msg_info = $dayid.'号,'.$source_name.'广告平台程序失败，失败原因：'.$e->getMessage();
        DataImportImp::saveDataErrorLog(5,$source_id,$source_name,2,$error_msg_info);

        }
    }


}
