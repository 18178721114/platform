<?php

namespace App\Console\Commands\AdDataProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessImp\PlatformImp;
use App\BusinessLogic\AdReportLogic;
use App\BusinessLogic\DataImportLogic;
use App\BusinessLogic\PlatformLogic;
use App\Common\CommonFunction;
use App\Common\CurlRequest;
use App\Common\ParseDayid;
use App\Common\Service;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\ApplicationLogic;
use App\BusinessLogic\CommonLogic;

class YumiPolymerizationCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'YumiPolymerizationCommond {dayid?} ';

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
        $dayid = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));
        //查询pgsql 的数据
        $source_id = 'pad262';
        $source_name = '玉米广告国内安卓渠道';
        try{
        $sql = "SELECT
        a.platform_app_id
        FROM
        c_app_ad_platform a,
        c_app b
        WHERE
        a.app_id = b.id
        AND b.os_id = 2
        AND a.platform_id = '$source_id'
        AND a.platform_app_id IS NOT NULL";
        $app_info_list = DB::select($sql);
        $app_info_list = Service::data($app_info_list);
        $str ='';
        foreach ($app_info_list as $key => $value) {
            $str.="'".$value['platform_app_id']."',";
        }
        $str=trim($str,',');


        //玉米数据库取回来的数据
        $sql ="SELECT
        (
        CASE
        WHEN app_uuid = '5d075c26fddc0e19d9afae150c38d752'
        AND version = '2.0.0' THEN
        'fb63efca8c295b3336ded8cd92bc490a'
        when  app_uuid ='509eb81e61ddbdcfb1a7034534e3e6bc' and version ='2.1.0.1' then '3eb242db97ef2e096232a65da4659391'
        when  app_uuid ='5a0f0fbfc3655924c22e64429fa0ebc7' and channel='zy013' then '6350c0b9ab29ef1874d5548b83c0b62d'
        else app_uuid end ) AS app_id,  
        app_name,
        date_format(day_id, '%Y%m%d') AS date,
         (CASE
        WHEN channel = '-'
        THEN 'zy000' else channel end) as
        channel,
        version,  
        country,
        a.plat_id AS ad_plat_id,
        t.ad_type,
        (
        CASE t.request_type
        WHEN 1 THEN
        'SDK'
        WHEN 2 THEN
        'API'
        ELSE
        ''
        END
        ) AS inter_type, 
        t.sdk_version,  
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
        SUBSTRING_INDEX(t.ad_plat_key, ',', 1) AS adkey1,   
        substr(
        t.ad_plat_key,
        length(
        SUBSTRING_INDEX(t.ad_plat_key, ',', 1)
        ) + 2,
        length(
        SUBSTRING_INDEX(t.ad_plat_key, ',', 2)
        ) - length(
        SUBSTRING_INDEX(t.ad_plat_key, ',', 1)
        ) - 1
        ) AS adkey2,    
        SUBSTRING_INDEX(t.ad_plat_key, ',' ,- 1) AS adkey3, 
        sum(real_income) AS earning,
        now() as create_time,
        slot_uuid,   
        slot_name
        FROM
        slot_income_sdk_stat t
        LEFT JOIN ad_plat a ON a.id = t.ad_plat_id
        WHERE
        cp_id = '1'
        AND app_uuid IN ($str)
        AND day_id ='$dayid' 
        GROUP BY
        app_uuid,
        day_id,
        cp_id,
        channel,
        version,
        country,
        a.plat_id,
        t.ad_type,
        t.request_type,
        t.sdk_version,
        ad_plat_key,
        slot_uuid,
        slot_name,
        app_name";

        $info = DB::connection('mysql_yumi')->select($sql);
        $info = Service::data($info);
//        var_dump(111,count($info));
        if(!$info){
//            $error_msg = $dayid.'号，'.$source_name.'广告平台数据处理程序获取原始数据为空';
//            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,2,$error_msg);
            exit;
        }
        if ($info){

            DB::beginTransaction();
            $sel_sql = "select count(1) as count from yumi_polymerization_data where date = '{$dayid}'";
            $sel_res = DB::select($sel_sql);
            $sel_res = Service::data($sel_res);
            if ($sel_res && $sel_res[0]['count'] > 0) {
                $del_sql = "delete from yumi_polymerization_data where date = '{$dayid}'";
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
                    $insert_sql = DB::table('yumi_polymerization_data')->insert($v);
                    if (!$insert_sql) {
                        DB::rollBack();
                    }
                }
            }
            DB::commit();
            sleep(10);
            Artisan::call('YumiPolymerizationHandleProcesses' ,['dayid'=>$dayid]);
        }
        } catch (\Exception $e) {
            $error_msg_info = $dayid.'号,'.$source_name.'广告平台程序失败，失败原因：'.$e->getMessage();
            DataImportImp::saveDataErrorLog(5,$source_id,$source_name,2,$error_msg_info);

        }

    }


}