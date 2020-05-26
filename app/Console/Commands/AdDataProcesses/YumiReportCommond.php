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

        $sql = "SELECT
        platform_app_id
        FROM
        c_app_ad_platform
        WHERE
        platform_id = '$source_id' and platform_app_id is not null";
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

//        define('AD_PLATFORM', '玉米广告');
//        define('SCHEMA', 'ad_data');
//        define('TABLE_NAME', 'erm_data');
//        define('SOURCE_ID', 'pad262'); // todo 这个需要根据平台信息表确定平台ID
//
//        $developer_id = 'yumi7onrmzfnyull29n2'; // 开发者ID
//        $token = 'inayuwc3qdegmvphwi93eu132ibx27jt'; // 秘钥
//        // 生成签名
//        $timestamp = time();
//        $nonce = rand(1,999);
//        $tmpArr = array($token, $timestamp, $nonce);
//        sort($tmpArr, SORT_STRING);
//        $signature = implode( $tmpArr );
//        $signature = sha1( $signature );
//
//        $params = array(
//            'signature' => $signature,
//            'timestamp' => $timestamp,
//            'nonce' => $nonce,
//        );
//
//        $url = "https://im.yumimobi.com/report_api/developer/{$developer_id}/app";
//        $url .= '?' . http_build_query($params, null, '&');
//        $appInfoList = self::get_response($url);
//        $appInfoList = json_decode($appInfoList,true);
////        var_dump($appInfoList);
//        //获取应用信息
//        if(!empty($appInfoList['data'])){
//            foreach ($appInfoList['data'] as $appInfo){
//                $os = isset($appInfo['os']) ? $appInfo['os'] : ''; // 系统：1=Android，2=iOS
//                $app_name = isset($appInfo['app_name']) ? $appInfo['app_name'] : ''; // 应用名称
//                $app_uuid = isset($appInfo['app_uuid']) ? $appInfo['app_uuid'] : ''; // 应用id
//                $dataUrl = "https://im.yumimobi.com/report_api/developer/{$developer_id}/app/{$app_uuid}/stat";
//                $params['start_date'] = $dayid;
//                $params['end_date'] = $dayid;
//
//                $dataUrl .= '?' . http_build_query($params, null, '&');
//                $dataInfo = self::get_response($dataUrl);
//                $dataInfo = json_decode($dataInfo,true);
//                if(!empty($dataInfo['data'])){
//
//                    $map['dayid'] = $dayid;
//                    $map['source_id'] = SOURCE_ID;
//                    $map['account'] = 'zplay';
//                    $map['app_id'] = $app_uuid;
//                    $count = DataImportLogic::getChannelData('ad_data','erm_data',$map)->count();
//                    if($count>0){
//
//                    //删除数据
//                        DataImportLogic::deleteHistoryData(SCHEMA,TABLE_NAME,$map);
//                    }
//                    $index = 0;
//                    $insert_data =[];
//                    $step =[];
//                    foreach ($dataInfo['data'] as $data){
//
//                        if ($data['ad_plat_name'] != 'yumi') continue;
//                        $data['app_name'] = $app_name;
//                        $data['app_id'] = $app_uuid;
//                        $data['developer_id'] = $developer_id;
//                        $data['app_uuid'] = $app_uuid;
//                        $data['country'] = isset($data['country_code']) ? $data['country_code'] : '';
//                        $data['os'] = $os;
//
//                        $insert_data[$index]['account'] = 'zplay';
//                        $insert_data[$index]['app_id'] = $app_uuid;
//                        $insert_data[$index]['type'] = 2;
//                        $insert_data[$index]['source_id'] = SOURCE_ID;
//                        $insert_data[$index]['dayid'] = $dayid;
//                        $insert_data[$index]['json_data'] =str_replace('\'','\'\'',json_encode($data));
//                        $insert_data[$index]['create_time'] = date("Y-m-d H:i:s");
//                        $insert_data[$index]['ad_id'] =$data['slot_uuid'];
//                        $insert_data[$index]['app_id'] =$data['app_id'];
//                        $insert_data[$index]['app_name'] =$data['app_name'];
//                        $insert_data[$index]['income'] =$data['income'];
//                        $insert_data[$index]['year'] = date("Y",strtotime($dayid));
//                        $insert_data[$index]['month'] = date("m",strtotime($dayid));
//
//                        $index++;
//
//                    }
//                    $i = 0;
//                    foreach ($insert_data as $kkkk => $insert_data_info) {
//                        if ($kkkk % 2000 == 0) $i++;
//                        if ($insert_data_info) {
//                            $step[$i][] = $insert_data_info;
//                        }
//                    }
//                    if ($step) {
//                        foreach ($step as $k => $v) {
//                            $result = DataImportLogic::insertChannelData(SCHEMA,TABLE_NAME,$v);
//                            if (!$result) {
//                                echo 'mysql_error'. PHP_EOL;
//                            }
//                        }
//
//                    }
//                }else{
//
//                    if(count($dataInfo['data']) ==0){
//
////                        $error_msg = AD_PLATFORM.'广告平台'.$developer_id.'账号下应用'.$app_uuid.'数据为空';
////                        DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);
//                    }else{
//                        $error_msg = AD_PLATFORM.'广告平台'.$developer_id.'账号下应用'.$app_uuid.'取数失败,错误信息:'.$dataInfo['error'];
//                        DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);
//
//                        $error_msg_arr = [];
//                        $error_msg_arr[] = $error_msg;
//                        CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'广告平台取数error');
//                    }
//
//                }
//            }
//
//        }else {
//            $error_msg = AD_PLATFORM.'广告平台'.$developer_id.'账号取数失败,错误信息:'.$appInfoList['error'];
//            DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);
//        }
    }

//    public static function get_response($url, $headers='')
//    {
//        $ch = curl_init();
//        curl_setopt($ch, CURLOPT_URL, $url);
//
//        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
//        curl_setopt($ch, CURLOPT_HEADER, 0);
//        curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
//        curl_setopt($ch, CURLOPT_TIMEOUT,120);
//        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, FALSE);
//        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, FALSE);
//
//        if (!empty($headers)) {
//            curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
//        }
//
//        $output = curl_exec($ch);
//        curl_close($ch);
//        return $output;
//    }
}
