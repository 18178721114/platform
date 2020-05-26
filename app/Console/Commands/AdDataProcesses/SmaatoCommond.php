<?php

namespace App\Console\Commands\AdDataProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\BusinessLogic\DataImportLogic;
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

class SmaatoCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'SmaatoCommond {dayid?} {account?}';

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
        header('content-type:text/html;charset=utf-8');

        define('AD_PLATFORM', 'Smaato');
        define('SCHEMA', 'ad_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID_CONF', '10015'); // todo 这个需要根据平台信息表确定平台ID
        define('SOURCE_ID', 'pad21'); // todo 这个需要根据平台信息表确定平台ID

        $date = $this->argument('dayid') ? $this->argument('dayid') : date('Y-m-d',strtotime('-1 day'));
        $account = $this->argument('account');


        // todo  数据库配置
//        $PlatInfo = DataImportLogic::getConf(SOURCE_ID_CONF);
//        $PlatInfo = Service::data($PlatInfo);

        $sql = " SELECT  data_account as company_account,account_user_id  as client_id,account_token  as client_secret from c_platform_account_mapping WHERE platform_id ='pad21' ";
        $PlatInfo = DB::select($sql);
        $PlatInfo = Service::data($PlatInfo);

        if (!$PlatInfo){
            $message = "{$dayid}, " . AD_PLATFORM . " 广告平台取数失败,失败原因:取数配置信息为空" ;
            DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$message);

            $error_msg_arr = [];
            $error_msg_arr[] = $message;
            CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'广告平台取数error');
            exit;
        }

        $tokenInfo = self::gettoken($PlatInfo);

        if ($tokenInfo){
            if ($account){
                foreach ($tokenInfo as $iii){
                    if ($iii['account_id'] == $account) {
                        self::get_smaato_data($iii,$date);
                    }
                }

                // 调用数据处理过程
                Artisan::call('SmaatoHandleProcesses',['dayid' => $date]);
            }else{
                foreach ($tokenInfo as $iii){
                    self::get_smaato_data($iii,$date);
                }

                // 调用数据处理过程
                Artisan::call('SmaatoHandleProcesses',['dayid' => $date]);
            }
        }

    }

    private static function get_smaato_data($iii,$date){
        $account = $iii['account_id'];
        $token_type = $iii['token_type'];
        $refresh_token = $iii['refresh_token'];
        $urls = "https://api.smaato.com/v1/reporting/";
        $header = array(
            'Content-type:application/json',
            "Authorization:$token_type $refresh_token"
        );
        $stime = date('Y-m-d',time()-86400);
        $etime = $stime;
        if(!empty($dayid)){
            $stime = $dayid;
            $etime = $stime;
        }
        $stime = $etime = $date ? trim($date) : $stime;

        $post_data = '{
            "criteria": {
                "dimension": "ApplicationId",
                "fields": [
                    "name",
                    "app_url"
                ],
                "child": {
                    "dimension": "CountryCode",
                    "child":{
                            "dimension": "AdspaceId",
                            "child": null
                        }
                    }
            },
            "kpi": {
                "clicks": true,
                "impressions": true,
                "CTR": true,
                "eCPM": true,
                "grossRevenue": true,
                "incomingAdRequests": true,
                "netRevenue": true,
                "fillrate": true
            },
            "period": {
                "period_type": "fixed",
                "start_date": "'.$stime.'",
                "end_date": "'.$etime.'"
            }
        }';

        $message = "account: $account; "."url: $urls";
        self::saveLog(AD_PLATFORM, $message);

        $data = self::zcurl($urls,$post_data,$header);

        $data = json_decode($data,true);

        if(!$data || isset($data['message'])){

            $error_msg = AD_PLATFORM.'广告平台'.$account.'账号取数失败,错误信息:'. (isset($data['message']) ? $data['message'] : '未知');
            DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);

            $error_msg_arr = [];
            $error_msg_arr[] = $error_msg;
            CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'广告平台取数error');
        }else {

            var_dump(count($data));
            //$message = $stime . ", 获取到当前账号：" . $account . "的数据条数为：" . count($data);
            //self::saveLog(AD_PLATFORM, $message);

            $map = [];
            $map['dayid'] = $stime;
            $map['source_id'] = SOURCE_ID;
            $map['account'] = $account;
            $count = DataImportLogic::getChannelData('ad_data','erm_data',$map)->count();
            if($count>0){
                $bool = DataImportLogic::deleteHistoryData(SCHEMA, TABLE_NAME, $map);
            }

            //$message = $stime . ", 删除当前账号：" . $account . "的数据条数：" . $bool;
            //self::saveLog(AD_PLATFORM, $message);

            $create_time = date("Y-m-d H:i:s", time());
            $insert_datas = [];
            foreach ($data as $k => $val) {
                $arr = [];
                $arr['appid'] = $val['criteria'][0]['value'];
                $arr['appname'] = $val['criteria'][0]['meta']['name'];
                $arr['country'] = $val['criteria'][1]['value'];
                $arr['AdspaceId'] = $val['criteria'][2]['value'];
                $arr['grossRevenue'] = $val['kpi']['grossRevenue'];
                $arr['netRevenue'] = $val['kpi']['netRevenue'];
                $arr['clicks'] = $val['kpi']['clicks'];
                $arr['eCPM'] = $val['kpi']['eCPM'];
                $arr['fillrate'] = $val['kpi']['fillrate'];
                $arr['incomingAdRequests'] = $val['kpi']['incomingAdRequests'];
                $arr['CTR'] = $val['kpi']['CTR'];
                $arr['impressions'] = $val['kpi']['impressions'];
                $arr['account'] = $account;

                $insert_datas[] = $arr;
            }


            $insert_data = [];
            if ($insert_datas) {
                foreach ($insert_datas as $i_k => $i_value) {

                    $insert_data[$i_k]['type'] = 2;
                    $insert_data[$i_k]['app_id'] = $arr['appid'];
                    $insert_data[$i_k]['app_name'] = $arr['appname'];
                    $insert_data[$i_k]['account'] = $account;
                    $insert_data[$i_k]['source_id'] = SOURCE_ID;
                    $insert_data[$i_k]['json_data'] = json_encode($i_value);
                    $insert_data[$i_k]['dayid'] = $date;
                    $insert_data[$i_k]['create_time'] = $create_time;
                    $insert_data[$i_k]['ad_id'] =$i_value['AdspaceId'];
                    $insert_data[$i_k]['app_id'] =$i_value['appid'];
                    $insert_data[$i_k]['app_name'] =$i_value['appname'];
                    $insert_data[$i_k]['income'] =$i_value['grossRevenue'];
                    $insert_data[$i_k]['year'] = date("Y", strtotime($date));
                    $insert_data[$i_k]['month'] = date("m", strtotime($date));
                }
            }

            if ($insert_data) {

                //拆分批次
                $step = array();
                $i = 0;
                foreach ($insert_data as $kkkk => $insert_data_info) {
                    if ($kkkk % 1000 == 0) $i++;
                    if ($insert_data_info) {
                        $step[$i][] = $insert_data_info;
                    }
                }

                $is_success = [];
                if ($step) {
                    foreach ($step as $k => $v) {
                        $result = DataImportLogic::insertChannelData(SCHEMA, TABLE_NAME, $v);
                        if (!$result) {
                            $is_success[] = $k;
                        }
                    }
                }

                if (count($is_success)) {

                    $error_msg = AD_PLATFORM.'广告平台'.$account.'账号数据入库失败';
                    DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);

                    //$message = "{$date}, Smaato接口获取数据插入失败" . date('Y-m-d H:i:s');
                    //self::saveLog(AD_PLATFORM, $message);
                } 
            }
        }



    }
    /**
     * 根据请求url，得到响应
     * @param $url
     * @return bool|string
     */
    private static function getContent2($urls,$post_data,$header)
    {
        static $degree = 0;

        if (!$content = self::zcurl($urls,$post_data,$header)) {
            if ($degree > 1) {
                return false;
            }
            $degree++;
            sleep(2);
            return self::zcurl($urls,$post_data,$header);
        }

        $data = json_decode($content, true);

        if (!$data) {
            return "JSON解析错误";
        }

        return $data;
    }

    // 获取token
    private static function gettoken($smaato_info){
        foreach ($smaato_info as $smaato){
//            $client_id_info = json_decode($smaato['param_key'],true);
            $client_id = $smaato['client_id'];
//            $client_secret_info = json_decode($smaato['key_list'],true);
            $client_secret = $smaato['client_secret'];
            $token_url="https://auth.smaato.com/v2/auth/token/?grant_type=client_credentials";
            $post_data = "client_id={$client_id}&client_secret={$client_secret}";
            $token = self::zcurl($token_url,$post_data);
            $token = json_decode($token,true);

            if (isset($token['error'])){
                $error_msg = AD_PLATFORM.'平台'.$smaato['company_account'].'账号取数失败,错误信息:'. ($token['error'] ? $token['error'] : '未知');
                DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);

            }

            $token_type = $token['token_type'];
            $refresh_token = $token['access_token'];
            $tokenInfo[] = array('token_type'=>$token_type,'refresh_token'=>$refresh_token,'account_id'=>$smaato['company_account']);
        }
        return $tokenInfo;
    }

    private static function zcurl($url,$post_data,$header=''){
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, "{$url}");
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_ENCODING, '');
        curl_setopt($ch, CURLOPT_MAXREDIRS, 10);
        curl_setopt($ch, CURLOPT_TIMEOUT, 120);
        curl_setopt($ch, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_1_1);
        curl_setopt($ch,CURLOPT_CUSTOMREQUEST,"POST");
        curl_setopt($ch, CURLOPT_HEADER,  0);
        curl_setopt($ch, CURLOPT_POSTFIELDS, $post_data);
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST,2);
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER,false);
        if($header)
            curl_setopt($ch, CURLOPT_HTTPHEADER,  $header);
        $output = curl_exec($ch);
        curl_close($ch);
        return $output;
    }
    // 保存日志
    private static function saveLog($platform_name = '未知', $message = ''){

        $fileName = date('Y-m-d',time());
        $dir = './storage/adDataLogs';

        if (!is_dir($dir)) {
            mkdir($dir,0777,true);
        }
        $logFilename = $dir.'/'.$platform_name.'_ad'.'.log';
        //生成日志
        file_put_contents( $logFilename,$message . "\n\n",FILE_APPEND);
    }
}