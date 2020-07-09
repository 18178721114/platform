<?php

namespace App\Console\Commands\AdDataProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\BusinessLogic\DataImportLogic;
use App\Common\ApiResponseFactory;
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
use Illuminate\Support\Facades\Redis;

class FacebookAdBiddingCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'FacebookAdBiddingCommond {dayid?} {app_id?}';

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
        //账号pub-9383649441632391取数
        header('Content-Type: text/html; charset=utf-8');

        //得到 web 服务器和 PHP 之间的接口类型
        $sapi_type = php_sapi_name();

        define('AD_PLATFORM', 'FacebookBidding');
        define('SCHEMA', 'ad_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID_CONF', '10007'); // todo 这个需要根据平台信息表确定平台ID
        define('SOURCE_ID', 'pad23'); // todo 这个需要根据平台信息表确定平台ID

        $dayid = $dayid = $this->argument('dayid') ? $this->argument('dayid') : date('Y-m-d',strtotime('-1 day'));
        $fb_app_id = $this->argument('app_id') ? $this->argument('app_id') : '';
        $date = ParseDayid::get_dayid($dayid);
        try{

        if (empty($date)) {
            $msg = "invalid date.";
            $message = "{$date}, Facebook广告平台分bidding取数失败,失败原因:" . $msg;
            DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$message);
            exit;
        }

        $data_start = time();
        var_dump('Facebook广告平台分bidding取数开始：'.$data_start);

        // todo  数据库配置
//        $PlatInfo = DataImportLogic::getConf(SOURCE_ID_CONF);
//        $PlatInfo = Service::data($PlatInfo);
        $where = '';
        if ($fb_app_id){
            $where .= "  and (b.platform_app_id = '{$fb_app_id}' or b.publisher_id = '{$fb_app_id}') ";
        }
        $sql = "SELECT distinct a.platform_id,
        a.data_account AS company_account,
        b.platform_app_id AS application_id,
        b.publisher_id as publisher_id,
        a.account_api_key AS appkey  from c_app_ad_platform b LEFT JOIN  c_platform_account_mapping a  ON b.platform_id = a.platform_id
        WHERE
        a.platform_id = 'pad23' and a.account_api_key != '' and b.publisher_id != '' and b.redundancy_status = 1 and b.status =1 $where ;";

        $PlatInfo = DB::select($sql);
        $PlatInfo = Service::data($PlatInfo);

        if (!$PlatInfo){
            $message = "{$date}, Facebook广告平台分bidding取数失败,失败原因:取数配置信息为空" ;
            DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$message);
            $error_msg_arr = [];
            $error_msg_arr[] = $message;
            CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'取数error');
            exit;
        }

        $j =0 ;

        $error_application_ids = [];

        foreach ($PlatInfo as $rss){
            $account = $rss['company_account'];
            if (trim($rss['publisher_id']) != -1){
                $facebook[$j]['appid'] = trim($rss['publisher_id']);
            }else{
                $facebook[$j]['appid'] = trim($rss['application_id']);
            }
            $facebook[$j]['appkey'] = trim($rss['appkey']);

            if($facebook[$j]['appkey']){
                $appinfo = json_encode($facebook);
                $url = "http://47.88.186.190/reportingApi/api/facebook_bidding_api.php?stime=".urlencode($date)."&appinfo=".urlencode($appinfo);
                $result = CurlRequest::get_response($url);
                $re = json_decode($result,true);

//                // 数据获取重试
//                $api_data_i=1;
//                while(!$re){
//                    $result = CurlRequest::get_response($url);
//                    $re = json_decode($result,true);
//                    $api_data_i++;
//                    if($api_data_i>3)
//                        break;
//                }
//
//                //取数四次 取数结果仍为空
//                if($api_data_i ==4 && empty($re)){
//                    $error_msg_1 = AD_PLATFORM.'广告平台'.$account.'账号下应用或资产id'.$facebook[$j]['appid'].'取数失败,错误信息:返回数据为空('.json_encode($result).')';
//                    DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg_1);
//                    continue;
//
//                }

                // pgsql 逻辑
                if(!isset($re['error'])){
                    if ($re) {
                        $message = $date . ", 获取到当前账号：" . $account . "的数据条数为：" . count($re);
                        self::saveLog(AD_PLATFORM, $message);

                        $map = [];
                        $map['type'] = 3;
                        $map['dayid'] = $date;
                        $map['source_id'] = SOURCE_ID;
                        $map['account'] = $account;
                        $map['app_id'] = $facebook[$j]['appid'];

                        $count = DataImportLogic::getChannelData(SCHEMA, TABLE_NAME, $map)->count();
                        $bool = 0;
                        if ($count > 0) {
                            $bool = DataImportLogic::deleteHistoryData(SCHEMA, TABLE_NAME, $map);
                        }


                        $message = $date . ", 删除当前账号：" . $account . "的数据条数：" . $bool;
                        self::saveLog(AD_PLATFORM, $message);

                        $create_time = date("Y-m-d H:i:s", time());
                        $insert_data = [];
                        foreach ($re as $k => $v) {
                            $v['account'] = $account;
                            $insert_data[$k]['type'] = 3;
                            $insert_data[$k]['app_id'] = $v['appid'];
                            $insert_data[$k]['account'] = $account;
                            $insert_data[$k]['source_id'] = SOURCE_ID;
                            $insert_data[$k]['json_data'] = json_encode($v);
                            $insert_data[$k]['dayid'] = $date;
                            $insert_data[$k]['create_time'] = $create_time;
                            $insert_data[$k]['year'] = date("Y", strtotime($date));
                            $insert_data[$k]['month'] = date("m", strtotime($date));
                            $insert_data[$k]['ad_id'] = '';
                            $insert_data[$k]['ad_name'] = '';
                            $insert_data[$k]['income'] = $v['income'];

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
                                $message = "{$date}, Facebook广告平台分bidding，账号{$account}数据插入失败" . date('Y-m-d H:i:s');
                                DataImportImp::saveDataErrorLog(1, SOURCE_ID, AD_PLATFORM, 2, $message);
                            }
                        }
                    }

                }else{
                    $message = "{$date}, Facebook广告平台分bidding应用" ;
                    $message .= trim($facebook[$j]['appid'])."取数失败,失败原因:".json_encode($result);
                    DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$message);
                }
                sleep(6);
            }
        }

//        if ($error_application_ids){
//            $error_application_str = implode(',',$error_application_ids);
//            $message = "{$date}, Facebook广告平台分bidding应用".$error_application_str ;
//            DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$message);
//        }

        $data_end = time();
        var_dump('Facebook广告平台分bidding取数结束：'.$data_end);
        var_dump('Facebook广告平台分bidding取数用时：'.($data_end - $data_start));

        // 调用数据处理过程
        Artisan::call('FacebookBiddingHandleProcesses',['dayid' => $date]);
        } catch (\Exception $e) {
            $error_msg_info = $dayid.'号,'.AD_PLATFORM.'广告平台程序失败，失败原因：'.$e->getMessage();
            DataImportImp::saveDataErrorLog(5,SOURCE_ID,AD_PLATFORM,2,$error_msg_info);

        }
    }

    /**
     * 根据请求url，得到响应
     * @param $url
     * @return bool|string
     */
    private static function getContent2($url)
    {
        static $degree = 0;

        if (!$content = CurlRequest::get_response($url)) {
            if ($degree > 1) {
                return false;
            }
            $degree++;
            sleep(2);
            return self::getContent2($url);
        }

        $data = json_decode($content, true);

        if (!$data) {
            return "JSON解析错误";
        } else if ($data['success'] == false) {
            return $data['message'];
        }

        return $data['data'];
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