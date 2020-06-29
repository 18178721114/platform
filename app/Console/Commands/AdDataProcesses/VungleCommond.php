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

class VungleCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'VungleCommond {dayid?} {account?}';

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

        define('AD_PLATFORM', 'Vungle');
        define('SCHEMA', 'ad_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'pad09'); // todo 这个需要根据平台信息表确定平台ID
        try{
        // todo 以后从数据库获取  目前写死
//        $plat_list = [
//            [
//                'platform_id' => '10021',
//                'company_account' => 'weibo@zplay.cn',
//                'param_key' => '{"key":"59b832d628fca789fd7aa73c561b6a96"}'
//            ],
//            [
//                'platform_id' => '10021',
//                'company_account' => 'global@yumimobi.com',
//                'param_key' => '{"key":"c3f07564d9634fd7224b79f0380e0d9c"}'
//            ],
//            [
//                'platform_id' => '10021',
//                'company_account' => 'noodlecake',
//                'param_key' => '{"key":"0fb3a0bdfe30832b60965f9c5fd54077"}'
//            ],
//        ];


        $date = $this->argument('dayid');
        $account = $this->argument('account');

        $sql = " SELECT  data_account as company_account,account_api_key  as api_key from c_platform_account_mapping WHERE platform_id ='pad09'  and status = 1 order by data_account desc ";
        $plat_list = DB::select($sql);
        $plat_list = Service::data($plat_list);
        if (!$plat_list) return;

        $account_key = [];
        foreach ($plat_list as $plat_key=> $plat_value){
            $account_id = $plat_value['company_account'];
//            $param_key = json_decode($plat_value['param_key'],true);
            $api_key = $plat_value['api_key'];
            $account_key[$account_id] = $api_key;
        }


        if ($account){

            foreach ($account_key as $account_k => $account_v) {
                if ($account_k == $account){
                    $account = $account_k;
                    $api_key = $account_v;

                    $headers = [
                        "Authorization:Bearer $api_key",
                        "Vungle-Version:1",
                        "Accept:application/json"
                    ];

                    $url = "https://report.api.vungle.com/ext/pub/reports/performance?dimensions=date,application,country,platform,placement&aggregates=views,completes,clicks,revenue,ecpm&start={$date}&end={$date}";

//                    $message = "account: $account; " . "url: $url";
//                    self::saveLog(AD_PLATFORM, $message);

                    $data = self::getContent2($url, $headers,$account);

                    if (!$data) {

                        $error_msg = AD_PLATFORM.'广告平台'.$account.'账号取数失败,错误信息:'.json_encode($data);
                        DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);

                        $error_msg_arr = [];
                        $error_msg_arr[] = $error_msg;
                        CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'广告平台取数error');

                    }else {

                        $message = $date . ", 获取到当前账号：" . $account . "的数据条数为：" . count($data);
                        
                        // self::saveLog(AD_PLATFORM, $message);

                        $map = [];
                        $map['dayid'] = $date;
                        $map['source_id'] = SOURCE_ID;
                        $map['account'] = $account;
                        $count = DataImportLogic::getChannelData('ad_data','erm_data',$map)->count();
                        if($count>0){
                            $bool = DataImportLogic::deleteHistoryData(SCHEMA, TABLE_NAME, $map);
                        }
                        // $message = $date . ", 删除当前账号：" . $account . "的数据条数：" . $bool;
                        // self::saveLog(AD_PLATFORM, $message);

                        $create_time = date("Y-m-d H:i:s", time());
                        $insert_data = [];
                        //var_dump($data);die;
                        foreach ($data as $k => $v) {
                            $v['account'] = $account;
                            $insert_data[$k]['type'] = 2;
                            $insert_data[$k]['ad_id'] =$v['placement reference id'];
                            $insert_data[$k]['ad_name'] =$v['placement name'];
                            $insert_data[$k]['app_id'] =$v['application id'];
                            $insert_data[$k]['app_name'] =$v['application name'];
                            $insert_data[$k]['income'] =$v['revenue'];
                            $insert_data[$k]['account'] = $account;
                            $insert_data[$k]['source_id'] = SOURCE_ID;
                            $insert_data[$k]['json_data'] = json_encode($v);
                            $insert_data[$k]['dayid'] = $date;
                            $insert_data[$k]['create_time'] = $create_time;
                            $insert_data[$k]['year'] = date("Y", strtotime($date));
                            $insert_data[$k]['month'] = date("m", strtotime($date));

                        }

                        if ($insert_data) {
                            $is_success = [];
                            foreach ($insert_data as $kk => $v) {
                                $result = DataImportLogic::insertChannelData(SCHEMA, TABLE_NAME, $v);
                                if (!$result) {
                                    $is_success[] = $kk;
                                }
                            }

                            $message = $date . ", 当前账号：" . $account . "的数据入库条数为：" . count($insert_data);
                            self::saveLog(AD_PLATFORM, $message);

                            if (count($is_success)) {
                                $message = "{$date}, Vungle接口获取结束, 数据插入失败" . date('Y-m-d H:i:s');
                                self::saveLog(AD_PLATFORM, $message);
                            } else {
                                $message = "{$date}, Vungle接口获取完成。" . date('Y-m-d H:i:s');
                                self::saveLog(AD_PLATFORM, $message);
                            }
                        }
                    }
                }
            }



        }else{
            if ($account_key){
                foreach ($account_key as $account_k => $account_v) {

                    $account = $account_k;
                    $api_key = $account_v;

                    $headers = [
                        "Authorization:Bearer $api_key",
                        "Vungle-Version:1",
                        "Accept:application/json"
                    ];

                    $url = "https://report.api.vungle.com/ext/pub/reports/performance?dimensions=date,application,country,platform,placement&aggregates=views,completes,clicks,revenue,ecpm&start={$date}&end={$date}";

                    $message = "account: $account; " . "url: $url";
                    self::saveLog(AD_PLATFORM, $message);

                    $data = self::getContent2($url, $headers,$account);

                    if (!$data) {
                        $error_msg = AD_PLATFORM.'平台'.$account.'账号取数失败,错误信息:'.json_encode($data);
                        DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);

                        $error_msg_arr = [];
                        $error_msg_arr[] = $error_msg;
                        CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'取数error');
                    }else {

                        $message = $date . ", 获取到当前账号：" . $account . "的数据条数为：" . count($data);
                        //self::saveLog(AD_PLATFORM, $message);
                        //echo $message ;die;
                        $map = [];
                        $map['dayid'] = $date;
                        $map['source_id'] = SOURCE_ID;
                        $map['account'] = $account;
                        $count = DataImportLogic::getChannelData('ad_data','erm_data',$map)->count();
                        if($count>0){
                            $bool = DataImportLogic::deleteHistoryData(SCHEMA, TABLE_NAME, $map);
                        }

                       // $message = $date . ", 删除当前账号：" . $account . "的数据条数：" . $bool;
                        //self::saveLog(AD_PLATFORM, $message);


                        $create_time = date("Y-m-d H:i:s", time());
                        $insert_data = [];
                        foreach ($data as $k => $v) {
                            $v['account'] = $account;
                            $insert_data[$k]['type'] = 2;
                            $insert_data[$k]['ad_id'] =$v['placement reference id'];
                            $insert_data[$k]['ad_name'] =$v['placement name'];
                            $insert_data[$k]['app_id'] =$v['application id'];
                            $insert_data[$k]['app_name'] =$v['application name'];
                            $insert_data[$k]['income'] =$v['revenue'];
                            $insert_data[$k]['account'] = $account;
                            $insert_data[$k]['source_id'] = SOURCE_ID;
                            $insert_data[$k]['json_data'] = json_encode($v);
                            $insert_data[$k]['dayid'] = $date;
                            $insert_data[$k]['create_time'] = $create_time;
                            $insert_data[$k]['year'] = date("Y", strtotime($date));
                            $insert_data[$k]['month'] = date("m", strtotime($date));

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
                                $message = "{$date}, Vungle接口获取数据插入失败" . date('Y-m-d H:i:s');
                                self::saveLog(AD_PLATFORM, $message);
                            } else {
                                $message = "{$date}, Vungle接口，账号{$account}获取数据完成。" . date('Y-m-d H:i:s');
                                self::saveLog(AD_PLATFORM, $message);
                            }
                        }
                    }
                }
            }
        }
        Artisan::call('VungleHandleProcesses' ,['dayid'=>$date]);
        } catch (\Exception $e) {
            $error_msg_info = $date.'号,'.AD_PLATFORM.'广告平台程序失败，失败原因：'.$e->getMessage();
            DataImportImp::saveDataErrorLog(5,SOURCE_ID,AD_PLATFORM,2,$error_msg_info);

        }

    }

    /**
     * 根据请求url，得到响应
     * @param $url
     * @return bool|string
     */
    private static function getContent2($url,$header,$company_account)
    {
        static $degree = 0;

        if (!$content = CurlRequest::get_response_header($url,$header)) {
            if ($degree > 1) {
                return false;
            }
            $degree++;
            sleep(2);
            return self::getContent2($url,$header,$company_account);
        }

        self::saveLog(AD_PLATFORM, $content);

        $data = json_decode($content, true);

        if (!$data || isset($data['error'])) {

            $status = isset($data['status']) ? $data['status'] : '';
            $error = isset($data['error']) ? $data['error'] : '未知错误';
            $error_msg = 'Vungle平台'.$company_account.'账号取数失败,错误信息:'.$status.','.$error;

            DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);

            return false;
        }

        return $data;
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