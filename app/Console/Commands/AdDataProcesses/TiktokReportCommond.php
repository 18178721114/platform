<?php

namespace App\Console\Commands\AdDataProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\Common\CommonFunction;
use App\Common\CurlRequest;
use App\Common\Service;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\DataImportLogic;
use Illuminate\Support\Facades\Artisan;

class TiktokReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'TiktokReportCommond {dayid?}';

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

        define('AD_PLATFORM', 'Tiktok');
        define('SCHEMA', 'ad_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'pad271'); // todo 这个需要根据平台信息表确定平台ID
        try{
        $start = $dayid;
        $end = $start;
        $time = time();

        // todo 正式

        $sql = " select distinct platform_id,data_account as company_account,account_user_id as user_id,account_token as secret_key from c_platform_account_mapping where platform_id = 'pad271' and account_user_id <> '' and account_token <> '' ";
        $PlatInfo = DB::select($sql);
        $PlatInfo = Service::data($PlatInfo);

        if ($PlatInfo){
            foreach ($PlatInfo as $singleInfo){

                $company_account = $singleInfo['company_account'];
                $user_id = $singleInfo['user_id'];

                list($t1, $t2) = explode(' ', microtime());
                $time1 = floatval($t2);
                $time2 = round(floatval($t1)*100000);
                $time3 = uniqid();
                $nonce = md5($time1.$time2.$time3);

                $timestamp = time();
                $secure_key = $singleInfo['secret_key'];
                $keys = array($secure_key, $timestamp, $nonce);
                sort($keys,2);
                $sign = implode('',$keys);
                $sign = sha1($sign);

                $url = "https://partner.oceanengine.com/union/media/open/api/report/slot?user_id=".$user_id."&nonce=".$nonce."&timestamp=".$timestamp."&sign=".$sign."&start_date=".$start."&end_date=".$end;
//                var_dump($url);
                $result = CurlRequest::get_response($url);
                $result_arr = json_decode($result,true);

                // 数据获取重试
                $api_data_i=1;
                while(!$result_arr){
                    list($t1, $t2) = explode(' ', microtime());
                    $time1 = floatval($t2);
                    $time2 = round(floatval($t1)*100000);
                    $time3 = uniqid();
                    $nonce = md5($time1.$time2.$time3);

                    $timestamp = time();
                    $secure_key = $singleInfo['secret_key'];
                    $keys = array($secure_key, $timestamp, $nonce);
                    sort($keys,2);
                    $sign = implode('',$keys);
                    $sign = sha1($sign);

                    $url = "https://partner.oceanengine.com/union/media/open/api/report/slot?user_id=".$user_id."&nonce=".$nonce."&timestamp=".$timestamp."&sign=".$sign."&start_date=".$start."&end_date=".$end;
//                var_dump($url);
                    $result = CurlRequest::get_response($url);
                    $result_arr = json_decode($result,true);
                    $api_data_i++;
                    if($api_data_i>3)
                        break;
                }
                //判断是否有数
                if(isset($result_arr['code']) && $result_arr['code'] == 100 ){
                    if (count($result_arr['data'])>0) {
                        $map = [];
                        $map['dayid'] = $dayid;
                        $map['source_id'] = SOURCE_ID;
                        $map['account'] = $company_account;
                        $count = DataImportLogic::getChannelData('ad_data', 'erm_data', $map)->count();
//                    var_dump($count);
                        if ($count > 0) {
                            //删除数据
                            DataImportLogic::deleteHistoryData('ad_data', 'erm_data', $map);
                        }
                        $step = [];
                        $insert_data = [];

                        foreach ($result_arr['data'] as $data) {
                            if ($data) {
                                $one_data['type'] = 2;
                                $one_data['app_id'] = $data['appid'];
                                $one_data['app_name'] = $data['site_name'];
                                $one_data['account'] = $company_account;
                                $one_data['source_id'] = SOURCE_ID;
                                $one_data['json_data'] = str_replace('\'', '\'\'', json_encode($data));
                                $one_data['dayid'] = $dayid;
                                $one_data['create_time'] = date("Y-m-d H:i:s");
                                $one_data['year'] = date("Y", strtotime($dayid));
                                $one_data['month'] = date("m", strtotime($dayid));
                                $one_data['ad_id'] = $data['ad_slot_id'];
                                $one_data['ad_name'] = $data['ad_slot_type'];
                                $one_data['income'] = $data['cost'];
                                $insert_data[] = $one_data;
                            }
                        }

                        if ($insert_data) {
                            //批量插入
                            $i = 0;
                            foreach ($insert_data as $kkkk => $insert_data_info) {
                                if ($kkkk % 500 == 0) $i++;
                                if ($insert_data_info) {
                                    $step[$i][] = $insert_data_info;
                                }
                            }
                            if ($step) {
                                foreach ($step as $k => $v) {
                                    $result = DataImportLogic::insertChannelData('ad_data', 'erm_data', $v);
                                    if (!$result) {
                                        echo 'mysql_error' . PHP_EOL;
                                    }
                                }

                            }
                        }
                    }

                } else {
                    $error_msg = AD_PLATFORM.'广告平台'.$company_account.'账号取数失败,错误信息:'.( isset($result_arr['message']) ? $result_arr['message']: '暂无数据');
                    DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);

                    $error_msg_arr = [];
                    $error_msg_arr[] = $error_msg;
                    CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'广告平台取数error');
                }

            }
            // todo tiktok 广告处理过程
            Artisan::call('TiktokReportHandleProcesses' ,['dayid'=>$dayid]);
        }
        } catch (\Exception $e) {
            $error_msg_info = $dayid.'号,'.AD_PLATFORM.'渠道数据匹配失败：'.$e->getMessage();
            DataImportImp::saveDataErrorLog(5,SOURCE_ID,AD_PLATFORM,2,$error_msg_info);

        }
    }

    /**
     * @param $URL
     * @param $header
     * @return mixed
     */
    private static function get_response($URL, $header)
    {
        $crl = curl_init();
        curl_setopt($crl, CURLOPT_URL, $URL);
        curl_setopt($crl, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($crl, CURLOPT_HTTPHEADER, $header);
        curl_setopt($crl, CURLOPT_SSL_VERIFYPEER, false); // 跳过证书检查
        curl_setopt($crl, CURLOPT_SSL_VERIFYHOST, 2);  // 从证书中检查SSL加密算法是否存在
        curl_setopt($crl, CURLOPT_TIMEOUT, 30);  // 从证书中检查SSL加密算法是否存在
        $output = curl_exec($crl);
        curl_close($crl);
        return $output;
    }
}
