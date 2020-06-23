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

class DiankaiReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'DiankaiReportCommond {dayid?}';

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

        define('AD_PLATFORM', '点开聚合');
        define('SCHEMA', 'ad_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'pad63'); // todo 这个需要根据平台信息表确定平台ID

        $start = $dayid;
        $end = $start;
        $time = time();

        // todo 正式
        $sql = " select distinct platform_id,data_account as company_account,account_token as secret_key from c_platform_account_mapping where platform_id = 'pad63' ";
        $PlatInfo = DB::select($sql);
        $PlatInfo = Service::data($PlatInfo);
        if ($PlatInfo){
            foreach ($PlatInfo as $singleInfo){
                $company_account = $singleInfo['company_account'];
                $secret_key = $singleInfo['secret_key'];
                $sign = md5($company_account.$time.$secret_key);
                $url = "http://www.yungao.mobi/media/data/report_api?username=".$company_account."&ts=".$time."&sign=".$sign."&start=".$start."&end=".$end;
                $result = file_get_contents($url);

                // 兼容点开原系统app_id和slot_id
                $app_ids = array("6DRP0J"=>"0731026b1292de03027b3c4b45c712ce", "6DR506"=>"11fea30b669305b59d316d8e613b2291", "6DRA0D"=>"3c62db1e3aa1bc1adfd323f206f0e79a");
                $slot_ids = array("UW2G03"=>"b1cbd186f6d5fbae5fc70571ebe8c322", "UW270I"=>"f364edf7cc4ceb545f23ff98d92d4fc2", "UW2E0I"=>"1d211c1cda2896dea6f65d9957d9e9cf","UW5Z0Z"=>"854b828c570f0bca3f011e50f925dfae");
                var_dump($result);
                $result_arr = json_decode($result,true);

                // 数据获取重试
                $api_data_i=1;
                while(!$result_arr){
                    $result = file_get_contents($url);
                    $result_arr = json_decode($result,true);
                    $api_data_i++;
                    if($api_data_i>3)
                        break;
                }
                //判断是否有数
                if(isset($result_arr['success']) && $result_arr['success']===true && count($result_arr['data'])>0){
                    $map = [];
                    $map['dayid'] = $dayid;
                    $map['source_id'] = SOURCE_ID;
                    $map['account'] = $company_account;
                    $count = DataImportLogic::getChannelData('ad_data','erm_data',$map)->count();
                    if($count>0){ 
                        //删除数据
                        DataImportLogic::deleteHistoryData('ad_data','erm_data',$map);
                    }
                    $step =[];
                    $insert_data = [];

                    foreach($result_arr['data'] as $data){
                        if($data){
                            $click_rate = str_replace('%','',$data['click_rate']);
                            if (array_key_exists($data['app_id'], $app_ids)) {
                                $app_id = $app_ids[$data['app_id']];
                            } else {
                                $app_id = $data['app_id'];
                            }

                            if (array_key_exists($data['slot_id'], $slot_ids)) {
                                $slot_id = $slot_ids[$data['slot_id']];
                            } else {
                                $slot_id = $data['slot_id'];
                            }

                            $data['click_rate'] = $click_rate;
                            $data['app_id'] = $app_id;
                            $data['slot_id'] = $slot_id;
                            $data['app_name'] = addslashes($data['app_name']);
                            $data['slot_name'] = addslashes($data['slot_name']);


                            $one_data['type'] = 2;
                            $one_data['app_id'] = $app_id;
                            $one_data['app_name'] = $data['app_name'];
                            $one_data['account'] = $company_account;
                            $one_data['source_id'] = SOURCE_ID;
                            $one_data['json_data'] = str_replace('\'', '\'\'', json_encode($data));
                            $one_data['dayid'] = $dayid;
                            $one_data['create_time'] = date("Y-m-d H:i:s");
                            $one_data['year'] = date("Y", strtotime($dayid));
                            $one_data['month'] = date("m", strtotime($dayid));
                            $one_data['ad_id'] =$slot_id;
                            $one_data['ad_name'] =$data['slot_name'];
                            $one_data['income'] =$data['income'];
                            $insert_data[] = $one_data;

                        }
                    }

                    if ($insert_data){
                        //批量插入
                        $i = 0;
                        foreach ($insert_data as $kkkk => $insert_data_info) {
                            if ($kkkk % 2000 == 0) $i++;
                            if ($insert_data_info) {
                                $step[$i][] = $insert_data_info;
                            }
                        }
                        if ($step) {
                            foreach ($step as $k => $v) {
                                $result = DataImportLogic::insertChannelData('ad_data','erm_data',$v);
                                if (!$result) {
                                    echo 'mysql_error'. PHP_EOL;
                                }
                            }

                        }
                    }

                } else {
                    $error_msg = AD_PLATFORM.'广告平台'.$company_account.'账号取数失败,错误信息:'.( isset($result_arr['msg']) ? $result_arr['msg']: '该账号无数据');
                    DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);

                    $error_msg_arr = [];
                    $error_msg_arr[] = $error_msg;
                    CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'广告平台取数error');
                }

            }
            Artisan::call('DiankaiHandleProcesses' ,['dayid'=>$dayid]);
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
