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

class IronSourceReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'IronSourceReportCommond {dayid?} {appid?}';

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
        $appid = $this->argument('appid')?$this->argument('appid'):'';
        var_dump($dayid);

        define('AD_PLATFORM', 'IronSource');
        define('SCHEMA', 'ad_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'pad05'); // todo 这个需要根据平台信息表确定平台ID
        try{
        //这里面要写新测试平台里的数据配置 从数据库里取数据
//        $PlatInfo[0]['company_account'] ='weibo@zplay.cn';
//        $PlatInfo[0]['access_key'] ='f35e652190d0';
//        $PlatInfo[0]['secret_key'] ='a98a09866df39217e4ad82445c070426';
//        $PlatInfo[1]['company_account'] ='contact@zplay.cn';
//        $PlatInfo[1]['access_key'] ='283ff2bae2bd';
//        $PlatInfo[1]['secret_key'] ='d1703e6801815818844fdd6a1871cb3c';
//        $PlatInfo[2]['company_account'] ='weibo@zplay.com';
//        $PlatInfo[2]['access_key'] ='a13287d6df45';
//        $PlatInfo[2]['secret_key'] ='0e16d28f99033925ef9fb25e28d98c5f';


        $sql = "SELECT  data_account as company_account,account_api_key  as access_key,account_token  as secret_key from c_platform_account_mapping WHERE platform_id ='pad05' ";
        $PlatInfo = DB::select($sql);
        $PlatInfo = Service::data($PlatInfo);
        if ($PlatInfo){
            foreach ($PlatInfo as $singleInfo){
                $user_name = $singleInfo['company_account'];
                if($user_name =='weibo@zplay.cn'){
                    continue;
                }
                sleep(5);
                // var_dump($user_name);
                $access_key = $singleInfo['access_key'];
                $secret_key = $singleInfo['secret_key'];
                $base64encoded = base64_encode("$user_name:$secret_key");
                $header = array();
                $header[] = 'Authorization: Basic ' . $base64encoded;
//                $url_applist = "https://platform.ironsrc.com/partners/publisher/mediation/applications/v4/stats?startDate={$dayid}&endDate={$dayid}&breakdowns=app,adSource,date,country";  //20170206更新接口地址
                $url_applist = "https://platform.ironsrc.com/partners/publisher/mediation/applications/v5/stats?startDate={$dayid}&endDate={$dayid}&breakdowns=app,adSource,date,country&adSource=ironSource&metrics=revenue,impressions,clicks,adSourceResponses,adSourceResponses,adUnits";  //20200621更新接口地址
                $dataList = self::get_response($url_applist, $header);
                //var_dump($dataList);
                $dataList  = json_decode($dataList,true);
                $res_i=1;
                while(!$dataList){
                    $dataList = self::get_response($url_applist, $header);
                    $dataList  = json_decode($dataList,true);
                    $res_i++;
                    if($res_i>3)
                        break;
                }

                if(!empty($dataList['0']['data'])){

                    $map = [];
                    $map['dayid'] = $dayid;
                    $map['source_id'] = 'pad05';
                    $map['account'] = $user_name;
                    $count = DataImportLogic::getChannelData('ad_data','erm_data',$map)->count();
                    if($count>0){
                        //删除数据
                        DataImportLogic::deleteHistoryData('ad_data','erm_data',$map);
                    }

                    $index = 0;
                    $insert_data =[];
                    $step =[];
                    $insert_data = [];
                    foreach ($dataList as $sData) {
                        $data_info_list = $sData['data'];
                        unset($sData['data']);
                        foreach ($data_info_list as $key => $data_value) {
                            $sData['appName'] = addslashes($sData['appName']);

                            $json_data = array_merge($sData,$data_value);

                            $one_data['type'] = 2;
                            $one_data['app_id'] = $sData['appKey'];
                            $one_data['app_name'] = $sData['appName'];
                            $one_data['account'] = $user_name;
                            $one_data['source_id'] = 'pad05';
                            $one_data['json_data'] = str_replace('\'', '\'\'', json_encode($json_data));
                            $one_data['dayid'] = $dayid;
                            $one_data['create_time'] = date("Y-m-d H:i:s");
                            $one_data['year'] = date("Y", strtotime($dayid));
                            $one_data['month'] = date("m", strtotime($dayid));
                            $one_data['ad_name'] =$json_data['adUnits'];
                            $one_data['ad_id'] = '';
                            $one_data['income'] =$json_data['revenue'];
                            $insert_data[] = $one_data;
                        }
                    }

                    var_dump(count($insert_data));
                    if ($insert_data){
                        //批量插入
                        $i = 0;
                        foreach ($insert_data as $kkkk => $insert_data_info) {
                            if ($kkkk % 1000 == 0) $i++;
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
                    $error_msg = AD_PLATFORM.'广告平台'.$user_name.'账号取数失败,错误信息:'.( isset($dataList['code']) ? $dataList['code']: 0 . ", ERROR:" . $dataList['error'] ? $dataList['error'] : '该账号无数据');
                    DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);

                    $error_msg_arr = [];
                    $error_msg_arr[] = $error_msg;
                    CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'广告平台取数error');
                }

            }
            Artisan::call('IronsourceHandleProcesses' ,['dayid'=>$dayid]);
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
        curl_setopt($crl, CURLOPT_TIMEOUT, 120);  // 从证书中检查SSL加密算法是否存在
        $output = curl_exec($crl);
        curl_close($crl);
        return $output;
    }
}
