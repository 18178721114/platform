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

class KewanReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'KewanReportCommond {dayid?} {appid?}';

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
        die;
        set_time_limit(0);
        echo '<pre>';
        // 入口方法
        $dayid = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));
        $appid = $this->argument('appid')?$this->argument('appid'):'';
        $dayid = str_replace('-', '', $dayid);
        var_dump($dayid);


        define('AD_PLATFORM', 'KeWan');
        define('SCHEMA', 'ad_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'pad69'); // todo 这个需要根据平台信息表确定平台ID
        try{
        //这里面要写新测试平台里的数据配置 从数据库里取数据
//        $info[0]['company_username'] ='8C8F7D4B-B2A5-F9FC-1371-CB271911B84E';
//        $info[0]['api_key'] ='5392FE33-1AD4-A98F-B4FE-527718366B27';
//        $info[1]['company_username'] ='BBD48B13-B725-49B1-A3F9-BE6CBFA2189D';
//        $info[1]['api_key'] ='B7FF4F07-9876-C910-1DCA-6240422D0230';

         $sql = "SELECT  data_account as company_username,account_api_key  as api_key from c_platform_account_mapping WHERE platform_id ='pad69' and status = 1 ";
        $PlatInfo = DB::select($sql);
        $info = Service::data($PlatInfo);
        foreach ($info as $key => $value) {

            $token = $value['api_key'];
            // 生成签名
            $timestamp = time();
            $nonce = rand(1,999);
            $tmpArr = array($token, $timestamp, $nonce);
            sort($tmpArr, SORT_STRING);
            $signature = implode( $tmpArr );
            $signature = sha1( $signature );

            $params = array(
                'signature' => $signature,
                'timestamp' => $timestamp,
                'nonce' => $nonce,
            );

            $url = env('KEWAN_URL')."/developer/{$value['company_username']}/apps";
            $url .= '?' . http_build_query($params, null, '&');
            //echo $url;
            $appInfoList1 = self::get_response($url);
            $appInfoList = json_decode($appInfoList1,true);

            // 数据获取重试
            $api_data_i=1;
            while(!$appInfoList){
                // 生成签名
                $timestamp = time();
                $nonce = rand(1,999);
                $tmpArr = array($token, $timestamp, $nonce);
                sort($tmpArr, SORT_STRING);
                $signature = implode( $tmpArr );
                $signature = sha1( $signature );

                $params = array(
                    'signature' => $signature,
                    'timestamp' => $timestamp,
                    'nonce' => $nonce,
                );

                $url = env('KEWAN_URL')."/developer/{$value['company_username']}/apps";
                $url .= '?' . http_build_query($params, null, '&');
                //echo $url;
                $appInfoList1 = self::get_response($url);
                $appInfoList = json_decode($appInfoList1,true);
                $api_data_i++;
                if($api_data_i>3)
                    break;
            }
            if($api_data_i ==4 && empty($appInfoList)){
                $error_msg_1 = AD_PLATFORM.'广告平台'.$value['company_username'].'账号取数失败,错误信息:返回数据为空('.json_encode($appInfoList1).')';
                DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg_1);
                continue;

            }

            //获取应用信息
            if(!empty($appInfoList['data'])){
                foreach ($appInfoList['data'] as $appInfo){
                    $dataUrl = env('KEWAN_URL') . "/developer/{$value['company_username']}/app/{$appInfo['app_id']}/stats";
                    $params['start_date'] = $dayid;
                    $params['end_date'] = $dayid;
                    $params['group_dimension'] = "country";

                    $params['page'] = 1;
                    $params['size'] = 80000;

                    $dataUrl .= '?' . http_build_query($params, null, '&');
                    $dataInfo = self::get_response($dataUrl);
                    $dataInfo = json_decode($dataInfo,true);
                    if(!empty($dataInfo['data'])){
                        $map['dayid'] = $dayid;
                        $map['source_id'] = SOURCE_ID;
                        $map['account'] = $value['company_username'];
                        $map['app_id'] = $appInfo['app_id'];
                        $count = DataImportLogic::getChannelData('ad_data','erm_data',$map)->count();
                        if($count>0){

                        //删除数据
                            DataImportLogic::deleteHistoryData(SCHEMA,TABLE_NAME,$map);
                        }
                        $index = 0;
                        $insert_data =[];
                        $step =[];
                        foreach ($dataInfo['data']['list'] as $data){
                                    $insert_data[$index]['account'] = $value['company_username'];
                                    $insert_data[$index]['type'] = 2;
                                    $insert_data[$index]['app_name'] = $appInfo['name'];
                                    $insert_data[$index]['app_id'] = $appInfo['app_id'];
                                    $insert_data[$index]['source_id'] = SOURCE_ID;
                                    $insert_data[$index]['dayid'] = $dayid;
                                    $data['os'] = $appInfo['os'];
                                    $data['name'] = $appInfo['name'];
                                    $insert_data[$index]['json_data'] =str_replace('\'','\'\'',json_encode($data));
                                    $insert_data[$index]['create_time'] = date("Y-m-d H:i:s");
                                    $insert_data[$index]['year'] = date("Y",strtotime($dayid));
                                    $insert_data[$index]['month'] = date("m",strtotime($dayid));
                                    $insert_data[$index]['income'] =$data['income']/100;
                                    $insert_data[$index]['ad_id'] =$data['ad_unit_id'];

                                    $index++;
                        }
                        $i = 0;
                        foreach ($insert_data as $kkkk => $insert_data_info) {
                            if ($kkkk % 2000 == 0) $i++;
                            if ($insert_data_info) {
                                $step[$i][] = $insert_data_info;
                            }
                        }
                        if ($step) {
                            foreach ($step as $k => $v) {
                                $result = DataImportLogic::insertChannelData(SCHEMA,TABLE_NAME,$v);
                                if (!$result) {
                                     echo 'mysql_error'. PHP_EOL;
                                }
                            }
                            
                        }
                    }else{

                        if(count($dataInfo['data']['list']) ==0){

                            $error_msg = AD_PLATFORM.'广告平台'.$value['company_username'].'账号下应用'.json_encode($dataInfo).'数据为空';
                            DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);
                        }else{
                            $error_msg = AD_PLATFORM.'广告平台'.$value['company_username'].'账号下应用'.$appInfo['app_id'].'取数失败,错误信息:'.$dataInfo['error'];
                            DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);

                            $error_msg_arr = [];
                            $error_msg_arr[] = $error_msg;
                            CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'广告平台取数error');
                        }
                        
                    }
                }

            }else {
                $error_msg = AD_PLATFORM.'广告平台'.$value['company_username'].'账号取数失败,错误信息:'.$appInfoList1;
                DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);
            }
        }
            Artisan::call('KewanHandleProcesses' ,['dayid'=>$dayid]);
        } catch (\Exception $e) {
            $error_msg_info = $dayid.'号,'.AD_PLATFORM.'广告平台程序失败，失败原因：'.$e->getMessage();
            DataImportImp::saveDataErrorLog(5,SOURCE_ID,AD_PLATFORM,2,$error_msg_info);

        }
    }
    public static function get_response($url, $headers='')
    {
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);

        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_HEADER, 0);
        curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
        curl_setopt($ch, CURLOPT_TIMEOUT,120); 
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, FALSE); 
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, FALSE);

        if (!empty($headers)) {
            curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
        }

        $output = curl_exec($ch);
        curl_close($ch);
        return $output;
    }
}
