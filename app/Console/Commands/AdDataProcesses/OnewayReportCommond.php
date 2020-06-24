<?php

namespace App\Console\Commands\AdDataProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\Common\CommonFunction;
use App\Common\CurlRequest;
use App\Common\Service;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\DataImportLogic;

class OnewayReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'OnewayReportCommond {dayid?} {appid?}';

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
        // 入口方法
    	$dayid = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));
    	$appid = $this->argument('appid')?$this->argument('appid'):'';
        $begin_date =  strtotime($dayid);
        $end_date =$begin_date+86400;
    	var_dump($dayid);

        define('AD_PLATFORM', 'OneWay');
        define('SCHEMA', 'ad_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID_CONF', '10064'); // todo 这个需要根据平台信息表确定平台ID
        define('SOURCE_ID', 'pad52'); // todo 这个需要根据平台信息表确定平台ID
        try{
//        $PlatInfo = DataImportLogic::getConf(SOURCE_ID_CONF);
//        $PlatInfo = Service::data($PlatInfo);

        // todo  数据库配置
        $sql = " SELECT  data_account as company_account,account_api_key  as accessKey from c_platform_account_mapping WHERE platform_id ='pad52' and status = 1";
        $PlatInfo = DB::select($sql);
        $PlatInfo = Service::data($PlatInfo);

        if (!$PlatInfo){
            $message = "{$dayid}, " . AD_PLATFORM . "广告平台取数失败,失败原因:取数配置信息为空" ;
            DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$message);

            $error_msg_arr = [];
            $error_msg_arr[] = $message;
            CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'广告平台取数error');
            exit;
        }


    	foreach ($PlatInfo as $key => $value) {

            //删除数据库里原来数据
            $map['dayid'] = $dayid;
            $map['source_id'] = SOURCE_ID;
            $map['account'] = $value['company_account'];
            $count = DataImportLogic::getChannelData('ad_data','erm_data',$map)->count();
            if($count>0){

            //删除数据
                DataImportLogic::deleteHistoryData('ad_data','erm_data',$map);
            }

            $access_key = $value['accessKey'];
            $ad_type_list = array("1","2","3","6","7");
            foreach($ad_type_list as $ad_type) {
                sleep(5);
                $result = array();
                $url = str_replace(array('_ACCESSKEY_','_ADTYPE_','_END_DATE_','_BEGIN_DATE_'),array($access_key,$ad_type,$end_date,$begin_date),env('ONEWAY_URL'));
                //echo $url.PHP_EOL;
                $data = self::get_response($url);
                $report_data = json_decode($data,true);

                if($report_data['message']=='Request too often') {
                    $error_msg = AD_PLATFORM.'广告平台'.$value['company_account'].'账号取数失败,错误信息:'.$report_data['message'];
                    DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);

                    $error_msg_arr = [];
                    $error_msg_arr[] = $error_msg;
                    CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'广告平台取数error');

                    sleep(5);
                    $data = self::get_response($url);
                    $report_data = json_decode($data,true);
                }

                if($report_data['success']!=true){

                    $error_msg = AD_PLATFORM.'广告平台'.$value['company_account'].'账号取数失败,错误信息:'.$report_data['message'];
                    DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);

                    $error_msg_arr = [];
                    $error_msg_arr[] = $error_msg;
                    CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'广告平台取数error');
                    continue;
                }
                $result = $report_data['data']['rows'];
                $count = $report_data['data']['total'];
                $pages = ceil(1.0*$count/200);
                for ($i=2;$i<=$pages;$i++) {
                    $offset = ($i-1)*200;
                    $data = self::get_response($url."&offset={$offset}");
                    $report_data = json_decode($data,true);
                    if($report_data['success']!==true){
                        continue;
                    }
                    $result = array_merge($result,$report_data['data']['rows']);

                }

                if (count($result)<=0) {
                    continue;
                }

                $index =0;
                $insert_data =[];
                $step =[];
                foreach ($result as $v){
                    $insert_data[$index]['income'] =$v['revenue'];
                    $insert_data[$index]['app_id'] = $v['publishId'];
                    $insert_data[$index]['app_name'] = $v['appName'];
                    $insert_data[$index]['account'] = $value['company_account'];
                    $insert_data[$index]['type'] = 2;
                    $insert_data[$index]['source_id'] = SOURCE_ID;
                    $insert_data[$index]['dayid'] = $dayid;
                    $insert_data[$index]['json_data'] =str_replace('\'','\'\'',json_encode($v));
                    $insert_data[$index]['create_time'] = date("Y-m-d H:i:s");
                    $insert_data[$index]['year'] = date("Y",strtotime($dayid));
                    $insert_data[$index]['month'] = date("m",strtotime($dayid));
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
                        $result = DataImportLogic::insertChannelData('ad_data','erm_data',$v);
                        if (!$result) {
                             echo 'mysql_error'. PHP_EOL;
                        }
                    }
                }

            }
    	}

        // 调用数据处理过程
            Artisan::call('OnewayHandleProcesses',['dayid' => $dayid]);
        } catch (\Exception $e) {
            $error_msg_info = $dayid.'号,'.AD_PLATFORM.'广告平台程序失败，失败原因：'.$e->getMessage();
            DataImportImp::saveDataErrorLog(5,SOURCE_ID,AD_PLATFORM,2,$error_msg_info);

        }
    		
    }
    /**
     * @param $api_key
     * @param $start_dayid
     * @param $end_dayid
     * @return mixed
     */
    public static function get_response($url,$headers=array())
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
