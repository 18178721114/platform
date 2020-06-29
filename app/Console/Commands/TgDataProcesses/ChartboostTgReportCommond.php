<?php

namespace App\Console\Commands\TgDataProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\Common\CurlRequest;
use App\Common\Service;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\DataImportLogic;
use App\Common\CommonFunction;

class ChartboostTgReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'ChartboostTgReportCommond {dayid?} {appid?}';

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
    public function handle(){


        define('AD_PLATFORM', 'Chartboost');
        define('SCHEMA', 'tg_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'ptg25'); // todo 这个需要根据平台信息表确定平台ID

        // 入口方法
        $dayid = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));
        $appid = $this->argument('appid')?$this->argument('appid'):'';
        var_dump($dayid);

        self::getChartBoostData($dayid);
    		
    }

    public static function getChartBoostData($dayid){

        try {
            $sql = " select distinct platform_id,data_account as company_account,account_user_id as userId,account_token as userSignature from c_platform_account_mapping where platform_id = 'ptg25' and status = 1 ";
            $info = DB::select($sql);
            $info = Service::data($info);
            if (!$info) {
                // 无配置报错提醒
                $message = "{$dayid}号, " . AD_PLATFORM . " 推广平台取数失败,失败原因:取数配置信息为空";
                DataImportImp::saveDataErrorLog(1, SOURCE_ID, AD_PLATFORM, 4, $message);
                $error_msg_arr[] = $message;
                CommonFunction::sendMail($error_msg_arr, '推广平台取数error');
                exit;
            }

            $all_data_err = [];
            foreach ($info as $key => $value) {
                //获取应用信息
                $url = str_replace(array('_USER_SIGNATURE_', '_USERID_', '_END_DATE_', '_BEGIN_DATE_'), array($value['userSignature'], $value['userId'], $dayid, $dayid), env('CHARTBOOST_TG_URL'));
                $response = self::get_response($url);
                $response = json_decode($response, true);

                // 数据获取重试
                $api_data_i = 1;
                while (!$response) {
                    sleep(10);
                    $response = self::get_response($url);
                    $response = json_decode($response, true);
                    $api_data_i++;
                    if ($api_data_i > 3) break;
                }
                // 获取jobId失败
                if (!isset($response['jobId']) || !$response) {
                    $all_data_err[] = $value['company_account'] . '账号获取jobId失败,错误信息:' . (isset($response['message']) ? $response['message'] : '无数据,接口未返回任何信息');
                }else{
                    $jobId = $response['jobId'];
                    $dataUrl = env('CHARTBOOST_TG_URL_INFO') . $jobId;
                    $info = self::get_response($dataUrl);
                    $info = str_replace("\t", ",", $info);
                    $data = self::parse_csv($info);
                    // 数据获取重试
                    $api_data_ii = 1;
                    while (!$data) {
                        sleep(10);
                        $dataUrl = env('CHARTBOOST_TG_URL_INFO') . $jobId;
                        $info = self::get_response($dataUrl);
                        $info = str_replace("\t", ",", $info);
                        $data = self::parse_csv($info);
                        $api_data_ii++;
                        if ($api_data_ii > 3) break;
                    }

                    if (!empty($data)) {//成功取到数
                        //删除数据库里原来数据
                        $map['dayid'] = $dayid;
                        $map['source_id'] = SOURCE_ID;
                        $map['account'] = $value['company_account'];
                        $count = DataImportLogic::getChannelData('tg_data', 'erm_data', $map)->count();
                        if ($count > 0) {
                            //删除数据
                            DataImportLogic::deleteHistoryData('tg_data', 'erm_data', $map);
                        }
                        $index = 0;
                        $arr = [];
                        $insert_data = [];
                        $step = [];
                        foreach ($data as $v) {
                            $v = self::del_arr_mark($v);
                            if (count($v) != 1) {
                                $new_v = str_replace(array("\u0000"), '', json_encode($v));
                                $insert_data[$index]['account'] = $value['company_account'];
                                $insert_data[$index]['type'] = 2;
                                $insert_data[$index]['source_id'] = SOURCE_ID;
                                $insert_data[$index]['dayid'] = $dayid;
                                $insert_data[$index]['json_data'] = str_replace(array("\u0000"), '', json_encode($v));
                                $insert_data[$index]['create_time'] = date("Y-m-d H:i:s");
                                $insert_data[$index]['year'] = date("Y", strtotime($dayid));
                                $insert_data[$index]['month'] = date("m", strtotime($dayid));
                                $new_v = json_decode($new_v, true);
                                $insert_data[$index]['app_id'] = isset($new_v['app_id']) ? $new_v['app_id'] : '';
                                $insert_data[$index]['campaign_id'] = isset($new_v['campaign_id']) ? $new_v['campaign_id'] : '';
                                $insert_data[$index]['campaign_name'] = isset($new_v['campaign_name']) ? addslashes(str_replace('\'\'', '\'', $new_v['campaign_name'])) : '';;
                                $insert_data[$index]['cost'] = isset($new_v['money_spent']) ? $new_v['money_spent'] : 0;
                                $index++;
                            }
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
                                $result = DataImportLogic::insertChannelData(SCHEMA, TABLE_NAME, $v);
                                if (!$result) {
                                    echo 'mysql_error' . PHP_EOL;
                                }
                            }
                        }

                    } else {
                        $all_data_err[] = 'Chartboos推广平台' . $value['company_account'] . '账号取数失败,错误信息:无数据,接口未返回任何信息';
                    }
                }

            }

            if ($all_data_err){
                $error_application_str = implode(',',$all_data_err);
                $message = "{$dayid}号,". AD_PLATFORM . " 推广平台取数失败信息:".$error_application_str ;
                DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,4,$message);
                $error_msg_arr[] = $message;
                CommonFunction::sendMail($error_msg_arr, '推广平台取数error');
            }

            // 调用数据处理过程
            Artisan::call('ChartboostTgHandleProcesses', ['dayid' => $dayid]);

        }catch (\Exception $e) {
            // 异常报错
            $message = "{$dayid}号, " . AD_PLATFORM . " 推广平台程序报错,报错原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, SOURCE_ID, AD_PLATFORM, 4, $message);
            $error_msg_arr[] = $message;
            CommonFunction::sendMail($error_msg_arr, '推广平台程序error');
            exit;

        }
    }


    public static function del_arr_mark($arr){
        $new_arr = array();
        foreach($arr as $a_k =>  $s){
            $n_s = str_replace('"','',$s);
            $n_s = self::characet($n_s);
            //$n_s = iconv('unicode','utf-8',$n_s);
            $new_arr[$a_k] = trim($n_s);
        }
        return $new_arr;
    }

    public static function characet($data){
        if( !empty($data) ){
            $fileType = mb_detect_encoding($data , array('UTF-8','GBK','LATIN1','BIG5')) ;
            if( $fileType != 'UTF-8'){
                $data = mb_convert_encoding($data ,'utf-8' , $fileType);
            }
        }
        return $data;
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

    public static function parse_csv($content){
        $data = explode("\n", trim($content, "\n"));
        $data = array_map('str_getcsv', $data);
        if (isset($data[1])) {
            $filed = array_map(function ($value) {
                return strtolower(preg_replace('/\s+/', '_', $value));
            }, $data[0]);
            $filed[0]='date';

            unset($data[0]);
            foreach ($data as &$value) {
                if(count($filed) ==count($value) ){
                    $value = array_combine($filed, $value);

                }else{
                    // todo
//                     file_put_contents('./storage/tgDataLogs/charset_tg.log',date('Y-m-d H:i:s').json_encode($filed)."\n",FILE_APPEND);
//                     file_put_contents('./storage/tgDataLogs/charset_tg.log',date('Y-m-d H:i:s').json_encode($value)."\n",FILE_APPEND);
                }

            }
            unset($value);

            return $data;
        }
    }
}
