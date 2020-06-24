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

class UnityTgReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'UnityTgReportCommond {dayid?} {appid?}';

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
        header('content-type:text/html;charset=utf-8');
        // 入口方法
    	$dayid = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));
    	$appid = $this->argument('appid')?$this->argument('appid'):'';
    	var_dump($dayid);

        define('AD_PLATFORM', 'Unity');
        define('SCHEMA', 'tg_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'ptg36'); // todo 这个需要根据平台信息表确定平台ID

        try {
            $sql = " select distinct platform_id,data_account as company_account,account_api_key as api_key,account_user_id as Organization_ID from c_platform_account_mapping where platform_id = 'ptg36' ";
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

//    	$info[0]['company_account'] ='contact@zplay.com';
//    	$info[0]['api_key'] ='32227f77a958a849b8b782c8db75fdd1fb0416b4e54df09e2bde6563ef59a97f';
//        $info[0]['Organization_ID'] ='57cfe1006ff3cc1300aa233a';

            foreach ($info as $key => $value) {
                //获取应用信息
                $url = str_replace(array('_Organization_ID_', '_API_KEY_', '_END_DATE_', '_BEGIN_DATE_'), array($value['Organization_ID'], $value['api_key'], $dayid, $dayid), env('UNITY_TG_URL'));
                $info = self::get_response($url);
                $result_info = json_decode($info, true);

                // 数据获取重试
                $api_data_i = 1;
                while (!$result_info) {
                    $info = self::get_response($url);
                    $result_info = json_decode($info, true);
                    $api_data_i++;
                    if ($api_data_i > 3) break;
                }

                if (isset($result_info['error']) || !$info) {
                    if($result_info){
                        $error_msg_arr = $result_info['error']['parameters'][0];
                        $error_msg_arr = array_values($error_msg_arr);
                        $error_msg_str = implode(',', $error_msg_arr);
                    }else{
                        $error_msg_str = '无数据，接口未返回任何信息';
                    }
                    $error_msg = AD_PLATFORM . '推广平台' . $value['company_account'] . '账号获取数据失败,错误信息:' . $error_msg_str;
                    DataImportImp::saveDataErrorLog(1, SOURCE_ID, AD_PLATFORM, 4, $error_msg);
                    $error_msg_arr = [];
                    $error_msg_arr[] = $error_msg;
                    CommonFunction::sendMail($error_msg_arr, AD_PLATFORM . '推广平台取数error');
                    continue;
                }

                $response_arr = self::parse_csv($info);

                if (!empty($response_arr)) {//成功取到数

                    //删除数据库里原来数据
                    $map['dayid'] = $dayid;
                    $map['source_id'] = SOURCE_ID;
                    $map['account'] = $value['company_account'];

                    //删除数据
                    $count = DataImportLogic::getChannelData('tg_data', 'erm_data', $map)->count();
                    if ($count > 0) {
                        DataImportLogic::deleteHistoryData('tg_data', 'erm_data', $map);
                    }
                    $index = 0;
                    $insert_data = [];
                    $step = [];
                    foreach ($response_arr as $v) {

                        $insert_data[$index]['campaign_id'] = isset($v['campaign_id']) ? $v['campaign_id'] : '';
                        $insert_data[$index]['campaign_name'] = isset($v['campaign_name']) ? $v['campaign_name'] : '';
                        $insert_data[$index]['cost'] = isset($v['spend']) ? $v['spend'] : 0.00; // 流水原币
                        $insert_data[$index]['app_id'] = isset($v['target_store_id']) ? $v['target_store_id'] : '';
                        $insert_data[$index]['app_name'] = isset($v['target_name']) ? $v['target_name'] : '';

                        $insert_data[$index]['account'] = $value['company_account'];
                        $insert_data[$index]['type'] = 2;
                        $insert_data[$index]['source_id'] = SOURCE_ID;
                        $insert_data[$index]['dayid'] = $dayid;
                        $insert_data[$index]['json_data'] = str_replace('\'', '\'\'', json_encode($v));
                        $insert_data[$index]['create_time'] = date("Y-m-d H:i:s");
                        $insert_data[$index]['year'] = date("Y", strtotime($dayid));
                        $insert_data[$index]['month'] = date("m", strtotime($dayid));
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
                            $result = DataImportLogic::insertChannelData('tg_data', 'erm_data', $v);
                            if (!$result) {
                                echo 'mysql_error' . PHP_EOL;
                            }
                        }
                    }

                }

            }

            // 调用数据处理过程
            Artisan::call('UnityTgHandleProcesses', ['dayid' => $dayid]);
        }catch (\Exception $e) {
            // 异常报错
            $message = "{$dayid}号, " . AD_PLATFORM . " 推广平台程序报错,报错原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, SOURCE_ID, AD_PLATFORM, 4, $message);
            $error_msg_arr[] = $message;
            CommonFunction::sendMail($error_msg_arr, '推广平台程序error');
            exit;

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

    public static function parse_csv($content){
        $data = explode("\n", trim($content, "\n"));
        $data = array_map('str_getcsv', $data);
        if (isset($data[1])) {
            $filed = array_map(function ($value) {
                return strtolower(preg_replace('/\s+/', '_', $value));
            }, $data[0]);

            unset($data[0]);
            foreach ($data as &$value) {
                $value = array_combine($filed, $value);
            }
            unset($value);

            return $data;
        }
    }
}
