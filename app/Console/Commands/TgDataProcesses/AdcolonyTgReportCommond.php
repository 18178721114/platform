<?php

namespace App\Console\Commands\TgDataProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\Common\CurlRequest;
use App\Common\Service;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\DataImportLogic;
use Illuminate\Support\Facades\Artisan;
use App\Common\CommonFunction;

class AdcolonyTgReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'AdcolonyTgReportCommond {dayid?} {appid?}';

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
    	var_dump('Adcolony-ptg63-'.$dayid);

        define('AD_PLATFORM', 'Adcolony');
        define('SCHEMA', 'tg_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'ptg63'); // todo 这个需要根据平台信息表确定平台ID
        $sdayid = date('mdY',strtotime($dayid));

        try {
            //这里面要写新测试平台里的数据配置 从数据库里取数据
            $sql = " select distinct a.platform_id,a.data_account as company_account,a.account_api_key as apiKey from c_platform_account_mapping a where a.platform_id = 'ptg63' and status = 1";
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
                $url = "http://clients.adcolony.com/api/v2/advertiser_summary?user_credentials={$value['apiKey']}&date={$sdayid}&end_date={$sdayid}&format=json&group_by=ad_group&date_group=day&group_by=creative&group_by=country";
                //var_dump($url);
                $info = self::get_response($url);
                $ret = json_decode($info, true);

                // 数据获取重试
                $api_data_i = 1;
                while (!$ret) {
                    $info = self::get_response($url);
                    $ret = json_decode($info, true);
                    $api_data_i++;
                    if ($api_data_i > 3) break;
                }
                if (isset($ret['status']) && $ret['status'] == 'success') {
                    // 接口成功，是否有无数据
                    if ($ret['results']) {
                        // 成功取到数
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
                        $insert_data = [];
                        $step = [];
                        foreach ($ret['results'] as $v) {

                            $v['campaign_name'] = addslashes($v['campaign_name']);
                            $v['creative_name'] = addslashes($v['creative_name']);
                            $v['group_name'] = addslashes($v['group_name']);

                            $insert_data[$index]['app_id'] = $v['store_id'];
                            $insert_data[$index]['account'] = $value['company_account'];
                            $insert_data[$index]['type'] = 2;
                            $insert_data[$index]['source_id'] = SOURCE_ID;
                            $insert_data[$index]['dayid'] = $dayid;
                            $insert_data[$index]['json_data'] = str_replace('\'', '\'\'', json_encode($v));
                            $insert_data[$index]['create_time'] = date("Y-m-d H:i:s");
                            $insert_data[$index]['year'] = date("Y", strtotime($dayid));
                            $insert_data[$index]['month'] = date("m", strtotime($dayid));
                            $insert_data[$index]['campaign_id'] = $v['campaign_id'];
                            $insert_data[$index]['campaign_name'] = str_replace('\'', '\'\'', $v['campaign_name']);
                            $insert_data[$index]['cost'] = $v['spend'] ? $v['spend'] : 0;
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
                            }
                        }
                    }else{
                        // 无数据报错
//                        $all_data_err[] = $value['company_account'].'账号取数失败,错误信息:暂无数据,'.json_encode($ret);
                    }
                } else {
                    // 取数错误报错
                    $all_data_err[] = $value['company_account'] . '账号取数失败,错误信息:' . (isset($ret['result']) ? $ret['result'] : '无数据,接口未返回任何信息');
                }

            }

            if ($all_data_err){
                $error_application_str = implode(',',$all_data_err);
                $message = "{$dayid}号,". AD_PLATFORM . " 推广平台取数失败信息:".$error_application_str ;
                DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,4,$message);
                $error_msg_arr[] = $message;
                CommonFunction::sendMail($error_msg_arr, '推广平台取数error');
            }

            Artisan::call('AdcolonyTgHandleProcesses',['dayid'=>$dayid]);

        } catch (\Exception $e) {
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
}
