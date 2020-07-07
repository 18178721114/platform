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
use Illuminate\Support\Facades\Redis;

class ToutiaoTgReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'ToutiaoTgReportCommond {dayid?}';

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
        Redis::select(0);
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
    	$dayid = $this->argument('dayid') ? $this->argument('dayid') : date('Y-m-d',strtotime('-1 day'));

        define('AD_PLATFORM', '今日头条');
        define('SCHEMA', 'tg_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'ptg66'); // todo 这个需要根据平台信息表确定平台ID

        try {
            $toutiao_app_list = DB::select("select distinct platform_id,data_account as username,account_app_id as app_id,account_token as secret,account_user_id as advertiser_id from c_platform_account_mapping   where platform_id = 'ptg66' and  account_user_id is not null and account_token is not null and account_app_id is not null and status = 1 ");
            $toutiao_app_list = Service::data($toutiao_app_list);
            if (!$toutiao_app_list) {
                // 无配置报错提醒
                $message = "{$dayid}号, " . AD_PLATFORM . " 推广平台取数失败,失败原因:取数配置信息为空";
                DataImportImp::saveDataErrorLog(1, SOURCE_ID, AD_PLATFORM, 4, $message);
                $error_msg_arr[] = $message;
                CommonFunction::sendMail($error_msg_arr, '推广平台取数error');
                exit;
            }

            //这里面要写新测试平台里的数据配置 从数据库里取数据
            if ($toutiao_app_list) {
                foreach ($toutiao_app_list as $toutiao_app_info) {

                    $get_access_token_result = self::refreshToken($toutiao_app_info);
                    if ($get_access_token_result) {

                        //删除数据库里原来数据
                        $map['dayid'] = $dayid;
                        $map['source_id'] = SOURCE_ID;
                        $map['account'] = $toutiao_app_info['username'];

                        //删除数据
                        $count = DataImportLogic::getChannelData('tg_data', 'erm_data', $map)->count();
                        if ($count > 0) {
                            DataImportLogic::deleteHistoryData('tg_data', 'erm_data', $map);
                        }

                        // 获取数据
                        $get_access_token_arr = json_decode($get_access_token_result, true);
                        $group_by = json_encode(array('STAT_GROUP_BY_FIELD_ID'));

                        for ($page = 1; $page <= 2; $page++) {
                            $data_url = "https://ad.toutiao.com/open_api/2/report/ad/get/?advertiser_id={$toutiao_app_info["advertiser_id"]}&start_date={$dayid}&end_date={$dayid}&page={$page}&page_size=1000&group_by={$group_by}";
                            echo $data_url . PHP_EOL;
                            $data = self::get_data($data_url, $get_access_token_arr['data']['access_token']);
                            $data_arr = json_decode($data, true);
                            if (!empty($data_arr['data']['list'])) {
                                $data_arr = $data_arr['data']['list'];

                                $index = 0;
                                $insert_data = [];
                                $step = [];
                                foreach ($data_arr as $v) {
                                    $v['advertiser_id'] = $toutiao_app_info['advertiser_id'];
                                    $v['account'] = $toutiao_app_info['username'];

                                    $insert_data[$index]['campaign_id'] = isset($v['campaign_id']) ? $v['campaign_id'] : '';
                                    $insert_data[$index]['campaign_name'] = isset($v['campaign_name']) ? $v['campaign_name'] : '';
                                    $insert_data[$index]['cost'] = isset($v['cost']) ? $v['cost'] : 0;

                                    $insert_data[$index]['account'] = $toutiao_app_info['username'];
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
                            }else{
                                $error_msg = AD_PLATFORM.'推广平台广告主ID为' . $toutiao_app_info["advertiser_id"] .'获取数据失败,错误信息:';
//                                if (key_exists('code',$data_arr) && $data_arr['code'] == 0){
//                                    $error_msg .= '暂无数据'.json_encode($data_arr);
//                                }else
                                if(key_exists('code',$data_arr) && $data_arr['code'] != 0){
                                    $error_msg .= $data_arr['message'];
                                }else{
                                    $error_msg .= '无数据，接口未返回任何信息';
                                }
                                DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,4,$error_msg);
                            }
                        }
                    }
                }
                // 数据处理过程
                Artisan::call('JinritoutiaoTgHandleProcesses', ['dayid' => $dayid]);
            }
        }catch (\Exception $e) {
            // 异常报错
            $message = "{$dayid}号, " . AD_PLATFORM . " 推广平台程序报错,报错原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, SOURCE_ID, AD_PLATFORM, 4, $message);
            $error_msg_arr[] = $message;
            CommonFunction::sendMail($error_msg_arr, '推广平台程序error');
            exit;

        }
    }


    // 刷新token
    private static function refreshToken($toutiao_app_info){

        $advertiser_id = $toutiao_app_info['advertiser_id'];
        $access_json = Redis::get($advertiser_id);
        if ($access_json) {
            $content_arr = json_decode($access_json, true);
            $refresh_token_data = [
                'app_id' => $toutiao_app_info['app_id'],
                'secret' => $toutiao_app_info['secret'],
                'grant_type' => 'refresh_token',
                'refresh_token' => $content_arr['data']['refresh_token']
            ];
            $get_access_token_url = "https://ad.toutiao.com/open_api/oauth2/refresh_token/";
            $get_access_token_result = CurlRequest::curl_post_https($get_access_token_url, $refresh_token_data);
            $get_access_token_result = json_decode($get_access_token_result,true);
            if ($get_access_token_result && !empty($get_access_token_result['data'])){
                Redis::set($advertiser_id,json_encode($get_access_token_result));
                return json_encode($get_access_token_result);
            }else{
                $error_msg = '今日头条推广平台广告主'.$advertiser_id.'获取数据失败,错误信息:'.($get_access_token_result['message'] ? $get_access_token_result['message'] : '获取access_token失败');
                DataImportImp::saveDataErrorLog(1,'ptg66','今日头条',4,$error_msg);
                $error_msg_arr[] = $error_msg;
                CommonFunction::sendMail($error_msg_arr,'推广平台取数error');
                return false;
            }
        }else{
            $error_msg = '今日头条推广平台广告主'.$advertiser_id.'获取数据失败,错误信息:access_token信息无效,请重新授权!';
            DataImportImp::saveDataErrorLog(1,'ptg66','今日头条',4,$error_msg);
            $error_msg_arr[] = $error_msg;
            CommonFunction::sendMail($error_msg_arr,'推广平台取数error');
            return false;
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

    public static function get_data($url,$access_token)
    {
        $headers = array('Access-Token: '.$access_token);
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);

        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_HEADER, 0);
        curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
        curl_setopt($ch, CURLOPT_TIMEOUT,120); //超时时间  秒
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, FALSE);
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, FALSE);
        curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);

        $output = curl_exec($ch);
        curl_close($ch);
        return $output;
    }
}
