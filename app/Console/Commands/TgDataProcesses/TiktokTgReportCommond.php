<?php

namespace App\Console\Commands\TgDataProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\Common\CurlRequest;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\DataImportLogic;
use Illuminate\Support\Facades\Artisan;
use App\Common\CommonFunction;
use Illuminate\Support\Facades\Redis;

class TiktokTgReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'TiktokTgReportCommond {dayid?} ';

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
        Redis::select(1);
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

        define('AD_PLATFORM', 'TikTok');
        define('SCHEMA', 'tg_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'ptg76'); // todo 这个需要根据平台信息表确定平台ID

        $tiktok_conf_arr = [
            'username' => 'promtion@zplay.com', 'pass' => 'Zpl@y1119', 'app_id' => 1648343684797446, 'secret' => '6c566e4b401d6fb14a0418e07eb30abd1681e202'
        ];

        self::getAdvertiserList($tiktok_conf_arr,$dayid);
    }

    //获取 Advertiser List
    private static function getAdvertiserList($tiktok_conf_arr,$dayid){

        // todo 此处为生成access_token代码
        // self::getAccessToken($tiktok_conf_arr);

        $account_name = $tiktok_conf_arr['username'];
//        $access_token = self::refreshToken($tiktok_conf_arr);
        Redis::select(1);
        $access_json = Redis::get('tiktok_tg_access_token');
        if ($access_json) {
            $content_arr = json_decode($access_json, true);
            $access_token = $content_arr['data']['access_token'];
            $app_id = $tiktok_conf_arr['app_id'];
            $secret = $tiktok_conf_arr['secret'];

            // 获取advertiser id信息
            $data_url = "https://ads.tiktok.com/open_api/oauth2/advertiser/get/?access_token={$access_token}&app_id={$app_id}&secret={$secret}";

            $data = self::get_response($data_url);
            $data_arr = json_decode($data, true);

            // 数据获取重试
            $api_data_i = 1;
            while(!$data_arr){
                $data = self::get_response($data_url);
                $data_arr = json_decode($data, true);
                $api_data_i++;
                if($api_data_i > 3)
                    break;
            }

            if (!empty($data_arr['data']['list'])) {
                $tiktok_advertiser_list = $data_arr['data']['list'];

                //删除数据库里原来数据
                $map['dayid'] = $dayid;
                $map['source_id'] = SOURCE_ID;
//                $map['account'] = $account_name;
                //删除数据
                $count = DataImportLogic::getChannelData('tg_data','erm_data',$map)->count();
                if($count>0){
                    DataImportLogic::deleteHistoryData('tg_data', 'erm_data', $map);
                }
                foreach ($tiktok_advertiser_list as $tiktok_advertiser_info) {
                    $advertiser_id = $tiktok_advertiser_info['advertiser_id'];
                    $advertiser_name = $tiktok_advertiser_info['advertiser_name'];
                    self::getAdIds($account_name, $advertiser_id, $advertiser_name, $dayid, $access_token);
                }
                // 数据处理过程
                Artisan::call('TiktokTgHandleProcesses',['dayid'=>$dayid]);
            }else{
                $error_msg = AD_PLATFORM.'推广平台'.'获取数据失败,错误信息:'. (isset($data_arr['message']) ? $data_arr['message'] : '广告主列表获取失败!');;
                DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,4,$error_msg);
            }
        }else{
            $error_msg = AD_PLATFORM.'推广平台'.'获取数据失败,错误信息:授权失败,access_token信息不存在,请重新授权!';
            DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,4,$error_msg);
        }
    }

    // 获取广告ID数据
    public static function getAdIds($data_account,$advertiser_id,$advertiser_name,$dayid, $access_token){

        // 获取ad id信息
        $data_url = "https://ads.tiktok.com/open_api/2/campaign/get/?advertiser_id={$advertiser_id}&page_size=1000";
        $data = self::getContent($data_url, $access_token);
        $data_arr = json_decode($data, true);
        if (!empty($data_arr['data']['list'])) {
            foreach ($data_arr['data']['list'] as $ad_info){
                $campaign_name = $ad_info['campaign_name'];
                $campaign_id = $ad_info['campaign_id'];
//                $adgroup_name = $ad_info['adgroup_name'];
//                $adgroup_id = $ad_info['adgroup_id'];
//                $ad_name = $ad_info['ad_name'];
//                $ad_id = $ad_info['ad_id'];
                self::getTiktokData($access_token,$data_account,$advertiser_id, $advertiser_name, $campaign_name, $campaign_id,$dayid);
            }
        }
    }

    // 获取数据
    public static function getTiktokData($access_token,$data_account,$advertiser_id, $advertiser_name, $campaign_name, $campaign_id,$dayid)
    {
        // 获取数据
        $campaign_ids = json_encode(array($campaign_id));
//        $data_url = "https://ads.tiktok.com/open_api/2/audience/ad/get/?advertiser_id={$advertiser_id}&ad_ids={$ad_ids}&start_date={$dayid}&end_date={$dayid}&page_size=1000&dimension=COUNTRY";
        $data_url = "https://ads.tiktok.com/open_api/2/audience/campaign/get/?advertiser_id={$advertiser_id}&campaign_ids={$campaign_ids}&start_date={$dayid}&end_date={$dayid}&page_size=1000&dimension=COUNTRY";
        var_dump($data_url);
        $data = self::getContent($data_url, $access_token);
        var_dump($data);
        $data_arr = json_decode($data, true);
        if (!empty($data_arr['data']['list'])) {
            $final_insert_arr = [];
            foreach ($data_arr['data']['list'] as $toutiao_data) {
                $insert_data = [];
                if (isset($toutiao_data['metrics']) && $toutiao_data['metrics']){
                    $insert_data = array_merge($insert_data,$toutiao_data['metrics']);
                }
                if (isset($toutiao_data['dimensions']) && $toutiao_data['dimensions']){
                    $insert_data = array_merge($insert_data,$toutiao_data['dimensions']);
                }

                $insert_data['account_name'] = $data_account;
                $insert_data['advertiser_id'] = $advertiser_id;
                $insert_data['advertiser_name'] = $advertiser_name;
                $insert_data['campaign_name'] = $campaign_name;
                $insert_data['campaign_id'] = $campaign_id;
//                $insert_data['adgroup_name'] = $adgroup_name;
//                $insert_data['adgroup_id'] = $adgroup_id;
//                $insert_data['ad_name'] = $ad_name;
//                $insert_data['ad_id'] = $ad_id;
                $insert_data['dayid'] = $dayid;
                $final_insert_arr[] = $insert_data;
            }

            self::getReportData($final_insert_arr, $data_account, $dayid);
        }
    }

    public static function getReportData($data , $data_account , $dayid){
        //这里面要写新测试平台里的数据配置 从数据库里取数据
        if ($data && $data_account && $dayid){

            $index =0;
            $insert_data =[];
            $step =[];
//            var_dump(count($data));
            foreach ($data as $v){

                $v['account_name'] = isset($v['account_name']) ? addslashes($v['account_name']) : '';
                $v['advertiser_name'] = isset($v['advertiser_name']) ? addslashes($v['advertiser_name']) : '';
                $v['campaign_name'] = isset($v['campaign_name']) ? addslashes($v['campaign_name']) : '';
//                $v['adgroup_name'] = isset($v['adgroup_name']) ? addslashes($v['adgroup_name']) : '';
//                $v['ad_name'] = isset($v['ad_name']) ? addslashes($v['ad_name']) : '';
                $v['account'] = $data_account;

                $insert_data[$index]['campaign_id'] = $v['campaign_id'];
                $insert_data[$index]['campaign_name'] = $v['campaign_name'];
                $insert_data[$index]['cost'] = $v['stat_cost'];

                $insert_data[$index]['account'] = $data_account;
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
                if ($kkkk % 1000 == 0) $i++;
                if ($insert_data_info) {
                    $step[$i][] = $insert_data_info;
                }
            }

            if ($step) {
                foreach ($step as $k => $v) {
                    $result = DataImportLogic::insertChannelData('tg_data','erm_data',$v);
                    if (!$result) {
                        echo 'mysql_error'. PHP_EOL;
                    }
                }
            }
        }
    }

    // 获取access_token
    public static function getAccessToken($tiktok_conf_arr){
        // todo 此处为生成access_token代码
        $account_name = $tiktok_conf_arr['username'];
        $token_file = "./tiktok_".$account_name.".txt";
        $get_access_token_data = [
            'app_id'=>$tiktok_conf_arr['app_id'],
            'secret'=>$tiktok_conf_arr['secret'],
            'grant_type'=>'auth_code',
            'auth_code'=>'4935cfd496259af0f52d74540036b11730bd2ca8'
        ];
        var_dump($get_access_token_data);
        $get_access_token_url = "https://ads.tiktok.com/open_api/oauth2/access_token/";
        $get_access_token_result = CurlRequest::curl_header_json_Post($get_access_token_url, $get_access_token_data,[]);
        var_dump($get_access_token_result);
        file_put_contents($token_file, $get_access_token_result);
    }

    // 刷新token
    private static function refreshToken($tiktok_conf_arr){

        $account_name = $tiktok_conf_arr['username'];
        $token_file = "./tiktok_".$account_name.".txt";
        $app_id = $tiktok_conf_arr['app_id'];
        $secret = $tiktok_conf_arr['secret'];

        if (file_exists($token_file)) {
            $content = file_get_contents($token_file);
            $content_arr = json_decode($content, true);
            if (isset($content_arr['data']) && $content_arr['data']){
                $content_arr_data = $content_arr['data'];
                if (isset($content_arr_data['refresh_token']) && $content_arr_data['refresh_token']){
                    $refresh_token = $content_arr_data['refresh_token'];
                    $refresh_token_data = [
                        'app_id' => $app_id,
                        'secret' => $secret,
                        'grant_type' => 'refresh_token',
                        'refresh_token' => $refresh_token
                    ];
                    $get_access_token_url = "https://ads.tiktok.com/open_api/oauth2/refresh_token/";
                    $get_access_token_result = CurlRequest::curl_header_json_Post($get_access_token_url, $refresh_token_data,[]);
                    var_dump($get_access_token_result);
                    if($get_access_token_result){
                        file_put_contents($token_file, $get_access_token_result);
                        $get_access_token_arr = json_decode($get_access_token_result, true);
                        if (isset($get_access_token_arr['data']) && isset($get_access_token_arr['data']['access_token']) && $get_access_token_arr['data']['access_token']) {
                            $access_token = $get_access_token_arr['data']['access_token'];
                            var_dump($access_token);
                            return $access_token;
                        }else{
                            return false;
                        }
                    }else{
                        return false;
                    }
                }else{
                    return false;
                }
            }else{
                return false;
            }
        }else{
            return false;
        }

    }

    // 重试机制 获取数据内容
    public static function getContent($url, $access_token)
    {
        static $degree = 0;
        // echo $url . PHP_EOL;
        $content = self::get_data($url, $access_token);
        // echo PHP_EOL;

        if (!$content) {
            if ($degree > 1) {
                return false;
            }
            $degree++;
            sleep(2);
            return self::get_data($url, $access_token);
        }

        return $content;
    }



    public static function get_data($url, $access_token)
    {
        $headers = array('Access-Token: ' . $access_token);
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);

        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_HEADER, 0);
        curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
        curl_setopt($ch, CURLOPT_TIMEOUT, 30); //超时时间  秒
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, FALSE);
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, FALSE);
        curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);

        $output = curl_exec($ch);
        curl_close($ch);
        return $output;
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
