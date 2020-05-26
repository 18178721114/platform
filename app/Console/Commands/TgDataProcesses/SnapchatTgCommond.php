<?php

namespace App\Console\Commands\TgDataProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\BusinessLogic\DataImportLogic;
use App\Common\CurlRequest;
use App\Common\ParseDayid;
use App\Common\Service;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Artisan;
use App\Common\CommonFunction;
use Illuminate\Support\Facades\Redis;

class SnapchatTgCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'SnapchatTgCommond {dayid?} {account?}';

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
        date_default_timezone_set('Europe/London');

        define('AD_PLATFORM', 'SnapchatTg');
        define('SCHEMA', 'tg_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'ptg75'); // todo 这个需要根据平台信息表确定平台ID

        $dayid = $this->argument('dayid');
        $account = $this->argument('account');
        $date = ParseDayid::get_dayid($dayid);

        $message = "date: $date";
        self::saveLog(AD_PLATFORM, $message);

        $start_date = date('Y-m-d',strtotime($date) - 86400);
        $end_date= $date;

        // 获取Sino代理token
        $sino_api_url = 'https://adsapi.snapchat.com/v1';
        self::getSinoSnapchatToken($sino_api_url,$start_date, $end_date);

        // 获取百度代理token
//        $baidu_organization_id = 'fbbf7671-924a-4abf-adaa-a8c67f44fae9';
//        $baidu_api_url = 'https://mediago.baidu.com/api/ads/?sf=v1';
//        self::getBaiduSnapchatToken($baidu_api_url,$baidu_organization_id,$start_date, $end_date);

        // 数据处理
        Artisan::call('SnapchatTgHandleProcesses',['dayid'=>$end_date]);

    }

    // 获取Sino代理token
    public static function getSinoSnapchatToken($sino_api_url,$start_date, $end_date){
        $access_json = Redis::get('snapchat_sino_access_token');
        $sino_access_token = '';
        if ($access_json) {
            $content_arr = json_decode($access_json, true);
            // todo 此处为生成access_token代码
            $get_access_token_data = [
                'client_id'=>'69da582e-40f7-4284-a94f-b9eecd3d85b0',
                'client_secret'=>'3d5f45d44d3d89ca2b2e',
                'grant_type'=>'refresh_token',
                'code' => $content_arr['refresh_token']
            ];
            $get_access_token_url = "https://accounts.snapchat.com/login/oauth2/access_token";
            $get_access_token_result = CurlRequest::curl_header_Post($get_access_token_url, $get_access_token_data,[]);
            $get_access_token_result = json_decode($get_access_token_result,true);
            $token_res_i=1;
            while(!$get_access_token_result || !isset($get_access_token_result['access_token'])){
                $get_access_token_result = CurlRequest::curl_header_Post($get_access_token_url, $get_access_token_data,[]);
                $get_access_token_result = json_decode($get_access_token_result,true);

                $token_res_i++;
                if($token_res_i>2)
                    break;
            }
            if ($get_access_token_result && isset($get_access_token_result['access_token'])) {
                $sino_access_token = $get_access_token_result['access_token'];
                Redis::set('snapchat_sino_access_token', json_encode($get_access_token_result));
            }else{
                $error_msg = 'Snapchat推广平台Sino代理获取数据失败,错误信息:获取access_token失败';
                DataImportImp::saveDataErrorLog(1,'ptg75','Snapchat',4,$error_msg);
            }
        }
//        var_dump($sino_access_token);
        if ($sino_access_token){
            $organization_ids = ['16412453-e008-4353-a8da-881ed5170e9c','fbbf7671-924a-4abf-adaa-a8c67f44fae9'] ;
            foreach ($organization_ids as $organization_id){
                $header = array('Authorization: Bearer ' . $sino_access_token);
                self::getAdAccounts($sino_api_url,$organization_id, $header,$start_date, $end_date);
            }

        }
    }

    // 获取百度代理token
    public static function getBaiduSnapchatToken($baidu_api_url,$baidu_organization_id,$start_date, $end_date){
        // todo 需要改为正式查询
//        $sql = " select distinct a.platform_id,a.data_account as company_account,b.token as token from c_platform_account_mapping a left join c_generalize b on b.platform_id = a.platform_id and a.account = b.data_account where a.platform_id = 'ptg75' ";
        $sql = " select distinct platform_id,data_account as company_account,account_token as token from c_platform_account_mapping where platform_id = 'ptg75' ";
        $info = DB::select($sql);
        $info = Service::data($info);
        if (!$info) return;

        foreach ($info as $key => $value) {
            $refresh_token = $value['token'];
//            $refresh_token = "eyJraWQiOiJyZWZyZXNoLXRva2VuLWExMjhnY20uMCIsInR5cCI6IkpXVCIsImVuYyI6IkExMjhHQ00iLCJhbGciOiJkaXIifQ..FyQiPixWHZMWyRPu.HTN0ERNwyK5fQt1rwq82c1n_274VkhSObClWqBst-Xs1mnNlCT609H0c81we__JWH6g-Wv8pDntK1w8Gr3lR7WR1M5Qb_yahoERGULph66aMRlU0CBW9mD8OTHiHQ3F5xcIRq23EeAdQqCyof_I9WcPvoNYtHw3RWU39Vcfhwo21yguhI0CDR-wyyAgchWuqbVs6BEF3-MMa1wbepCooD1z70cfNmUt8uH1ScKOTiySvtxxsYRumshw8PH9vjn_K8MJNz5lPqEuuHUA.jVOgUPkI7A8pYgsGPdGDjw";

            $refresh_token_data = array('grant_type' => 'refresh_token', 'code' => $refresh_token,);
            $get_access_token_url = "https://mediago.baidu.com/api/refreshtoken";
            $get_access_token_result = self::curl_post_https($get_access_token_url, $refresh_token_data);
            if ($get_access_token_result) {
                $get_access_token_result = json_decode($get_access_token_result, true);
                $access_token = isset($get_access_token_result['access_token']) ? $get_access_token_result['access_token'] : '';
                if ($access_token) {
                    $header = array('Authorization: Bearer ' . $access_token);
                    self::getAdAccounts($baidu_api_url,$baidu_organization_id,$header, $start_date, $end_date);

                } else {
                    $error_msg = AD_PLATFORM . '推广平台获取access_token失败';
                    DataImportImp::saveDataErrorLog(1, SOURCE_ID, AD_PLATFORM, 4, $error_msg);
                    $error_msg_arr[] = $error_msg;
                    CommonFunction::sendMail($error_msg_arr, AD_PLATFORM . '推广平台取数error');
                }
            }
        }
    }

    // 获取账户ID信息
    public static function getAdAccounts($api_url,$organization_id, $header, $start_date, $end_date){
        $url = $api_url."/organizations/{$organization_id}/adaccounts";
        $adaccounts_res = self::get_response($url, $header);
        $adaccounts_res_i=1;
        while(!$adaccounts_res){
            $adaccounts_res = self::get_response($url, $header);

            $adaccounts_res_i++;
            if($adaccounts_res_i>2)
                break;
        }
        if ($adaccounts_res){
            $adaccounts_res = json_decode($adaccounts_res, true);
            if (isset($adaccounts_res['adaccounts']) && $adaccounts_res['adaccounts']){
                $adaccounts_list = $adaccounts_res['adaccounts'];
                foreach ($adaccounts_list as $adaccounts){
                    if (isset($adaccounts['adaccount']) && $adaccounts['adaccount']){
                        $adaccount = $adaccounts['adaccount'];
                        $adaccount_id = $adaccount['id'];
                        $adaccount_name = $adaccount['name'];
                        // 查询删除数据
                        $map = [];
                        $map['dayid'] = $end_date;
                        $map['source_id'] = SOURCE_ID;
                        $map['account'] = $adaccount_id;
                        $count = DataImportLogic::getChannelData('tg_data', 'erm_data', $map)->count();
                        if ($count > 0) {
                            $bool = DataImportLogic::deleteHistoryData(SCHEMA, TABLE_NAME, $map);
                        }

                        self::getCampaigns($api_url, $organization_id, $adaccount_name, $adaccount_id, $header, $start_date, $end_date);
                    }
                }
            }
        }
    }

    // 获取campaign id信息
    public static function getCampaigns($api_url,$organization_id,$adaccount_name,$adaccount_id, $header, $start_date, $end_date){
        $url = $api_url."/adaccounts/{$adaccount_id}/campaigns?limit=100";
        $campaigns_res = self::get_response($url, $header);
        $campaigns_res_i=1;
        while(!$campaigns_res){
            $campaigns_res = self::get_response($url, $header);
            $campaigns_res_i++;
            if($campaigns_res_i>2)
                break;
        }

        if ($campaigns_res){
            $campaigns_res = json_decode($campaigns_res, true);
            if (isset($campaigns_res['campaigns']) && $campaigns_res['campaigns']){
                $campaigns_res = $campaigns_res['campaigns'];
                foreach ($campaigns_res as $campaigns){
                    if (isset($campaigns['campaign']) && $campaigns['campaign']){
                        $campaign = $campaigns['campaign'];
                        $campaign_id = $campaign['id'];
                        $campaign_name = $campaign['name'];
                        self::getAdSquads($api_url,$organization_id,$adaccount_name,$adaccount_id,$campaign_id,$campaign_name, $header, $start_date, $end_date);
                    }
                }
            }
        }
    }


    // 获取adsquads id 信息
    public static function getAdSquads($api_url,$organization_id,$adaccount_name,$adaccount_id,$campaign_id,$campaign_name, $header, $start_date, $end_date){
        $url = $api_url."/campaigns/{$campaign_id}/adsquads?limit=100";
        $adsquads_res = self::get_response($url, $header);
        $adsquads_res_i=1;
        while(!$adsquads_res){
            $adsquads_res = self::get_response($url, $header);
            $adsquads_res_i++;
            if($adsquads_res_i>2)
                break;
        }
        if ($adsquads_res){
            $adsquads_res = json_decode($adsquads_res, true);
            if (isset($adsquads_res['adsquads']) && $adsquads_res['adsquads']){
                $adsquads_res = $adsquads_res['adsquads'];
                foreach ($adsquads_res as $adsquads){
                    if (isset($adsquads['adsquad']) && $adsquads['adsquad']){
                        $adsquads = $adsquads['adsquad'];
                        $adsquads_id = $adsquads['id'];
                        $adsquads_name = $adsquads['name'];
//                        self::getAds($organization_id,$adaccount_name,$adaccount_id,$campaign_id,$campaign_name,$adsquads_id,$adsquads_name, $header, $start_date, $end_date);
                        self::getAdReport($api_url,$organization_id,$adaccount_name,$adaccount_id,$campaign_id,$campaign_name,$adsquads_id,$adsquads_name, $header, $start_date, $end_date);
                    }
                }
            }
        }
    }

    // 获取adsquads id 信息
    public static function getAdReport($api_url,$organization_id,$adaccount_name,$adaccount_id,$campaign_id,$campaign_name,$adsquads_id,$adsquads_name, $header, $start_date, $end_date)
    {

//        // todo 夏令时取数采用
//        if (strtotime($end_date) >= strtotime('2020-03-08') && strtotime($end_date) <= strtotime('2020-11-01')){
//            $start_date_z = $start_date . "T23:00:00Z";
//            $end_date_z = $end_date . "T23:00:00Z";
//        }elseif ($end_date == '2020-11-02'){
//            // todo 东夏令时切换，特殊处理，取数采用
//            $start_date_z = $start_date . "T23:00:00Z";
//            $end_date_z = date('Y-m-d',strtotime($end_date)+2*86400) . "T00:00:00Z";
//        }elseif($end_date == '2020-11-03'){
//            // todo 东令时取数采用
//            $start_date_z = date('Y-m-d',strtotime($start_date)+2*86400) . "T00:00:00Z"; // 28号
//            $end_date_z = date('Y-m-d',strtotime($end_date)+86400) . "T00:00:00Z";
//        }elseif(strtotime($end_date) >= strtotime('2019-10-29')) {
//            $start_date_z = date('Y-m-d', strtotime($start_date) + 86400) . "T00:00:00Z"; // 29号之后
//            $end_date_z = date('Y-m-d',strtotime($end_date)+86400) . "T00:00:00Z";
//        }

        // todo 夏令时取数采用
        if ($adaccount_id == 'dee65c97-c782-4a7f-8629-0a1c1c4a78ad' || $adaccount_id == '6dc6bb62-d47a-4b49-84ff-d85a4f824859' || $adaccount_id == 'dbf5808a-7342-49ac-890b-f834b4b003bc'){
            if (strtotime($end_date) >= strtotime('2020-04-08') && strtotime($end_date) <= strtotime('2020-11-01')){
                // todo 夏令时切换，特殊处理，取数采用
                $start_date_z = $start_date . "T23:00:00Z";
                $end_date_z = $end_date . "T23:00:00Z";

//                var_dump($start_date_z);
//                var_dump($end_date_z);
            }
        }else {
            if (strtotime($end_date) == strtotime('2020-03-30')) {
                // todo 夏令时切换，特殊处理，取数采用
                $start_date_z = date('Y-m-d', strtotime($start_date) + 2 * 86400) . "T00:00:00Z"; // 29号之后
                $end_date_z = date('Y-m-d', strtotime($end_date) + 86400) . "T00:00:00Z";
            } elseif (strtotime($end_date) <= strtotime('2020-10-31')) {
                // todo 夏令时切换，特殊处理，取数采用
                $start_date_z = date('Y-m-d', strtotime($start_date) + 86400) . "T00:00:00Z"; // 29号之后
                $end_date_z = date('Y-m-d', strtotime($end_date) + 86400) . "T00:00:00Z";
            } elseif (strtotime($end_date) >= strtotime('2020-11-01') && strtotime($end_date) <= strtotime('2020-11-01')) {
                // todo 夏令时切换，特殊处理，取数采用
                $start_date_z = $start_date . "T23:00:00Z";
                $end_date_z = $end_date . "T23:00:00Z";
            } elseif ($end_date == '2020-11-02') {
                // todo 冬令时切换，特殊处理，取数采用
                $start_date_z = $start_date . "T23:00:00Z";
                $end_date_z = date('Y-m-d', strtotime($end_date) + 2 * 86400) . "T00:00:00Z";
            } elseif ($end_date == '2020-11-03') {
                // todo 冬令时取数采用
                $start_date_z = date('Y-m-d', strtotime($start_date) + 2 * 86400) . "T00:00:00Z"; // 28号
                $end_date_z = date('Y-m-d', strtotime($end_date) + 86400) . "T00:00:00Z";
            } elseif (strtotime($end_date) >= strtotime('2020-11-04')) {
                // todo 冬令时切换，特殊处理，取数采用
                $start_date_z = date('Y-m-d', strtotime($start_date) + 86400) . "T00:00:00Z"; // 29号之后
                $end_date_z = date('Y-m-d', strtotime($end_date) + 86400) . "T00:00:00Z";
            }
        }


//        var_dump($start_date_z);
//        var_dump($end_date_z);

        $url = $api_url."/adsquads/{$adsquads_id}/stats?granularity=TOTAL&start_time={$start_date_z}&end_time={$end_date_z}&fields=impressions,swipes,screen_time_millis,quartile_1,quartile_2,quartile_3,view_completion,spend,video_views,ios_installs,android_installs,total_installs";

        $ads_stats_res = self::get_response($url, $header);
        $ads_stats_res_i=1;
        while(!$ads_stats_res){
            $ads_stats_res = self::get_response($url, $header);
            $ads_stats_res_i++;
            if($ads_stats_res_i>2)
                break;
        }

        if ($ads_stats_res){
            $ads_stats_res = json_decode($ads_stats_res, true);
            if (isset($ads_stats_res['total_stats']) && $ads_stats_res['total_stats']){
                $ads_stats = $ads_stats_res['total_stats'];

                $all_data = [];
                foreach ($ads_stats as $ads_stat){
                    if (isset($ads_stat['total_stat']) && $ads_stat['total_stat']){
                        $total_stat = $ads_stat['total_stat'];
                        $insert_data = array();
                        $insert_data['organization_id'] = $organization_id;
                        $insert_data['adaccount_id'] = $adaccount_id;
                        $insert_data['adaccount_name'] = $adaccount_name;
                        $insert_data['campaign_id'] = $campaign_id;
                        $insert_data['campaign_name'] = $campaign_name;
                        $insert_data['adsquads_id'] = $adsquads_id;
                        $insert_data['adsquads_name'] = $adsquads_name;
//                        $insert_data['ad_name'] = $ads_name;
//                        $insert_data['ad_id'] = $total_stat['id'];
                        $insert_data['type'] = $total_stat['type'];
                        $insert_data['start_time'] = $total_stat['start_time'];
                        $insert_data['end_time'] = $total_stat['end_time'];
                        $insert_data['finalized_data_end_time'] = $total_stat['finalized_data_end_time'];
                        if (isset($total_stat['stats']) && $total_stat['stats']){
                            $stat = $total_stat['stats'];
                            $insert_data['impressions'] = $stat['impressions'];
                            $insert_data['swipes'] = $stat['swipes'];
                            $insert_data['quartile_1'] = $stat['quartile_1'];
                            $insert_data['quartile_2'] = $stat['quartile_2'];
                            $insert_data['quartile_3'] = $stat['quartile_3'];
                            $insert_data['spend'] = $stat['spend'];
                            $insert_data['video_views'] = $stat['video_views'];
                            $insert_data['view_completion'] = $stat['view_completion'];
                            $insert_data['screen_time_millis'] = $stat['screen_time_millis'];
                            $insert_data['total_installs'] = $stat['total_installs'];
                            $insert_data['android_installs'] = $stat['android_installs'];
                            $insert_data['ios_installs'] = $stat['ios_installs'];

                            $all_data[] = $insert_data;
                        }
                    }
                }

                self::insertData($all_data,$end_date,$organization_id,$adaccount_id);
            }
        }
    }

    // 保存到数据库
    public static function insertData($all_data,$end_date,$organization_id,$adaccount_id){

        $create_time = date("Y-m-d H:i:s");
        $insert_data = [];
        foreach ($all_data as $k => $v) {
            $insert_data[$k]['type'] = 2;
            $insert_data[$k]['app_id'] = '';
            $insert_data[$k]['app_name'] = '';
            $insert_data[$k]['account'] = $adaccount_id;
            $insert_data[$k]['source_id'] = SOURCE_ID;
            $insert_data[$k]['json_data'] = json_encode($v);
            $insert_data[$k]['dayid'] = $end_date;
            $insert_data[$k]['create_time'] = $create_time;
            $insert_data[$k]['year'] = date("Y", strtotime($end_date));
            $insert_data[$k]['month'] = date("m", strtotime($end_date));
            $insert_data[$k]['campaign_id'] = $v['campaign_id'];
            $insert_data[$k]['campaign_name'] = $v['campaign_name'];
            $insert_data[$k]['cost'] = isset($v['spend']) ? $v['spend']/1000000 : 0.00;;
        }

        //var_dump(count($insert_data));
        if ($insert_data) {

            $result = DataImportLogic::insertChannelData(SCHEMA, TABLE_NAME, $insert_data);

            if (!$result) {
                $message = "{$end_date}, 当前Snapchat数据插入失败" . date('Y-m-d H:i:s');
                self::saveLog(AD_PLATFORM, $message);
            }
        }
    }

    /* PHP CURL HTTPS POST */
    public static function curl_post_https($url,$post_data){ // 模拟提交数据函数
        $curl = curl_init(); // 启动一个CURL会话
        curl_setopt($curl, CURLOPT_URL, $url); // 要访问的地址
        curl_setopt($curl, CURLOPT_SSL_VERIFYPEER, 0); // 对认证证书来源的检查
        curl_setopt($curl, CURLOPT_SSL_VERIFYHOST, 0); // 从证书中检查SSL加密算法是否存在
//        curl_setopt($curl, CURLOPT_USERAGENT, $_SERVER['HTTP_USER_AGENT']); // 模拟用户使用的浏览器
        curl_setopt($curl, CURLOPT_FOLLOWLOCATION, 1); // 使用自动跳转
        curl_setopt($curl, CURLOPT_AUTOREFERER, 1); // 自动设置Referer
        curl_setopt($curl, CURLOPT_POST, 1); // 发送一个常规的Post请求
        curl_setopt($curl, CURLOPT_POSTFIELDS, $post_data); // Post提交的数据包
        curl_setopt($curl, CURLOPT_TIMEOUT, 30); // 设置超时限制防止死循环
        curl_setopt($curl, CURLOPT_HEADER, 0); // 显示返回的Header区域内容
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1); // 获取的信息以文件流的形式返回
        $tmpInfo = curl_exec($curl); // 执行操作
        if (curl_errno($curl)) {
            echo 'Errno'.curl_error($curl);//捕抓异常
        }
        curl_close($curl); // 关闭CURL会话
        return $tmpInfo; // 返回数据，json格式
    }

    // 重试机制 获取数据内容
    public static function getContent($url,$header)
    {
        static $degree = 0;
        echo $url . PHP_EOL;
        $content = self::get_response($url,$header);
        echo PHP_EOL;

        if (! $content) {
            if ($degree > 1) {
                return false;
            }
            $degree ++;
            sleep ( 2 );
            return self::getContent ($url,$header);
        }

        return $content;
    }

    /**
     * curl get
     * @param $url
     * @param $params
     * @param $header
     * @return string
     */
    public static function get_response($url,$header)
    {
        sleep(1);
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);

        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_HEADER, 0);
        curl_setopt($ch, CURLOPT_HTTPHEADER,$header);
        curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
        curl_setopt($ch, CURLOPT_TIMEOUT,120); //瓒堕  绉
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, FALSE);
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, FALSE);

        $output = curl_exec($ch);
        curl_close($ch);
        return $output;
    }

    // 保存日志
    private static function saveLog($platform_name = '未知', $message = ''){

        $fileName = date('Y-m-d',time());
        $dir = './storage/tgDataLogs';

        if (!is_dir($dir)) {
            mkdir($dir,0777,true);
        }
        $logFilename = $dir.'/'.$platform_name.'_tg'.'.log';
        //生成日志
        file_put_contents( $logFilename,$message . "\n\n",FILE_APPEND);
    }

}
