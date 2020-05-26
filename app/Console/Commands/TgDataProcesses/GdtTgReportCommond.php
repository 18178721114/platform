<?php

namespace App\Console\Commands\TgDataProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\BusinessLogic\DataImportLogic;
use App\Common\ApiResponseFactory;
use App\Common\CommonFunction;
use App\Common\CurlRequest;
use App\Common\ParseDayid;
use App\Common\Service;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class GdtTgReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'GdtTgReportCommond {dayid?} {advertiser?}';

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


        define('AD_PLATFORM', 'GDT');
        define('SCHEMA', 'tg_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'ptg80'); // todo 这个需要根据平台信息表确定平台ID

        $dayid = $this->argument('dayid') ? $this->argument('dayid') : date('Y-m-d',strtotime('-1 day'));
        $advertiser_id = $this->argument('advertiser') ? $this->argument('advertiser') : '';
        $date = ParseDayid::get_dayid($dayid);

        $message = "date: $date";
        self::saveLog(AD_PLATFORM, $message);

        $gdt_app_list = [
            [
                'client_id' => env('GDT_CLIENT_ID1'),
                'client_name' => env('GDT_CLIENT_NAME1'),
                'client_secret' => env('GDT_CLIENT_SECRET1'),
                'account_id' => env('GDT_ACCOUTN_ID1'),
                'advertiser_id' => env('GDT_ADVERTISER_ID1'),
                'advertiser_name' => env('GDT_ADVERTISER_NAME1'),
            ],
            [
                'client_id' => env('GDT_CLIENT_ID2'),
                'client_name' => env('GDT_CLIENT_NAME2'),
                'client_secret' => env('GDT_CLIENT_SECRET2'),
                'account_id' => env('GDT_ACCOUTN_ID2'),
                'advertiser_id' => env('GDT_ADVERTISER_ID2'),
                'advertiser_name' => env('GDT_ADVERTISER_NAME2'),
            ],
            [
                'client_id' => env('GDT_CLIENT_ID3'),
                'client_name' => env('GDT_CLIENT_NAME3'),
                'client_secret' => env('GDT_CLIENT_SECRET3'),
                'account_id' => env('GDT_ACCOUTN_ID3'),
                'advertiser_id' => env('GDT_ADVERTISER_ID3'),
                'advertiser_name' => env('GDT_ADVERTISER_NAME3'),
            ],
            [
                'client_id' => env('GDT_CLIENT_ID4'),
                'client_name' => env('GDT_CLIENT_NAME4'),
                'client_secret' => env('GDT_CLIENT_SECRET4'),
                'account_id' => env('GDT_ACCOUTN_ID4'),
                'advertiser_id' => env('GDT_ADVERTISER_ID4'),
                'advertiser_name' => env('GDT_ADVERTISER_NAME4'),
            ],
            [
                'client_id' => env('GDT_CLIENT_ID5'),
                'client_name' => env('GDT_CLIENT_NAME5'),
                'client_secret' => env('GDT_CLIENT_SECRET5'),
                'account_id' => env('GDT_ACCOUTN_ID5'),
                'advertiser_id' => env('GDT_ADVERTISER_ID5'),
                'advertiser_name' => env('GDT_ADVERTISER_NAME5'),
            ],
            [
                'client_id' => env('GDT_CLIENT_ID6'),
                'client_name' => env('GDT_CLIENT_NAME6'),
                'client_secret' => env('GDT_CLIENT_SECRET6'),
                'account_id' => env('GDT_ACCOUTN_ID6'),
                'advertiser_id' => env('GDT_ADVERTISER_ID6'),
                'advertiser_name' => env('GDT_ADVERTISER_NAME6'),
            ]
        ];

        // todo 正式数据库配置查询sql 配置需要修改
//        $sql = " select distinct a.platform_id,application_id as client_id, application_name as client_name,secret_key as client_secret,a.data_account as account_id, account_id as advertiser_id,'掌游天下(北京)' as advertiser_name from c_platform_account_mapping a left join c_generalize b on b.platform_id = a.platform_id and a.account = b.data_account where a.platform_id = 'ptg80' ";
//        $gdt_app_list = DB::select($sql);
//        $gdt_app_list = Service::data($gdt_app_list);
//        if (!$gdt_app_list) return;


        if ($advertiser_id){
            foreach ($gdt_app_list as $key => $gdt_app_info) {
                if ($gdt_app_info['advertiser_id'] == $advertiser_id){
                    self::getGdtCampaign($gdt_app_info,$date, $advertiser_id);
                }
            }
        }else{
            //获取报表数据 天报表
            foreach ($gdt_app_list as $key => $gdt_app_info){
                $advertiser_id = $gdt_app_info['advertiser_id'];
                self::getGdtCampaign($gdt_app_info,$date,$advertiser_id);
            }
        }

        // 调用数据处理过程
        Artisan::call('GdtTgHandleProcesses',['dayid' => $dayid]);
    }

    //获取campaign信息
    private static function getGdtCampaign($gdt_app_info,$date,$advertiser_id = ''){


        //删除数据库里原来数据
        $mysql_map['dayid'] = $date;
        $mysql_map['account'] = $gdt_app_info['account_id'];
        //删除数据 pgsql
        DataImportLogic::deleteGdtMysqlData($mysql_map);

        //删除数据库里原来数据
        $pgsql_map['dayid'] = $date;
        $pgsql_map['source_id'] = SOURCE_ID;
        $pgsql_map['account'] = $gdt_app_info['account_id'];
        //删除数据 pgsql
        DataImportLogic::deleteHistoryData('tg_data','erm_data',$pgsql_map);
        for ($page = 1; $page <= 5; $page++){
            $request_params = self::getCommonParams($gdt_app_info);
            $fields = json_encode(['campaign_id','campaign_name']);
            $url = env('GDT_AD_URL').'campaigns/get'.$request_params.'&account_id='.$advertiser_id.'&fields='.$fields.'&page_size=100&page='.$page;
            $result = json_decode(CurlRequest::curl_gdt_get($url),true);
            if(isset($result['data']['list']) && $result['data']['list']){
                $campaign_list = $result['data']['list'];
                //var_dump(count($campaign_list));
                foreach ($campaign_list as $campaign_key => $campaign_value){
                    $campaign_id = $campaign_value['campaign_id'];
                    $campaign_name = $campaign_value['campaign_name'];
                    self::getGdtAdgroup($gdt_app_info,$advertiser_id,$date,$campaign_id,$campaign_name);
                }
            }
        }

    }

    //获取广点通adgroupInfo
    private static function getGdtAdgroup ($gdt_app_info,$advertiser_id,$date,$campaign_id,$campaign_name){

        $request_params = self::getCommonParams($gdt_app_info);
        $fields = json_encode(['adgroup_id','adgroup_name']);
        $filtering = [
            [
                "field" => "campaign_id",
                "operator" => "EQUALS",
                "values" => [$campaign_id],
            ]
        ];
        $filtering_json = json_encode($filtering);

        $url = env('GDT_AD_URL').'adgroups/get'.$request_params.'&account_id='.$advertiser_id.'&filtering='.$filtering_json.'&fields='.$fields.'&page_size=100';
        sleep(1);
        $result = json_decode(CurlRequest::curl_gdt_get($url),true);
        if(isset($result['data']['list'])){
            $adgroup_list = $result['data']['list'];
            foreach ($adgroup_list as $adgroup_key => $adgroup_value){
                $adgroup_id = $adgroup_value['adgroup_id'];
                $adgroup_name = $adgroup_value['adgroup_name'];
                self::getReportDaily($gdt_app_info,$advertiser_id,$date,$campaign_id,$campaign_name,$adgroup_id,$adgroup_name);
            }
        }else{
            return false;
        }
    }

    //获取报表数据 天报表
    private static function getReportDaily($gdt_app_info,$advertiser_id,$date,$campaign_id,$campaign_name,$adgroup_id,$adgroup_name){

        $request_params = self::getCommonParams($gdt_app_info);
        $level = 'REPORT_LEVEL_ADGROUP'; // 获取日报表类型级别，可选值：ADVERTISER, CAMPAIGN, ADGROUP
        $date_range = ['start_date'=>$date,'end_date'=> $date];
        $date_range_json = json_encode($date_range);
        $filtering = [
            [
              "field" => "adgroup_id",
              "operator" => "EQUALS",
              "values" => [$adgroup_id],
            ]
        ];
        $filtering_json = json_encode($filtering);
        $page = 1;
        $page_size = 1000;
        $fields = json_encode(['date','view_count','valid_click_count','cost','download_count','activated_count','install_count']);
        $url = env('GDT_AD_URL').'daily_reports/get'.$request_params.'&account_id='.$advertiser_id.'&level='.$level.'&fields='.$fields.'&date_range='.$date_range_json.'&filtering='.$filtering_json.'&page='.$page.'&page_size='.$page_size;
        $groupBy = json_encode(['date','adgroup_id']);
        if($groupBy){
            $url.='&group_by='.$groupBy;
        }

        sleep(1);
        $result = CurlRequest::curl_gdt_get($url);

        $res = json_decode($result,true);
        if(!$res['code']){
            $gdt_data = $res['data']['list'];
            // mysql gdt data
            self::saveGdtMysqlData($gdt_app_info,$gdt_data,$advertiser_id,$date,$campaign_id,$campaign_name,$adgroup_id,$adgroup_name);

            // pgsql gdt data
            self::saveGdtPgsqlData($gdt_app_info,$gdt_data,$advertiser_id,$date,$campaign_id,$campaign_name,$adgroup_id,$adgroup_name);
        }else{
            $error_msg = 'GDT推广平台广告组ID为'.$adgroup_id.'取数失败,错误信息:'.$res['message'];
            DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,4,$error_msg);

            $error_msg_arr = [];
            $error_msg_arr[] = $error_msg;
            CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'推广平台取数error');
        }
    }

    // insert into mysql
    private static function saveGdtMysqlData($gdt_app_info,$gdt_data,$advertiser_id,$date,$campaign_id,$campaign_name,$adgroup_id,$adgroup_name){
        $create_time = date("Y-m-d H:i:s", time());
        $insert_data = [];
        foreach ($gdt_data as $k => $v) {
            $new_v = [];
            $new_v['account'] = $gdt_app_info['account_id'];
            $new_v['app_id'] = $gdt_app_info['client_id'];
            $new_v['app_name'] = addslashes($gdt_app_info['client_name']);
            $new_v['advertiser_id'] = $advertiser_id;
            $new_v['advertiser_name'] = addslashes('掌游天下(北京)信息技术股份有限公司');
            $new_v['campaign_id'] = $campaign_id;
            $new_v['campaign_name'] = addslashes($campaign_name);
            $new_v['adgroup_id'] = $adgroup_id;
            $new_v['adgroup_name'] = addslashes($adgroup_name);
            $new_v['impression'] = $v['view_count'];
            $new_v['click'] = $v['valid_click_count'];
            $new_v['install'] = $v['install_count'];
            $new_v['download'] = $v['download_count'];
            $new_v['activated'] = $v['activated_count'];
            $new_v['cost'] = $v['cost'];
            $new_v['dayid'] = $date;
            $new_v['create_time'] = $create_time;
            $insert_data[] = $new_v;
        }

        if ($insert_data) {
            $result = DataImportLogic::insertGdtMysqlData($insert_data);
            if (!$result) {
                $message = "{$date}, 当前GDT数据插入Mysql失败" . date('Y-m-d H:i:s');
                self::saveLog(AD_PLATFORM, $message);
            }

            $message = "{$date}, GDT接口，广告组{$adgroup_id}获取数据完成,Mysql入库条数：".count($insert_data). "，入库时间：". date('Y-m-d H:i:s');
            self::saveLog(AD_PLATFORM, $message);
        }
    }

    // insert into pgsql
    private static function saveGdtPgsqlData($gdt_app_info,$gdt_data,$advertiser_id,$date,$campaign_id,$campaign_name,$adgroup_id,$adgroup_name){

        $create_time = date("Y-m-d H:i:s", time());
        $insert_data = [];
        foreach ($gdt_data as $k => $v) {
            $new_v = [];
            $new_v['account'] = $gdt_app_info['account_id'];
            $new_v['app_id'] = $gdt_app_info['client_id'];
            $new_v['app_name'] = addslashes($gdt_app_info['client_name']);
            $new_v['advertiser_id'] = $advertiser_id;
            $new_v['advertiser_name'] = addslashes('掌游天下(北京)信息技术股份有限公司');
            $new_v['campaign_id'] = $campaign_id;
            $new_v['campaign_name'] = addslashes($campaign_name);
            $new_v['adgroup_id'] = $adgroup_id;
            $new_v['adgroup_name'] = addslashes($adgroup_name);
            $new_v['impression'] = $v['view_count'];
            $new_v['click'] = $v['valid_click_count'];
            $new_v['install'] = $v['install_count'];
            $new_v['download'] = $v['download_count'];
            $new_v['activated'] = $v['activated_count'];
            $new_v['cost'] = $v['cost'];
            $insert_data[$k]['type'] = 2;
            $insert_data[$k]['app_id'] = '';
            $insert_data[$k]['app_name'] = '';
            $insert_data[$k]['account'] = $gdt_app_info['account_id'];
            $insert_data[$k]['source_id'] = SOURCE_ID;
            $insert_data[$k]['json_data'] = json_encode($new_v);
            $insert_data[$k]['dayid'] = $date;
            $insert_data[$k]['create_time'] = $create_time;
            $insert_data[$k]['year'] = date("Y", strtotime($date));
            $insert_data[$k]['month'] = date("m", strtotime($date));
            $insert_data[$k]['campaign_id'] = $campaign_id;
            $insert_data[$k]['campaign_name'] = addslashes($campaign_name);
            $insert_data[$k]['cost'] = $v['cost'] / 100;
        }

        if ($insert_data) {
            $result = DataImportLogic::insertChannelData(SCHEMA, TABLE_NAME, $insert_data);
            if (!$result) {
                $message = "{$date}, 当前GDT数据插入Pgsql失败" . date('Y-m-d H:i:s');
                self::saveLog(AD_PLATFORM, $message);
            }

            $message = "{$date}, GDT接口，广告组{$adgroup_id}获取数据完成,Pgsql入库条数：".count($insert_data). "，入库时间：". date('Y-m-d H:i:s');
            self::saveLog(AD_PLATFORM, $message);
        }

    }

    // 获取token
    private static function getGdtToken($gdt_app_info){
        $account_id = $gdt_app_info['account_id'];
        $token_file = './'.$account_id.'.txt';
        $result = @file_get_contents($token_file);
        $token = '';
        if($result){
            $data = json_decode($result,true);
            if($data){
                if($data['expires_in']<time()){
                    if($data['refresh_token']){
                        $token = self::refreshToken($data['refresh_token'],$gdt_app_info);
                    }
                }else{
                    $token = $data['access_token'];
                }
            }
        }

        return $token;
    }

    // 生成固定请求参数
    private static function getCommonParams($gdt_app_info){

        $token = self::getGdtToken($gdt_app_info);
        if ($token){
            list($t1, $t2) = explode(' ', microtime());
            $time1 = floatval($t2);
            $time2 = round(floatval($t1)*100000);
            $time3 = uniqid();
            $nonce = md5($time1.$time2.$time3);

            $params       = '?access_token='.$token.'&timestamp='.time().'&nonce='.$nonce;
            return $params;
        }
    }

    // 刷新token
    private static function refreshToken($refresh_token,$gdt_app_info){
        $account_id = $gdt_app_info['account_id'];
        $params = [
            'grant_type'            => 'refresh_token',
            'client_id'             => $gdt_app_info['client_id'],
            'client_secret'         => $gdt_app_info['client_secret'],
            'refresh_token'         => $refresh_token
        ];
        $url                = env('GDT_OAUTH_URL').'oauth/token'. '?' . http_build_query($params, null, '&');

        $result             = CurlRequest::curl_gdt_get($url);
        $res                = json_decode($result,true);

        if($res['data']){
            $data = $res['data'];
            $token_file = "./".$account_id.".txt";
            $time = time();
            $data['expires_in'] = $time + $data['access_token_expires_in'];
            file_put_contents($token_file, json_encode($data));

            $token = $data['access_token'];
            return $token;
        }else{
            $error_msg = 'GDT推广平台'.$gdt_app_info['account_id'].'账号取数失败，失败原因：'.(isset($res['message']) ? $res['message'] : '未知错误');
            DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,4,$error_msg);

            $error_msg_arr = [];
            $error_msg_arr[] = $error_msg;
            CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'推广平台取数error');
        }
    }

    // 保存日志
    private static function saveLog($platform_name = '未知', $message = ''){

        $fileName = date('Y-m-d',time());
        $dir = './storage/tgDataLogs';

        if (!is_dir($dir)) {
            mkdir($dir,0777,true);
        }
        $logFilename = $dir.'/'.$platform_name.'_'.$fileName.'.log';
        //生成日志
        file_put_contents( $logFilename,$message . "\n\n",FILE_APPEND);
    }
}
