<?php

namespace App\Console\Commands;

use App\BusinessLogic\AdReportLogic;
use App\Common\CurlRequest;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class ZplayAdsReportCommondBak extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'ZplayAdsReportCommondBak';

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

        // 接口文档 https://github.com/zplayads/report_api/blob/master/%E5%BC%80%E5%8F%91%E8%80%85Report%20API.md

        $developer_account_id = env("DEVELOPER_ACCOUNT_ID"); // 开发者账号
        $base_url = env("DEVELOPER_REPORT_URL");
        $url = $base_url . "/developer/{$developer_account_id}/apps";
        $params = self::getSignature();
        $url .= '?' . http_build_query($params, null, '&');
        $result = CurlRequest::getContent($url);
        if (!$result){
            Log::error("开发者账号{$developer_account_id}获取应用列表失败！");
            exit;
        }
        $app_list = json_decode($result, true);
        if (isset($app_list['error']) && $app_list['error'] == '' && $app_list['data']){
            // 应用数据处理
            $app_list_info = $app_list['data'];
            foreach ($app_list_info as $app_info){
                $app_id = $app_info['app_id'];
                $app_name = $app_info['name'];
                $app_os = $app_info['os'];
                self::getAdUnits($developer_account_id, $app_id, $base_url);
            }
        }
    }


    // 获取广告位列表信息
    private static function getAdUnits($developer_account_id, $app_id, $base_url){

        $url = $base_url . "/developer/{$developer_account_id}/app/{$app_id}/ad_units";
        $params = self::getSignature();
        $url .= '?' . http_build_query($params, null, '&');
        $result = CurlRequest::getContent($url);
        if ($result) {
            $ad_units_list = json_decode($result, true);
            if (isset($ad_units_list['error']) && $ad_units_list['error'] == '' && $ad_units_list['data']) {
                // 应用数据处理
                $ad_units_info_list = $ad_units_list['data'];
                foreach ($ad_units_info_list as $ad_units_info) {
                    $ad_type = $ad_units_info['ad_type'];
                    $ad_unit_id = $ad_units_info['ad_unit_id'];
                    $app_id = $ad_units_info['app_id'];
                    $name = $ad_units_info['name'];
                    self::getTotalNumber($developer_account_id, $app_id, $base_url, $ad_type);
                }
            }
        }else{
            Log::error("获取{$app_id}应用的统计第{$page}页数据列表失败！！");
            exit;
        }
    }

    // 获取应用的总记录条数 计算页数
    private static function getTotalNumber($developer_account_id, $app_id, $base_url,$ad_type){

        $url = $base_url . "/developer/{$developer_account_id}/app/{$app_id}/stats";
        $params = self::getSignature();
        $params['start_date'] = date('Ymd' , time()-86400);
        $params['end_date'] = date('Ymd',time()-86400);
        $params['group_dimension'] = "country";

        $url .= '?' . http_build_query($params, null, '&');
        $result = CurlRequest::getContent($url);
        if ($result) {
            $total_data = json_decode($result, true);
            if (isset($total_data['error']) && $total_data['error'] == '' && isset($total_data['data']) && $total_data['data']) {
                $total_info = $total_data['data'];
                if (isset($total_info['total']) && $total_info['total']) {
                    $total = $total_info['total'];
                    $pages = ceil($total / 20);
                    if ($pages) {
                        for ($page = 1; $page <= $pages; $page++) {
                            self::getAdStats($developer_account_id, $app_id, $base_url, $ad_type, $page);
                        }
                    }
                }

            }
        }else{
            Log::error("获取{$app_id}列表数据总数失败！");
        }
    }

    // 获取应用的统计数据列表
    private static function getAdStats($developer_account_id, $app_id, $base_url,$ad_type,$page){

        $url = $base_url . "/developer/{$developer_account_id}/app/58D73C7A-D989-6C87-19EB-08DFDFFC2C6E/stats";
        $params = self::getSignature();
        $start_date = date('Ymd' , time()-86400);
        $params['start_date'] = $start_date;
        $end_date = date('Ymd',time()-86400);
        $params['end_date'] = $end_date;
        $params['group_dimension'] = "country";
        $params['page'] = $page;
        $params['size'] = 20;

        $url .= '?' . http_build_query($params, null, '&');

        $result = CurlRequest::getContent($url);
        if ($result) {
            $ad_stats_data = json_decode($result, true);
            if (isset($ad_stats_data['error']) && $ad_stats_data['error'] == '' && isset($ad_stats_data['data']) && $ad_stats_data['data']) {
                $ad_stats = $ad_stats_data['data'];
                if (isset($ad_stats['list']) && $ad_stats['list']) {
                    $ad_stats_list = $ad_stats['list'];

                    foreach ($ad_stats_list as $key => $ad_stats) {
                        $ad_stats_list[$key]['ad_type'] = $ad_type;
                    }
                    $result = AdReportLogic::insertAdStats($ad_stats_list);
                    // 数据库插入数据失败
                    if (!$result) {
                        Log::error("日期{$start_date}-{$start_date},保存{$app_id}应用的统计第{$page}页数据列表失败！");
                    }
                }
            }
        }else{
            Log::error("获取{$app_id}应用的统计第{$page}页数据列表失败！！");
        }
    }

    // 获取签名信息
    private static function getSignature(){

        $token = env("REPORT_API_KEY"); // API Key
        // 生成签名
        $timestamp = time();
        $nonce = rand(1,999);
        $tmpArr = array($token, $timestamp, $nonce);
        sort($tmpArr, SORT_STRING);
        $signature = implode( $tmpArr );
        $signature = sha1( $signature );

        $params = [
            'signature' => $signature,
            'timestamp' => $timestamp,
            'nonce' => $nonce,
        ];

        return $params;

    }
}
