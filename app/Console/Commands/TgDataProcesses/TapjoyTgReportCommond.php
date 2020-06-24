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
use App\Common\Service;
use App\Common\CommonFunction;

class TapjoyTgReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'TapjoyTgReportCommond {dayid?} {appid?}';

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
        $dayid = $this->argument('dayid') ? $this->argument('dayid') : date('Y-m-d', strtotime('-1 day'));
        $appid = $this->argument('appid') ? $this->argument('appid') : '';
        var_dump($dayid);

        define('AD_PLATFORM', 'tapjoy');
        define('SCHEMA', 'tg_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'ptg67'); // todo 这个需要根据平台信息表确定平台ID

        try {
            $sql = "  select distinct platform_id,data_account as company_account,account_api_key as api_key from c_platform_account_mapping where platform_id = 'ptg67' ";
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

            foreach ($info as $kk => $vv) {
                $company_account = $vv['company_account'];
                $app['api_key'] = $vv['api_key'];
                $access_token_header = array("authorization:Basic " . $app['api_key'], "accept: application/json; */*",);
                $token_url = env('TAPJOY_TOKEN_URL');
                $response = self::curl($token_url, 'POST', $access_token_header);
                $response = json_decode($response, true);

                // 数据获取重试
                $api_data_i = 1;
                while (!$response) {
                    $response = self::curl($token_url, 'POST', $access_token_header);
                    $response = json_decode($response, true);
                    $api_data_i++;
                    if ($api_data_i > 3) break;
                }

                if (isset($response['error']) || !$response) {
                    $error_msg = AD_PLATFORM . '推广平台' . $company_account . '账号获取access_token失败,错误信息:' . (isset($response['error']) ? $response['error'] : '无数据，接口未返回任何信息');
                    DataImportImp::saveDataErrorLog(1, SOURCE_ID, AD_PLATFORM, 4, $error_msg);
                }else{
                    $access_token = $response['access_token'];
                    $data_header = array("accept: application/json; */*", "authorization: Bearer  {$access_token}",);

                    //删除数据库里原来数据
                    $map['dayid'] = $dayid;
                    $map['source_id'] = SOURCE_ID;
                    $map['account'] = $company_account;
                    $count = DataImportLogic::getChannelData('tg_data', 'erm_data', $map)->count();
                    if ($count > 0) {
                        //删除数据
                        DataImportLogic::deleteHistoryData('tg_data', 'erm_data', $map);
                    }

                    // todo 获取广告集合
                    $post_url = "https://api.tapjoy.com/graphql";
                    $post_params = '{"query":"query {
                  advertiser {
                    adSets(first: 100) {
                      edges {
                        node {
                          id
                          name
                          nameSuffix
                        }
                      }
                      pageInfo {
                        endCursor
                        hasNextPage
                      }
                    }
                  }
                }"
                }';

                    $data_response = self::getContent2($post_url, $post_params, $data_header);
                    $data_response = json_decode($data_response, true);


                    if (isset($data_response['data']) && $data_response['data']) {
                        $res_data = $data_response['data'];
                        if (isset($res_data['advertiser']) && $res_data['advertiser']) {
                            $advertiser_info = $res_data['advertiser'];
                            if (isset($advertiser_info['adSets']) && $advertiser_info['adSets']) {
                                $adSets = $advertiser_info['adSets'];
                                if (isset($adSets['edges']) && $adSets['edges']) {
                                    $edges = $adSets['edges'];
                                    foreach ($edges as $adge_info) {
                                        if (isset($adge_info['node']) && $adge_info['node']) {
                                            $node_info = $adge_info['node'];
                                            $ad_id = $node_info['id'];
                                            $ad_name = $node_info['name'];
                                            $ad_name_suffix = $node_info['nameSuffix'];
                                            self::getAdReport($ad_id, $dayid, $company_account, $data_header, $ad_name, $ad_name_suffix);
                                        }
                                    }
                                }
                                if (isset($adSets['pageInfo']) && $adSets['pageInfo']) {
                                    $pageInfo = $adSets['pageInfo'];
                                    $endCursor = isset($pageInfo['endCursor']) ? $pageInfo['endCursor'] : '';
                                    $hasNextPage = isset($pageInfo['hasNextPage']) ? $pageInfo['hasNextPage'] : '';
                                    if ($hasNextPage && $endCursor) {
                                        self::getAdIds($dayid, $company_account, $data_header, $endCursor);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Artisan::call('TapjoyTgHandleProcesses', ['dayid' => $dayid]);

        }catch (\Exception $e) {
            // 异常报错
            $message = "{$dayid}号, " . AD_PLATFORM . " 推广平台程序报错,报错原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, SOURCE_ID, AD_PLATFORM, 4, $message);
            $error_msg_arr[] = $message;
            CommonFunction::sendMail($error_msg_arr, '推广平台程序error');
            exit;

        }

    }


    private static function getContent2($post_url, $post_params, $data_header)
    {
        static $degree = 0;
        $content = CurlRequest::curl_header_json_Post($post_url, $post_params, $data_header);
        if (!$content) {
            if ($degree > 1) {
                return false;
            }
            $degree++;
            sleep(2);
            return self::getContent2($post_url, $post_params, $data_header);
        }

        return $content;
    }

    private static function getAdIds($dayid,$company_account,$data_header,$endCursor){
        // todo 获取广告集合
        $post_url = "https://api.tapjoy.com/graphql";
        $post_params = '{"query":"query {
                  advertiser {
                    adSets(first: 100,after: \"'.$endCursor.'\") {
                      edges {
                        node {
                          id
                          name
                          nameSuffix
                        }
                      }
                      pageInfo {
                        endCursor
                        hasNextPage
                      }
                    }
                  }
                }"
                }';
        $data_response = self::getContent2($post_url, $post_params, $data_header);
        $data_response = json_decode($data_response, true);
        if (isset($data_response['data']) && $data_response['data']) {
            $res_data = $data_response['data'];
//            var_dump($res_data);
            if (isset($res_data['advertiser']) && $res_data['advertiser']) {
                $advertiser_info = $res_data['advertiser'];
                if (isset($advertiser_info['adSets']) && $advertiser_info['adSets']) {
                    $adSets = $advertiser_info['adSets'];
                    if (isset($adSets['edges']) && $adSets['edges']){
                        $edges = $adSets['edges'];
                        foreach ($edges as $adge_info){
                            if (isset($adge_info['node']) && $adge_info['node']){
                                $node_info = $adge_info['node'];
                                $ad_id = $node_info['id'];
                                $ad_name = $node_info['name'];
                                $ad_name_suffix = $node_info['nameSuffix'];
                                self::getAdReport($ad_id,$dayid,$company_account,$data_header,$ad_name,$ad_name_suffix);
                            }
                        }
                    }
                    if (isset($adSets['pageInfo']) && $adSets['pageInfo']) {
                        $pageInfo = $adSets['pageInfo'];
                        $endCursor = isset($pageInfo['endCursor']) ? $pageInfo['endCursor'] : '';
                        $hasNextPage = isset($pageInfo['hasNextPage']) ? $pageInfo['hasNextPage'] : '';
                        if ($hasNextPage && $endCursor){
                            self::getAdIds($dayid,$company_account,$data_header,$endCursor);
                        }
                    }
                }
            }
        }else{
            $error_msg = AD_PLATFORM . '推广平台' . $company_account . '账号获取报表数据失败,错误信息:' . (isset($data_response['errors']) ? $data_response['errors'] : '暂无数据');
            DataImportImp::saveDataErrorLog(1, SOURCE_ID, AD_PLATFORM, 4, $error_msg);
            $error_msg_arr[] = $error_msg;
            CommonFunction::sendMail($error_msg_arr, AD_PLATFORM . '推广平台取数error');
        }
    }

    private static function getAdReport($ad_group_id,$dayid,$company_account,$data_header,$ad_name,$ad_name_suffix){

        $post_url = "https://api.tapjoy.com/graphql";
        $post_params =
            '{"query":"query{
                       adSet(id: \"'.$ad_group_id.'\") {
                         insights(timeRange: {from: \"'.$dayid.'T00:00:00Z\", until: \"'.date('Y-m-d', strtotime("$dayid +1 days")).'T00:00:00Z\"}) {
                           timestamps
                             reports {
                               country
                               callToActionClicks
                               impressions
                               conversions
                               spend
                                }
                              }
                            }
                          }"
                         }';
        $data_response = CurlRequest::curl_header_json_Post($post_url, $post_params, $data_header);
        $data_response = json_decode($data_response, true);
        if (!empty($data_response['data'])) {
            $index = 0;
            $insert_data = [];
            $step = [];
            if (isset($data_response['data']) && $data_response['data']) {
                $result_data = $data_response['data'];
                if (isset($result_data['adSet']) &&  $result_data['adSet']) {
                    $tapjoy_adSet = $result_data['adSet'];
                    if (isset($tapjoy_adSet['insights']) && $tapjoy_adSet['insights']){
                        $insights = $tapjoy_adSet['insights'];
                        if(isset($insights['reports']) && $insights['reports']){
                            foreach ($insights['reports'] as $report_info){
                                $json_data = [];
                                $json_data['ad_group_id'] = $ad_group_id;
                                $json_data['ad_group_name'] = $ad_name;
                                $json_data['ad_group_suffix_name'] = $ad_name_suffix;
                                $json_data['paid_clicks'] = $report_info['callToActionClicks'][0];
                                $json_data['impressions'] = $report_info['impressions'][0];
                                $json_data['global_conversions'] = $report_info['conversions'][0];
                                $json_data['installs_spend'] = $report_info['spend'][0];
                                $json_data['country_code'] = $report_info['country'];

                                $insert_data[$index]['campaign_id'] = $json_data['ad_group_id'];
                                $insert_data[$index]['campaign_name'] = $json_data['ad_group_name'];
                                $insert_data[$index]['cost'] = isset($json_data['installs_spend']) ? abs($json_data['installs_spend'])/1000000 : 0.00; // 流水原币;

                                $insert_data[$index]['json_data'] = str_replace('\'', '\'\'', json_encode($json_data));
                                $insert_data[$index]['account'] = $company_account;
                                $insert_data[$index]['type'] = 2;
                                $insert_data[$index]['app_id'] = $ad_group_id;
//                            $insert_data[$index]['app_name'] = $ad_group_name;
                                $insert_data[$index]['source_id'] = SOURCE_ID;
                                $insert_data[$index]['dayid'] = $dayid;
                                $insert_data[$index]['create_time'] = date("Y-m-d H:i:s");
                                $insert_data[$index]['year'] = date("Y", strtotime($dayid));
                                $insert_data[$index]['month'] = date("m", strtotime($dayid));
                                $index++;
                            }

                        }


                    }
                }
            }

//            var_dump(count($insert_data));
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

        } else {
            $error_msg = AD_PLATFORM . '推广平台' . $company_account . '账号下ad_group_id为:' . $ad_group_id . '获取数据失败,错误信息:' . (isset($data_response['errors']) ? $data_response['errors'] : '暂无数据');
            DataImportImp::saveDataErrorLog(1, SOURCE_ID, AD_PLATFORM, 4, $error_msg);
            $error_msg_arr[] = $error_msg;
            CommonFunction::sendMail($error_msg_arr, AD_PLATFORM . '推广平台取数error');
        }

    }
    public static  function curl($url,$method,$header=array()){
        $curl = curl_init();

        curl_setopt_array($curl, array(
            CURLOPT_URL => $url,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_ENCODING => "",
            CURLOPT_MAXREDIRS => 10,
            CURLOPT_TIMEOUT => 30,
            CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_1,
            CURLOPT_CUSTOMREQUEST =>$method,
            CURLOPT_HTTPHEADER =>$header
        ));

        $response = curl_exec($curl);
        $err = curl_error($curl);

        curl_close($curl);

        return $response;
    }
}
