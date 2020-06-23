<?php

namespace App\Console\Commands\AdDataProcesses;

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

class TapjoyReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'TapjoyReportCommond {dayid?} {appid?}';

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
        define('SCHEMA', 'ad_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'pad31');
        try{
        $sql = "  SELECT  data_account as company_account,account_api_key  as api_key from c_platform_account_mapping WHERE platform_id ='pad31'";
        $info = DB::select($sql);
        $info = Service::data($info);
        if (!$info) return;
        foreach ($info as $kk => $vv) {
            $company_account = $vv['company_account'];
            $app['api_key'] = $vv['api_key'];
            $access_token_header = array("authorization:Basic " . $app['api_key'], "accept: application/json; */*",);
            $token_url = env('TAPJOY_TOKEN_URL');

            $response = self::curl($token_url, 'POST', $access_token_header);
            $response = json_decode($response, true);
            if (isset($response['error'])) {
                $error_msg = AD_PLATFORM . '推广平台' . $company_account . '账号获取access_token失败,错误信息:' . $response['error'];
                DataImportImp::saveDataErrorLog(1, SOURCE_ID, AD_PLATFORM, 4, $error_msg);
                die;
            }


            $access_token = $response['access_token'];
            $data_header = array("accept: application/json; */*", "authorization: Bearer  {$access_token}",);

            //删除数据库里原来数据
            $map['dayid'] = $dayid;
            $map['source_id'] = SOURCE_ID;
            $map['account'] = $company_account;
            $count = DataImportLogic::getChannelData('ad_data','erm_data',$map)->count();
            if($count>0){
            //删除数据
                DataImportLogic::deleteHistoryData('ad_data', 'erm_data', $map);
            }

            $index = 0;
            $insert_data = [];
            $step = [];
            for ($PageSize=1;$PageSize<5;$PageSize++) {
                $post_url = "https://api.tapjoy.com/v2/publisher/reports?date=$dayid&page=$PageSize&page_size=100&group_by=content_cards";
                $data_response = self::get_response($post_url, $data_header);
                $data_response = json_decode($data_response, true);

                if (isset($data_response['Apps']) && $data_response['Apps']) {
                    $res_data = $data_response['Apps'];
                    foreach ($res_data as $app_data) {
                        $app_name = isset($app_data['Name']) ? $app_data['Name'] : '';
                        $app_id = isset($app_data['AppKey']) ? $app_data['AppKey'] : '';
                        if (isset($app_data['ContentCards']) && $app_data['ContentCards']) {
                            $content_types = $app_data['ContentCards'];
                            foreach ($content_types as $content_type) {
                                $ad_type = isset($content_type['Type']) ? $content_type['Type'] : '';
                                if (isset($content_type['Countries']) && $content_type['Countries']) {
                                    $countries = $content_type['Countries'];
                                    foreach ($countries as $countrie) {
                                        $country = isset($countrie['Country']) ? $countrie['Country'] : '';
                                        $clicks = isset($countrie['Clicks']) ? $countrie['Clicks'] : '';
                                        $conversions = isset($countrie['Conversions']) ? $countrie['Conversions'] : '';
                                        $impressions = isset($countrie['Impressions']) ? $countrie['Impressions'] : '';
                                        $revenue = isset($countrie['Revenue']) ? $countrie['Revenue'] : 0.00;

                                        $json_data = [];
                                        $json_data['app_name'] = $app_name;
                                        $json_data['app_id'] = $app_id;
                                        $json_data['ad_type'] = $ad_type;
                                        $json_data['country'] = $country;
                                        $json_data['clicks'] = $clicks;
                                        $json_data['conversions'] = $conversions;
                                        $json_data['impressions'] = $impressions;
                                        $json_data['revenue'] = $revenue;

                                        $insert_data[$index]['ad_id'] = $app_id;
                                        $insert_data[$index]['ad_name'] = $app_name;
                                        $insert_data[$index]['income'] = $revenue; // 流水原币;

                                        $insert_data[$index]['json_data'] = str_replace('\'', '\'\'', json_encode($json_data));
                                        $insert_data[$index]['account'] = $company_account;
                                        $insert_data[$index]['type'] = 2;
                                        $insert_data[$index]['app_id'] = $app_id;
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
                }
            }

            var_dump(count($insert_data));
            $i = 0;
            foreach ($insert_data as $kkkk => $insert_data_info) {
                if ($kkkk % 2000 == 0) $i++;
                if ($insert_data_info) {
                    $step[$i][] = $insert_data_info;
                }
            }
            if ($step) {
                foreach ($step as $k => $v) {
                    $result = DataImportLogic::insertChannelData('ad_data', 'erm_data', $v);
                    if (!$result) {
                        echo 'mysql_error' . PHP_EOL;
                    }
                }
            }
        }
        // todo 正式打开
        Artisan::call('TapjoyHandleProcesses', ['dayid' => $dayid]);
        } catch (\Exception $e) {
            $error_msg_info = $dayid.'号,'.AD_PLATFORM.'渠道数据匹配失败：'.$e->getMessage();
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
