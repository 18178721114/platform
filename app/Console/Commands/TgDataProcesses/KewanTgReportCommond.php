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

class KewanTgReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'KewanTgReportCommond {dayid?} {appid?}';

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
        $dayid = str_replace('-', '', $dayid);
        var_dump($dayid);

        define('AD_PLATFORM', '可玩');
        define('SCHEMA', 'tg_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'ptg74'); // todo 这个需要根据平台信息表确定平台ID



        try {
            //这里面要写新测试平台里的数据配置 从数据库里取数据
            $info[0]['company_username'] = '717F4ECD-D04A-2DE8-706A-5641D412C940';
            $info[0]['api_key'] = 'F655BA32-3A1B-FC21-C408-5397B7E4184F';
            $all_data_err = [];
            foreach ($info as $key => $value) {

                $token = $value['api_key'];
                // 生成签名
                $timestamp = time();
                $nonce = rand(1, 999);
                $tmpArr = array($token, $timestamp, $nonce);
                sort($tmpArr, SORT_STRING);
                $signature = implode($tmpArr);
                $signature = sha1($signature);

                $params = array('signature' => $signature, 'timestamp' => $timestamp, 'nonce' => $nonce,);
                $url = env('KEWAN_URL') . "/advertiser/{$value['company_username']}/apps";
                $url .= '?' . http_build_query($params, null, '&');

                $appInfoList = self::get_response($url);
                $appInfoList = json_decode($appInfoList, true);

                // 数据获取重试
                $api_data_i = 1;
                while (!$appInfoList) {
                    // 生成签名
                    $timestamp = time();
                    $nonce = rand(1, 999);
                    $tmpArr = array($token, $timestamp, $nonce);
                    sort($tmpArr, SORT_STRING);
                    $signature = implode($tmpArr);
                    $signature = sha1($signature);

                    $params = array('signature' => $signature, 'timestamp' => $timestamp, 'nonce' => $nonce,);
                    $url = env('KEWAN_URL') . "/advertiser/{$value['company_username']}/apps";
                    $url .= '?' . http_build_query($params, null, '&');

                    $appInfoList = self::get_response($url);
                    $appInfoList = json_decode($appInfoList, true);
                    $api_data_i++;
                    if ($api_data_i > 3) break;
                }

                //获取应用信息
                if (!empty($appInfoList['data'])) {

                    $map['dayid'] = $dayid;
                    $map['source_id'] = SOURCE_ID;
                    $map['account'] = $value['company_username'];
                    $count = DataImportLogic::getChannelData('tg_data', 'erm_data', $map)->count();
                    if ($count > 0) {
                        //删除数据
                        DataImportLogic::deleteHistoryData(SCHEMA, TABLE_NAME, $map);
                    }

                    foreach ($appInfoList['data'] as $appInfo) {
                        $dataUrl = env('KEWAN_URL') . "/advertiser/{$value['company_username']}/app/{$appInfo['app_id']}/ads";
                        $dataUrl .= '?' . http_build_query($params, null, '&');
                        $dataInfo = self::get_response($dataUrl);
                        $dataInfo = json_decode($dataInfo, true);
                        if (!empty($dataInfo['data'])) {
                            foreach ($dataInfo['data'] as $ad_k => $ad_v) {

                                $url = env('KEWAN_URL') . "/advertiser/{$value['company_username']}/app/{$appInfo['app_id']}/ad/{$ad_v['ad_id']}/stats";
                                $params['start_date'] = $dayid;
                                $params['end_date'] = $dayid;
                                $params['group_dimension'] = "country";

                                $params['page'] = 1;
                                $params['size'] = 80000;
                                $url .= '?' . http_build_query($params, null, '&');
                                $ad_dataInfo = self::get_response($url);
                                $ad_stats_data = json_decode($ad_dataInfo, true);
                                if (isset($ad_stats_data['error']) && $ad_stats_data['error'] == '' && isset($ad_stats_data['data'])) {

                                    $index = 0;
                                    $insert_data = [];
                                    $step = [];
                                    foreach ($ad_stats_data['data']['list'] as $a => $data) {
                                        $insert_data[$index]['account'] = $value['company_username'];
                                        $insert_data[$index]['type'] = 2;
                                        $insert_data[$index]['app_name'] = addslashes($appInfo['name']);
                                        $insert_data[$index]['app_id'] = $appInfo['app_id'];
                                        $insert_data[$index]['source_id'] = SOURCE_ID;
                                        $insert_data[$index]['dayid'] = $dayid;
                                        $data['os'] = $appInfo['os'];
                                        $data['name'] = $appInfo['name'];
                                        $insert_data[$index]['json_data'] = str_replace('\'', '\'\'', json_encode($data));
                                        $insert_data[$index]['create_time'] = date("Y-m-d H:i:s");
                                        $insert_data[$index]['year'] = date("Y", strtotime($dayid));
                                        $insert_data[$index]['month'] = date("m", strtotime($dayid));
                                        $insert_data[$index]['campaign_id'] = $data['ad_id'];
                                        $insert_data[$index]['campaign_name'] = '';
                                        $insert_data[$index]['cost'] = isset($data['cost']) ? $data['cost'] / 100 : 0.00;
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
                                            $result = DataImportLogic::insertChannelData(SCHEMA, TABLE_NAME, $v);
                                            if (!$result) {
                                                echo 'mysql_error' . PHP_EOL;
                                            }
                                        }

                                    }


                                } else {
                                    $all_data_err[] = $value['company_username'] . '账号下应用' . $appInfo['app_id'] . '广告位' . $ad_v['ad_id'] . '数据为空,错误信息' . (isset($ad_stats_data['error']) ? $ad_stats_data['error'] : '无数据，接口未返回任何信息');
                                }
                            }

                        } else {
                            $all_data_err[] = $value['company_username'] . '账号下应用' . $appInfo['app_id'] . '取数失败,错误信息:' . (isset($dataInfo['error']) ? $dataInfo['error'] : '无数据，接口未返回任何信息');
                        }
                    }
                } else {
                    $all_data_err[] =  $value['company_username'] . '账号获取apps列表失败,错误信息:' .(isset($appInfoList['error']) ?  $appInfoList['error'] : '无数据，接口未返回任何信息');
                }
            }

            if ($all_data_err){
                $error_application_str = implode(',',$all_data_err);
                $message = "{$dayid}号,". AD_PLATFORM . " 推广平台取数失败信息:".$error_application_str ;
                DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,4,$message);
                $error_msg_arr[] = $message;
                CommonFunction::sendMail($error_msg_arr, '推广平台取数error');
            }

            Artisan::call('KewanTgHandleProcesses', ['dayid' => $dayid]);

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
}
