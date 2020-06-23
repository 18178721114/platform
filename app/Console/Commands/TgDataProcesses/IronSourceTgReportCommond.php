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

class IronSourceTgReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'IronSourceTgReportCommond {dayid?} {appid?}';

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

        define('AD_PLATFORM', 'IronSourceTg');
        define('SCHEMA', 'tg_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'ptg03'); // todo 这个需要根据平台信息表确定平台ID

        try {
            $sql = " select distinct platform_id,data_account as company_account,account_token as SecretKey from c_platform_account_mapping where platform_id = 'ptg03' ";
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
                $user_name = $value['company_account'];
                $secret_key = $value['SecretKey'];
                $base64encoded = base64_encode("$user_name:$secret_key");
                $header = array();
                $header[] = 'Authorization: Basic ' . $base64encoded;
                $url = str_replace(array('_END_DATE_', '_BEGIN_DATE_'), array($dayid, $dayid), env('IRONSRC_TG_URL'));
                $dataList = $this->get_response($url, $header);
                $dataList = json_decode($dataList, true);

                // 数据获取重试
                $api_data_i = 1;
                while (!$dataList) {
                    $dataList = $this->get_response($url, $header);
                    $dataList = json_decode($dataList, true);
                    $api_data_i++;
                    if ($api_data_i > 3) break;
                }

                /*如果数据返回正常，则response不含code属性*/
                if (!empty($dataList['data'])) {
                    $map = [];
                    $map['dayid'] = $dayid;
                    $map['source_id'] = SOURCE_ID;
                    $map['account'] = $user_name;
                    $count = DataImportLogic::getChannelData('tg_data', 'erm_data', $map)->count();
                    if ($count > 0) {
                        //删除数据
                        DataImportLogic::deleteHistoryData('tg_data', 'erm_data', $map);
                    }

                    $index = 0;
                    $insert_data = [];
                    $step = [];
                    foreach ($dataList['data'] as $k => $v) {
                        $insert_data[$index]['account'] = $user_name;
                        $insert_data[$index]['json_data'] = str_replace('\'', '\'\'', json_encode($v));
                        $insert_data[$index]['type'] = 2;
                        $insert_data[$index]['source_id'] = SOURCE_ID;
                        $insert_data[$index]['dayid'] = $dayid;
                        $insert_data[$index]['create_time'] = date("Y-m-d H:i:s");
                        $insert_data[$index]['year'] = date("Y", strtotime($dayid));
                        $insert_data[$index]['month'] = date("m", strtotime($dayid));
                        $insert_data[$index]['campaign_id'] = isset($v['campaign_id']) ? addslashes($v['campaign_id']) : '';
                        $insert_data[$index]['campaign_name'] = isset($v['campaign_name']) ? addslashes($v['campaign_name']) : '';
                        $insert_data[$index]['cost'] = isset($v['spend']) ? $v['spend'] : 0;
                        $index++;

                    }
                    //批量插入
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

                    $error_msg = $value['company_account'] . '账号取数失败,错误信息:';
                    if (isset($dataList['errorMessage'])) {
                        $error_msg .= $dataList['errorMessage'];
                    } else {
                        $error_msg .= '无数据,接口未返回任何信息';
                    }
                    $all_data_err[] = $error_msg;
                }

            }

            if ($all_data_err){
                $error_application_str = implode(',',$all_data_err);
                $message = "{$dayid}号,". AD_PLATFORM . " 推广平台取数失败信息:".$error_application_str ;
                DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,4,$message);
                $error_msg_arr[] = $message;
                CommonFunction::sendMail($error_msg_arr, '推广平台取数error');
            }

            Artisan::call('IronSourceTgHandleProcesses', ['dayid' => $dayid]);


        }catch (\Exception $e) {
            // 异常报错
            $message = "{$dayid}号, " . AD_PLATFORM . " 推广平台程序报错,报错原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, SOURCE_ID, AD_PLATFORM, 4, $message);
            $error_msg_arr[] = $message;
            CommonFunction::sendMail($error_msg_arr, '推广平台程序error');
            exit;

        }

    }
    public  function get_response($URL, $header)
    {
            $crl = curl_init();
            curl_setopt($crl, CURLOPT_URL, $URL);
            curl_setopt($crl, CURLOPT_RETURNTRANSFER, 1);
            curl_setopt($crl, CURLOPT_HTTPHEADER, $header);
            curl_setopt($crl, CURLOPT_SSL_VERIFYPEER, false); // 跳过证书检查
            curl_setopt($crl, CURLOPT_SSL_VERIFYHOST, 2);  // 从证书中检查SSL加密算法是否存在
            curl_setopt($crl, CURLOPT_TIMEOUT, 30);  // 从证书中检查SSL加密算法是否存在
            $output = curl_exec($crl);
            curl_close($crl);
            return $output;
    }
}
