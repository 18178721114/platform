<?php

namespace App\Console\Commands\TgDataProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\Common\CurlRequest;
use App\Common\Service;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\DataImportLogic;
use App\Common\CommonFunction;

class VungleTgReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'VungleTgReportCommond {dayid?} {appid?}';

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
        // 入口方法
    	$dayid = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));
    	$appid = $this->argument('appid')?$this->argument('appid'):'';
    	var_dump('Vungle-ptg37-'.$dayid);

        define('AD_PLATFORM', 'Vungle');
        define('SCHEMA', 'tg_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'ptg37');
        //ini_set('memory_limit', '200M');

        try {
            $sql = "  select distinct a.platform_id,a.data_account as company_account,b.application_id,a.account_api_key as api_key from c_platform_account_mapping a left join c_generalize b on b.platform_id = a.platform_id and a.account = b.data_account where a.platform_id = 'ptg37' and b.application_id != '' ";
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

            foreach ($info as $key => $value) {
                $header = array("accept: application/json", "authorization: Bearer " . $value['api_key'], "cache-control: no-cache", "vungle-version: 1");
                // todo 获取应用信息 上线需要修改下取数URL
                $url = str_replace(array('_END_DATE_', '_BEGIN_DATE_', '_APPLICATION_ID_'), array($dayid, $dayid, $value['application_id']), env('VUNGLE_URL'));
                $ret = self::getContent2($url, $header, $value['company_account'], $value['application_id']);
                if ($ret && !isset($ret['error'])) {//成功取到数

                    //删除数据库里原来数据
                    $map['dayid'] = $dayid;
                    $map['source_id'] = SOURCE_ID;
                    $map['account'] = $value['company_account'];
                    $map['app_id'] = $value['application_id'];

                    //删除数据
                    $count = DataImportLogic::getChannelData('tg_data', 'erm_data', $map)->count();
                    if ($count > 0) {
                        DataImportLogic::deleteHistoryData('tg_data', 'erm_data', $map);
                    }
                    $index = 0;
                    $insert_data = [];
                    $step = [];
                    foreach ($ret as $v) {

                        $insert_data[$index]['campaign_id'] = isset($v['campaign id']) ? $v['campaign id'] : '';
                        $insert_data[$index]['campaign_name'] = isset($v['campaign name']) ? $v['campaign name'] : '';
                        $insert_data[$index]['cost'] = isset($v['spend']) ? $v['spend'] : 0.00;

                        $insert_data[$index]['app_id'] = $value['application_id'];
                        $insert_data[$index]['account'] = $value['company_account'];
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

                }
            }

            // 调用数据处理过程
            Artisan::call('VungleTgHandleProcesses', ['dayid' => $dayid]);

        }catch (\Exception $e) {
            // 异常报错
            $message = "{$dayid}号, " . AD_PLATFORM . " 推广平台程序报错,报错原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, SOURCE_ID, AD_PLATFORM, 4, $message);
            $error_msg_arr[] = $message;
            CommonFunction::sendMail($error_msg_arr, '推广平台程序error');
            exit;

        }
    }

    /**
     * 根据请求url，得到响应
     * @param $url
     * @return bool|string
     */
    private static function getContent2($url,$header,$account,$application_id)
    {
        static $degree = 0;

        //var_dump($degree);
        if (!$content = self::get_response($url,$header)) {
            if ($degree > 2) {
                return false;
            }
            $degree++;
            sleep(2);
            return self::getContent2($url,$header,$account,$application_id);
        }

        $data = json_decode($content, true);
        if (isset($data['error']) || !$data) {
            if (!$data){
                $status = '';
                $error = '暂无数据'.json_encode($data);
            }else{
                $status = isset($data['status']) ? $data['status'] : '';
                $error = isset($data['error']) ? $data['error'] : '暂无数据'.json_encode($data);
            }

            $error_msg = 'Vungle推广平台'.$account.'账号下应用ID为'.$application_id.'取数失败,错误信息:'.$status.','.$error;

            DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,4,$error_msg);
            $error_msg_arr[] = $error_msg;
            CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'推广平台取数error');

            return false;
        }else{
            return $data;
        }

    }

    public static function get_response($url, $header=[])
    {
    	$ch = curl_init();
    	curl_setopt($ch, CURLOPT_URL, $url);

    	curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
    	curl_setopt($ch, CURLOPT_HEADER, 0);
    	curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
    	curl_setopt($ch, CURLOPT_TIMEOUT,120); 
    	curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, FALSE); 
    	curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, FALSE);


    	if (!empty($header)) {
    		curl_setopt($ch, CURLOPT_HTTPHEADER, $header);
    	}

    	$output = curl_exec($ch);
    	curl_close($ch);
    	return $output;
    }
}
