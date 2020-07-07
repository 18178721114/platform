<?php

namespace App\Console\Commands\AdDataProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\Common\CommonFunction;
use App\Common\CurlRequest;
use App\Common\Service;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\DataImportLogic;
use Illuminate\Support\Facades\Artisan;

class MobvistaReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'MobvistaReportCommond {dayid?} {appid?}';

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
    	$dayid = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));
    	$appid = $this->argument('appid')?$this->argument('appid'):'';
        $dayid = str_replace('-', "", $dayid);
    	var_dump($dayid);

        define('AD_PLATFORM', 'Mobvista');
        define('SCHEMA', 'ad_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'pad50'); // todo 这个需要根据平台信息表确定平台ID
        try{
        //这里面要写新测试平台里的数据配置 从数据库里取数据
//    	$info[0]['company_account'] ='weibo@zplay.com';
//    	$info[0]['Skey'] ='acb64ce075c6bc47f4f51acd261fd28f';
//    	$info[0]['Secret'] ='8cf6690b7d64407cfcec4d6493e2d90a';
        $sql = " SELECT  data_account as company_account,account_api_key  as Skey,account_token  as Secret from c_platform_account_mapping WHERE platform_id ='pad50' and status = 1 ";
        $info = DB::select($sql);
        $info = Service::data($info);
        if ($info){
            foreach ($info as $key => $value) {

                //删除数据库里原来数据
                $map['dayid'] = $dayid;
                $map['source_id'] = SOURCE_ID;
                $map['account'] = $value['company_account'];
                $count = DataImportLogic::getChannelData('ad_data','erm_data',$map)->count();
                if($count>0){

                //删除数据
                    DataImportLogic::deleteHistoryData('ad_data', 'erm_data', $map);
                }

                for($mo_i = 1;$mo_i <= 5;$mo_i++) {
                    $str_arr = array("skey" => $value['Skey'], "v" => "1.3", "time" => time(), "start" => $dayid, "end" => $dayid, 'group_by' => urlencode('date,app_id,unit_id,country'), 'limit' => 1000, 'page' => $mo_i);
                    ksort($str_arr);
                    $str = '';
                    foreach ($str_arr as $k => $v) {
                        $str .= "&$k=$v";
                    }
                    $str = ltrim($str, '&');
                    $str1 = urlencode($str);
                    $signature = md5(md5($str) . $value['Secret']);
                    $url = env('MOBVISTA_URL');
                    $url .= "?" . $str . "&sign=" . $signature;
                    //var_dump($url);
                    $result1 = self::get_response($url);
                    $result = json_decode($result1, true);

                    // 数据获取重试
                    $api_data_i=1;
                    while(!$result){
                        $str_arr = array("skey" => $value['Skey'], "v" => "1.3", "time" => time(), "start" => $dayid, "end" => $dayid, 'group_by' => urlencode('date,app_id,unit_id,country'), 'limit' => 1000, 'page' => $mo_i);
                        ksort($str_arr);
                        $str = '';
                        foreach ($str_arr as $k => $v) {
                            $str .= "&$k=$v";
                        }
                        $str = ltrim($str, '&');
                        $str1 = urlencode($str);
                        $signature = md5(md5($str) . $value['Secret']);
                        $url = env('MOBVISTA_URL');
                        $url .= "?" . $str . "&sign=" . $signature;
                        //var_dump($url);
                        $result1 = self::get_response($url);
                        $result = json_decode($result1, true);
                        $api_data_i++;
                        if($api_data_i>3)
                            break;
                    }
                    if($api_data_i ==4 && empty($result)){
                        $error_msg_1 = AD_PLATFORM.'广告平台'.$value['company_account'].'账号取数失败,错误信息:返回数据为空('.$result1.')';
                        DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg_1);
                        break;

                    }

                    if ($result['code'] == 'ok') {
                        if (!empty($result['data']['lists'])) {

                            $index = 0;
                            $insert_data = [];
                            $step = [];
                            foreach ($result['data']['lists'] as $v) {
                                $insert_data[$index]['account'] = $value['company_account'];
                                $insert_data[$index]['type'] = 2;
                                $insert_data[$index]['source_id'] = SOURCE_ID;
                                $insert_data[$index]['dayid'] = $dayid;
                                $insert_data[$index]['json_data'] = str_replace('\'', '\'\'', json_encode($v));
                                $insert_data[$index]['create_time'] = date("Y-m-d H:i:s");
                                $insert_data[$index]['year'] = date("Y", strtotime($dayid));
                                $insert_data[$index]['month'] = date("m", strtotime($dayid));
                                $insert_data[$index]['ad_id'] =$v['unit_id'];
                                $insert_data[$index]['ad_name'] =$v['unit_name'];
                                $insert_data[$index]['app_id'] =$v['app_id'];
                                $insert_data[$index]['app_name'] =$v['app_name'];
                                $insert_data[$index]['income'] =$v['est_revenue'];
                                $index++;
                            }
                            $i = 0;
                            //var_dump(count($insert_data));
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
                    } else {

                        $error_msg = AD_PLATFORM . '广告平台' . $value['company_account'] . '账号取数失败,错误信息:' . json_encode($result);
                        DataImportImp::saveDataErrorLog(1, SOURCE_ID, AD_PLATFORM, 2, $error_msg);

                        $error_msg_arr = [];
                        $error_msg_arr[] = $error_msg;
                        CommonFunction::sendMail($error_msg_arr, AD_PLATFORM . '广告平台取数error');
                    }
                }

            }
            Artisan::call('MobvistaHandleProcesses' ,['dayid'=>$dayid]);
        }
        } catch (\Exception $e) {
            $error_msg_info = $dayid.'号,'.AD_PLATFORM.'广告平台程序失败，失败原因：'.$e->getMessage();
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

}
