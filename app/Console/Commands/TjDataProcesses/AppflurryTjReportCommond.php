<?php

namespace App\Console\Commands\TjDataProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\Common\CommonFunction;
use App\Common\CurlRequest;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\DataImportLogic;

class AppflurryTjReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'AppflurryTjReportCommond {stime?}';

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
        set_time_limit(0);

        define('AD_PLATFORM', 'Flurry');
        define('SOURCE_ID', 'ptj01'); // todo 这个需要根据平台信息表确定平台ID
        // 入口方法
    	$stime = $this->argument('stime') ? $this->argument('stime') : date('Y-m-d',strtotime('-1 day'));
    	$etime = date('Y-m-d',strtotime($stime) + 86400);
        var_dump($stime,$etime);
        try {
            $tj_platform_list = [['account' => 'zplay', 'header' => ["Authorization:Bearer eyJhbGciOiJIUzI1NiIsImtpZCI6ImZsdXJyeS56dXVsLnByb2Qua2V5c3RvcmUua2V5LjEifQ.eyJpc3MiOiJodHRwczovL3p1dWwuZmx1cnJ5LmNvbTo0NDMvdG9rZW4iLCJpYXQiOjE0ODIxMzA4MTMsImV4cCI6MzMwMzkwMzk2MTMsInN1YiI6IjM3NzY2NiIsImF1ZCI6IjQiLCJ0eXBlIjo0LCJqdGkiOiIzODkifQ.jXz4-hV98brkCpu-OMzJ9kQIyeyBKvI6zTLy_e0o880"]], ['account' => 'noodlecake', 'header' => ["Authorization:Bearer eyJhbGciOiJIUzI1NiIsImtpZCI6ImZsdXJyeS56dXVsLnByb2Qua2V5c3RvcmUua2V5LjIifQ.eyJpc3MiOiJodHRwczovL3p1dWwuZmx1cnJ5LmNvbTo0NDMvdG9rZW4iLCJpYXQiOjE1MDUyNDM5MDIsImV4cCI6MzMwNjIxNTI3MDIsInN1YiI6IjQwMzg0MCIsImF1ZCI6IjQiLCJ0eXBlIjo0LCJqdGkiOiIyMDcyIn0.9CmVS2sZnlzH7vsNwOSlzx3dDJZ4x5C3uCGM7ga_o_A"]]];

            foreach ($tj_platform_list as $tj_platform_info) {
                $account = $tj_platform_info['account'];
                $header = $tj_platform_info['header'];

                $url = "https://api-metrics.flurry.com/public/v1/data/appUsage/day/app/appVersion/country?metrics=sessions,activeDevices,newDevices,timeSpent&dateTime={$stime}/{$etime}";
                $result = self::zplay_curl($url, 'get', array(), $header, 'https');
                if ($result['rows']) {

                    DB::delete("delete from flurry where dateTime like '{$stime}%' and account = '{$account}'");

                    $array = [];
                    var_dump(count($result['rows']));
                    foreach ($result['rows'] as $singleInfo) {
                        $flurry_data = [];
                        $flurry_data['account'] = $account;
                        $flurry_data['dateTime'] = isset($singleInfo['dateTime']) ? $singleInfo['dateTime'] : '';
                        $flurry_data['app_name'] = isset($singleInfo['app|name']) ? $singleInfo['app|name'] : '';
                        $flurry_data['version'] = isset($singleInfo['appVersion|name']) ? $singleInfo['appVersion|name'] : '';
                        $flurry_data['country'] = isset($singleInfo['country|name']) ? $singleInfo['country|name'] : '';
                        $flurry_data['region_name'] = isset($singleInfo['region|name']) ? $singleInfo['region|name'] : '';
                        $flurry_data['sessions'] = isset($singleInfo['sessions']) ? $singleInfo['sessions'] : '';
                        $flurry_data['active_devices'] = isset($singleInfo['activeDevices']) ? $singleInfo['activeDevices'] : '';
                        $flurry_data['new_devices'] = isset($singleInfo['newDevices']) ? $singleInfo['newDevices'] : '';
                        $flurry_data['time_spent'] = isset($singleInfo['timeSpent']) ? $singleInfo['timeSpent'] : '';
                        $flurry_data['update_time'] = now();
                        $array[] = $flurry_data;

                    }

                    if ($array) {
                        var_dump(count($array));
                        //拆分批次
                        $step = array();
                        $i = 0;
                        $account_total = 0;
                        foreach ($array as $kkkk => $insert_data_info) {
                            if ($kkkk % 1000 == 0) $i++;
                            if ($insert_data_info) {
                                $account_total++;
                                $step[$i][] = $insert_data_info;
                            }
                        }

                        $is_success = [];
                        if ($step) {
                            foreach ($step as $k => $v) {
                                $result = DataImportLogic::insertAdReportInfo('flurry', $v);
                                if (!$result) {
                                    $is_success[] = $k;
                                }
                            }
                        }
                    }

                } else {

                    $error_msg = AD_PLATFORM . '统计平台获取用户数据失败,错误信息:' . (isset($result['message']) ? $result['message'] : '未知');
                    DataImportImp::saveDataErrorLog(1, SOURCE_ID, AD_PLATFORM, 1, $error_msg);
                    $error_msg_arr[] = $error_msg;
                    CommonFunction::sendMail($error_msg_arr, AD_PLATFORM . '统计平台取数error');
                }
            }

            DB::update("update flurry set app_name = 'Android-SwingKing-taptap' where app_name='Android-SwingKing-GP' and version = '2.0.0' and dateTime like '{$stime}%' ");
            DB::update("update flurry set app_name = 'Android-10KTaps-taptap' where app_name='Android-10KTaps-GP' and version = '2.0' and dateTime like '{$stime}%' ");
        }catch (\Exception $e) {
            // 异常报错
            $message = "{$stime}号, " . AD_PLATFORM . "统计平台程序报错,报错原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, SOURCE_ID, AD_PLATFORM, 1, $message);
            $error_msg_arr[] = $message;
//            CommonFunction::sendMail($error_msg_arr, '统计平台程序error');
            exit;
        }

        Artisan::call('FlurryTjHandleProcesses' ,['dayid'=>$stime]);

    }


    public static  function zplay_curl($url,$method='',$post_data=array(),$httpheader=array(),$http=''){
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);  //获取页面内容，不直接输出到页面
        curl_setopt($ch, CURLOPT_HEADER,0);
        if($method=='post'){
            curl_setopt($ch,CURLOPT_POST, 1);
            if(count($post_data)==0){
                echo '无post数据';exit;
            }else
                curl_setopt($ch,CURLOPT_POSTFIELDS, json_encode($post_data)); //post请求参数
        }else{//get

        }
        if(count($httpheader)!=0){
            curl_setopt($ch, CURLOPT_HTTPHEADER,$httpheader);
        }
        if($http=='https'){
            curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, FALSE);
            curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, FALSE);
        }
        $output = curl_exec($ch);
        curl_close($ch);
        $ret = json_decode($output,true);
        return $ret;
    }
}
