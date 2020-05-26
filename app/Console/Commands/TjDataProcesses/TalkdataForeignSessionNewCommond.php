<?php

namespace App\Console\Commands\TjDataProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\Common\CommonFunction;
use App\Common\CurlRequest;
use App\Common\Service;
use App\Common\TdDataNewFunction;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\DataImportLogic;

class TalkdataForeignSessionNewCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'TalkdataForeignSessionNewCommond {dayid?}';

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
        // 维护 talkingdata_foreign_user 表
        // 0 2 * * * sh /data/web-server/openresty/nginx/html/reportingApi/talkingdata/sc/talkingdata_session2.sh
        // 0 6 * * * sh /data/web-server/openresty/nginx/html/reportingApi/talkingdata/sc/talkingdata_session2.sh

        header('content-type:text/html;charset=utf-8');
        set_time_limit(0);

        define('AD_PLATFORM', 'TalkingData');
        define('SOURCE_ID', 'ptj02'); // todo 这个需要根据平台信息表确定平台ID
        // 入口方法
        $day = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-2 day'));
        var_dump($day);

//        $sql = DB::select("select * from plat_tj_code where plat_id='ptj02' "); // todo 正式
//        $sql = "select * from plat_tj_code where plat_id='ptj02' and app_name = '铁头英雄'  group by app_id,app_key,country_type";
        $sql = "SELECT DISTINCT
                if(c_app.os_id = 1,2,1) as platform_id,
                c_app_statistic.td_app_id as app_id,
                c_app_statistic.access_key as app_key,
                c_app_statistic_version.statistic_app_name as app_name ,
                c_app_statistic_version.app_version as versionname,
                c_app_statistic_version.statistic_version as appversion,
                c_app_statistic_version.channel_id as channelid,
                c_channel.td_channel_id as channelname
                FROM
                c_app_statistic
                LEFT JOIN c_app_statistic_version ON c_app_statistic.id = c_app_statistic_version.app_statistic_id
                LEFT JOIN c_app ON c_app.id = c_app_statistic.app_id
                LEFT JOIN c_channel ON c_channel.id = c_app_statistic_version.channel_id
                WHERE
                c_app_statistic.statistic_type = 2
                AND c_app_statistic_version.ad_status != 2
                and (c_app_statistic_version.statistic_app_name = '铁头英雄' or c_app_statistic_version.statistic_app_name = 'Android-Beat Racer-GN') 
                order by platform_id,app_key"; // todo 正式删除

        $result = DB::select($sql); // todo 测试
        $result = Service::data($result);
        $app_key_versions = [];
        $app_key_channel = [];
        foreach ($result as $key_num => $row){
            //var_dump($row);
            unset ($channel);
            unset($version);
            unset($metrics_id);
            unset($groupby);
            unset($gameinfo);
            $channel = [];
            $version = [];

            $groupby = "geocountry";

            $now_app_key_versions = $row['app_key'] .'-'. $row['platform_id'];
            if($row['versionname'] == 'ALL' || $row['appversion'] == 'ALL'){
                $versions = [];
                // 全版本
                if (isset($app_key_versions[$now_app_key_versions]) && isset($app_key_versions[$now_app_key_versions])){
                    $versions = $app_key_versions[$now_app_key_versions];
                }else{
                    $versionInfo = TdDataNewFunction::getVersionList($row['app_key'], $row['platform_id']);
                    if ($versionInfo['status'] != 200) {
                        TdDataNewFunction::saveErrorLog($versionInfo['message']);
                        return;
                    }
                    foreach ($versionInfo['result'] as $ver) {
                        $versions[$ver['appversion']] = $ver['versionname'];
                    }
                    $app_key_versions = [];
                    $app_key_versions[$now_app_key_versions] = $versions;
                }

            }else{
                $versions = [];
                $versions[$row['appversion']]= $row['versionname'];
            }

            //判断channel_type
            $now_app_key_channel = $row['app_key'] .'-'. $row['platform_id'];
            // 全版本
            if (isset($app_key_channel[$now_app_key_channel]) && isset($app_key_channel[$now_app_key_channel])){
                $channels = [];
                $channels = $app_key_channel[$now_app_key_channel];
            }else{
                $channels = [];
                $channelInfo = TdDataNewFunction::getChannelList($row['app_key'], $row['platform_id']);
                if ($channelInfo['status'] != 200) {
                    TdDataNewFunction::saveErrorLog($channelInfo['message']);
                    return;
                }
                foreach($channelInfo['result'] as $d){
                    $channels[$d['channelid']] = $d['channelname'];
                }
                $app_key_channel = [];
                $app_key_channel[$now_app_key_channel] = $channels;
            }

            $channelname = $row['channelname'];

            if ($channelname && $channels){
                foreach ($channels as $channels_key => $channels_value){
                    if ($channelname == $channels_value){
                        $channel[$channels_key] = $channels_value;
                    }
                }
            }else{
                $channel = $channels;
            }

            var_dump($key_num);
            $gameinfo[$row['app_id']] = $row['app_name'];
            var_dump($gameinfo);
            TdDataNewFunction::foreignCountryCreateSessionData($row['app_key'],$gameinfo,$row['platform_id'],$day,$channel,$versions,$groupby);
        }
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

    // 保存日志
    private static function saveLog($platform_name = '未知', $message = ''){

        $fileName = date('Y-m-d',time());
        $dir = './storage/tjDataLogs';

        if (!is_dir($dir)) {
            mkdir($dir,0777,true);
        }
        $logFilename = $dir.'/'.$platform_name.'_tj'.'.log';
        //生成日志
        file_put_contents( $logFilename,$message . "\n\n",FILE_APPEND);
    }
}
