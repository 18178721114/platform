<?php

namespace App\Console\Commands\TjDataProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\Common\CommonFunction;
use App\Common\CurlRequest;
use App\Common\Service;
use App\Common\TdDataFunction;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\DataImportLogic;

class TalkdataForeignUserRetentionTjCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'TalkdataForeignUserRetentionTjCommond {dayid?}';

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
        // 维护 talkingdata_foreign_retention 表
        // 10 10 * * * sh /data/web-server/openresty/nginx/html/reportingApi/talkingdata/sc/talkingdata_retention.sh
        header('content-type:text/html;charset=utf-8');
        set_time_limit(0);

        define('AD_PLATFORM', 'TalkingData');
        define('SOURCE_ID', 'ptj02'); // todo 这个需要根据平台信息表确定平台ID
        // 入口方法
    	$day = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-3 day'));
        var_dump($day);

//        $sql = "select * from plat_tj_code where plat_id='ptj02'"; // 新加plat_id字段来区分td
//        $sql = "select DISTINCT app_key,app_id,app_name,platform_id,channel_id,channel_name,channel_type from `plat_tj_code` WHERE `plat_id`='ptj02'";

        $sql = "select * from plat_tj_code where plat_id='ptj02'  and app_name = '铁头英雄'"; // 测试
        $sql = "select DISTINCT app_key,app_id,app_name,platform_id,channel_id,channel_name,channel_type from `plat_tj_code` WHERE `plat_id`='ptj02' and app_name = '铁头英雄' group by app_id,app_key,country_type"; // 测试
        $result = DB::select($sql);
        $result = Service::data($result);
        foreach ($result as $row) {
            // var_dump($row);
            unset($channel);
            unset($version);
            unset($metrics_id);
            unset($groupby);
            unset($gameinfo);
            $channel = array ();
            $version = array ();
            $gameinfo[$row['app_key']] = $row['app_id'];
//            var_dump($gameinfo);

            $channelInfo = TdDataFunction::getChannelList ( $row ['app_key'], $row ['platform_id'] );
            if ($channelInfo['status'] != 200) {
                TdDataFunction::saveErrorLog($channelInfo['message']);
                return;
            }
            foreach ( $channelInfo ['result'] as $d ) {
                $channel [$d ['channelid']] = $d ['channelname'];
                $groupby = "daily";
                TdDataFunction::foreignCreateTetention( $row ['app_key'], $gameinfo, $row ['platform_id'], $day, $channel, '', $groupby ); // 获取留存率数据
            }
        }
        self::saveLog(AD_PLATFORM, $day. 'retention endtime:'.date('Y-m-d H:i:s'));
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
