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

class TalkdataForeignTotalSessionTjCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'TalkdataForeignTotalSessionTjCommond {dayid?}';

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
        // 维护 talkingdata_foreign_channel 表
        // 0 2 * * * sh /data/web-server/openresty/nginx/html/reportingApi/talkingdata/sc/talkingdata_session2.sh
        // 0 6 * * * sh /data/web-server/openresty/nginx/html/reportingApi/talkingdata/sc/talkingdata_session2.sh

        header('content-type:text/html;charset=utf-8');
        set_time_limit(0);

        define('AD_PLATFORM', 'TalkingData');
        define('SOURCE_ID', 'ptj02'); // todo 这个需要根据平台信息表确定平台ID
        // 入口方法
    	$day = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-2 day'));
        var_dump($day);

        //先删除数据
//        DB::delete("delete from talkingdata_foreign_sessionlength where day='{$day}'");

//        $result = DB::select("select * from plat_tj_code where plat_id='ptj02' "); // todo 正式
        $result = DB::select("select * from plat_tj_code where plat_id='ptj02' and app_name = '铁头英雄'  group by app_id,app_key,country_type"); // todo 测试
        $result = Service::data($result);
        foreach ($result as $row){
            //var_dump($row);
            unset ($channel);
            unset($version);
            unset($metrics_id);
            unset($groupby);
            unset($gameinfo);

            $groupby = "channelid";

            $gameinfo[$row['app_id']] = $row['app_name'];
            var_dump($gameinfo);
            TdDataFunction::foreignCreateSessionData($row['app_key'],$gameinfo,$row['platform_id'],$day,$groupby);
        }

//        $sum_result = DB::select("select day,count(id) c,sum(session) session,sum(sessionlength) sessionlength from talkingdata_foreign_sessionlength where day='{$day}' ");
//        $sum_row = Service::data($sum_result);
//
//        self::saveLog(AD_PLATFORM, $day.'new_active endnum:'.json_encode($sum_row));
//        self::saveLog(AD_PLATFORM, $day. 'new_active endtime:'.date('Y-m-d H:i:s'));
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
