<?php

namespace App\Console\Commands\TjDataProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\Common\CommonFunction;
use App\Common\CurlRequest;
use App\Common\Service;
use App\Common\TdDataFunction;
use App\Common\TdDataNewFunction;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\DataImportLogic;

class TalkdataChinaKeepUserCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'TalkdataChinaKeepUserCommond {dayid?}';

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
        // ===== start
        // 维护 talkingdata_china_keepuser 表
        // 从talkingdata获取数据插入到konglog talkingdata_keepuser表中
        // 4 6 * * * cd /data/www/ppgz/z+/talkdata/new;/usr/bin/php user_keep.php

        set_time_limit(0);
        define('AD_PLATFORM', 'TalkingData');
        define('SOURCE_ID', 'ptj02'); // todo 这个需要根据平台信息表确定平台ID

        // 入口方法
    	$day = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-2 day'));
        $days = [$day];
//        $sum_sql = "select * from plat_tj_gnjf_code where plat_id='ptj02'"; // todo 正式
//        $sum_sql = "select * from plat_tj_gnjf_code where plat_id='ptj02'  and app_name = '铁头英雄'"; // todo 测试
        $sum_sql = "SELECT DISTINCT
                    if(c_app.os_id = 1,2,1) as platform_id,
                    c_app_statistic.td_app_id as app_id,
                    c_app_statistic.access_key as app_key,
                    c_app_statistic_version.statistic_app_name as app_name
                    FROM
                    c_app_statistic
                    LEFT JOIN c_app_statistic_version ON c_app_statistic.id = c_app_statistic_version.app_statistic_id
                    LEFT JOIN c_app ON c_app.id = c_app_statistic.app_id
                    LEFT JOIN c_channel ON c_channel.id = c_app_statistic_version.channel_id
                    WHERE
                    c_app_statistic.statistic_type = 2
                    AND c_app_statistic_version.ad_status != 2 
                    AND c_app_statistic_version.statistic_app_name = '铁头英雄' "; // todo 正式删除

        $result = DB::select($sum_sql);
        $result = Service::data($result);
        $rows = [];
        foreach ($result as $row){
            $k = $row['app_key'] . $row['platform_id'];
            $rows[$k] = $row;
        }

        foreach ($days as $day) {
            foreach ($rows as $row) {
                $gameinfo = array(
                    'app_key' => $row['app_key'],
                    'app_id' => $row['app_id'],
                    'app_name' => $row['app_name'],
                    'platform_id' => $row['platform_id'],
                );
                TdDataNewFunction::getKeepUser($gameinfo, $day);
            }
        }

        // 调用处理过程
        Artisan::call('TdKeepTjHandleProcesses' ,['dayid'=>$day]);
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
