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
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\DataImportLogic;

class TalkdataChinaUserCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'TalkdataChinaUserCommand {dayid?}';

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
        // 维护 talkingdata_china_user 表
        // 数据组 每天 跑的basedata
        // 30 4 * * * cd /data/www/ppgz/z+/talkdata/new;/usr/bin/php td_base_data.php

        header('content-type:text/html;charset=utf-8');
        set_time_limit(0);

        define('AD_PLATFORM', 'TalkingData');
        define('SOURCE_ID', 'ptj02'); // todo 这个需要根据平台信息表确定平台ID
        // 入口方法
        $day = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));
        var_dump($day);

        //获取需要查询的gameid
//        $sql = "select * from plat_tj_gnjf_code_new where plat_id='ptj02'"; // todo pro
        // todo 新系统配置查询
//        $sql = "SELECT DISTINCT
//            c_app_statistic.td_app_id as app_id,
//            c_app_statistic.access_key as app_key,
//            c_app_statistic_version.statistic_app_name as app_name,
//            if(c_app.os_id = 1,2,1) as platform_id
//            FROM
//            c_app
//            LEFT JOIN c_app_statistic ON c_app.id = c_app_statistic.app_id
//            LEFT JOIN c_app_statistic_version ON c_app_statistic.id = c_app_statistic_version.app_statistic_id
//            WHERE
//            c_app_statistic.statistic_type = 2
//            AND c_app_statistic_version.ad_status != 2
//            AND c_app.`release_region_id` = 3
//            AND c_app.os_id = 2";

        $sql = "select * from plat_tj_gnjf_code_new where plat_id='ptj02' and app_name = '铁头英雄' group by app_id,app_key,country_type"; // todo test
        $result = DB::select($sql);
        $result = Service::data($result);
        $rows = array();
        foreach ($result as $row) {
            //var_dump($row);
            $rows[] = $row;
        }

        var_dump(count($rows));
        foreach ($rows as $gameinfo) {
            var_dump($gameinfo['id'] . '-' . $gameinfo['app_name']);
            self::saveLog(AD_PLATFORM, date('Y-m-d H:i:s') . "：talkingdata_base_data($day)-({$gameinfo['id']}) {$gameinfo['app_id']}-{$gameinfo['app_name']} is ok");

            TdDataFunction::getNewBaseDataGroup($gameinfo, $day);
            TdDataFunction::getBaseDataGroup_orther($gameinfo, $day);
        }

        // 调用处理过程
        Artisan::call('TdUserTjHandleProcesses' ,['dayid'=>$day]);
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
        $logFilename = $dir.'/'.$platform_name.'_talkingdata_base_data'.'.log';
        //生成日志
        file_put_contents( $logFilename,$message . "\n\n",FILE_APPEND);
    }

    private static function getAppid($game_ids)
    {
        $game = implode("','", $game_ids);
        $sql = "SELECT app_id FROM plat_tj_gnjf_code WHERE game_id IN ('$game');";
        $result = DB::query($sql);
        $result = Service::data($result);
        $res = array();
        foreach ($result as $row) {
            $res[] = $row['app_id'];
        }
        return array_unique($res);
    }
}
