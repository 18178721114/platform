<?php

namespace App\Console\Commands\ShellScript;

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
use Illuminate\Support\Facades\Redis;
use App\BusinessImp\PlatformImp;

class RedisBreakAfPushProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'RedisBreakAfPushProcesses {dayid?} ';

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
    	Redis::select(2);
    	define('MYSQL_AD_TABLE_NAME','af_active_new_ios');
        $af_idfa = env('REDIS_APPSFLYER_KEYS_BREAK');

        try {
            //获取广告数据长度
            $ad_len = Redis::llen($af_idfa);

            if ($ad_len > 0) {

                $be_time = time();


                $ad_sql = "insert into " . MYSQL_AD_TABLE_NAME . " ( `active_id`, `application_id`, `mac`, `idfa`, `device_key`, `cronid`, `intime`, `status`) VALUES ";
                for ($i = 1; $i <= $ad_len; $i++) {
                    $str = Redis::lpop($af_idfa);
                    if ($str) {
                        $idfa_arr = json_decode($str,true);
                        $af_idfa = isset($idfa_arr['idfa']) ? $idfa_arr['idfa']: '';
                        $af_install_time = isset($idfa_arr['install_time']) ? strtotime($idfa_arr['install_time']): '';
                        if ($af_idfa && $af_install_time) {
                            $sel_sql = "select * from  " . MYSQL_AD_TABLE_NAME . " where device_key ='{$af_idfa}' ";
                            $info = DB::connection('mysql_channel')->select($sel_sql);
                            $info = Service::data($info);
                            if ($info) {
                                continue;
                            }

                            $insert_str = "('',1,'','{$af_idfa}','{$af_idfa}','',$af_install_time,0)";
                            DB::connection('mysql_channel')->insert($ad_sql . $insert_str);
                        }

                    }
                }


            }
        }catch (\Exception $e) {
            // 异常报错
            $message = date("Y-m-d")."号,Redis越狱渠道af-push程序报错,报错原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, 'pad-001', 'Redis越狱渠道',2, $message);
            exit;
        }
    }
}