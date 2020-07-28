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

class BreakMatchProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'BreakMatchProcesses {dayid?} ';

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
    	define('MYSQL_AD_TABLE_NAME','af_active_new_ios');

    	//用af的的数据 匹配 越狱渠道上报数
        //af的 的一条数据 的时间  去匹配 往前推12 小时

        try {
            $begin_time = time();


            $sql = "select * from af_active_new_ios where  status = 0 and intime <= '$begin_time'";
            $info = DB::connection('mysql_channel')->select($sql);
            $info = Service::data($info);
            foreach ($info as $k =>$v){
                $time = $v['intime']+12*60;
                $sql_match ="select * from channel_request_ios01 where intime>= '{$time}' and intime>= '{$v['intime']}' limit 1 ";
                $info1 = DB::connection('mysql_channel')->select($sql_match);
                $info1 = Service::data($info1);
                if($info1){
                    $insert_sql = "insert into af_match (idfa,intime) VALUES ('{$v['idfa']}','{$v['intime']}')";
                    DB::connection('mysql_channel')->insert($insert_sql);
                }

            }
            $sql = "update  af_active_new_ios set status =1  where  status = 0 and intime <= '$begin_time'";
            DB::connection('mysql_channel')->update($sql);
        }catch (\Exception $e) {
            // 异常报错
            exit;
        }
    }
}