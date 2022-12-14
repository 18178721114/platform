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

class RedisAppsflyerProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'RedisAppsflyerProcesses  {dayid?} {hours?}';

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
        try{
            set_time_limit(0);
            Redis::select(2);

            $dayid = $this->argument('dayid') ? $this->argument('dayid'):date('Y-m-d');
            $hours = $this->argument('hours') ? $this->argument('hours'):date("H", time()) - 1;

            define('SCHEMA', 'appsflyer_push_data');
            define('TABLE_NAME', 'erm_data');

            $appsflyer_key = env('REDIS_APPSFLYER_KEYS');

            //获取广告数据长度
            $appsflyer_len = Redis::llen($appsflyer_key);

            $start_time = time();
            var_dump('开始时间：'.$start_time);
            $insert_date = [];
            var_dump($appsflyer_len);
            for ($i = 1; $i <= $appsflyer_len; $i++) {
                $str_json = Redis::lpop($appsflyer_key);
                $insert_date[] = $str_json;
            }
            var_dump(count($insert_date));
            $new_insert_date = [];
            if ($insert_date){
                $ii = 0;
                foreach ($insert_date as $str_key => $str){
                    if ($str_key % 500 == 0) $ii++;
                    $one_insert_data = [
                        "dayid" => $dayid,
                        "json_data" => $str,
                        "year" => date("Y", strtotime($dayid)),
                        "month" => date("m", strtotime($dayid)),
                        "hours" => $hours,
                        'create_time' => date("Y-m-d H:i:s")
                    ];
                    $new_insert_date[$ii][] = $one_insert_data;
                }
            }

            var_dump(count($new_insert_date));
            if ($new_insert_date) {
                foreach ($new_insert_date as $k => $v_info) {
                    $result = DataImportLogic::insertChannelData("appsflyer_push_data", "erm_data", $v_info);
                }
            }

            $end_time = time();
            var_dump('结束时间：'.$end_time);
            var_dump('用时：'.($end_time - $start_time));
        }catch (\Exception $e) {
            // 异常报错
            $message = date("Y-m-d")."号,Redis越狱渠道af-push-pgsql程序报错,报错原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, 'pad-001', 'Redis越狱渠道',2, $message);
            exit;
        }
    }
}