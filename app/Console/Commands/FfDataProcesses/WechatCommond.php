<?php

namespace App\Console\Commands\FfDataProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\BusinessLogic\DataImportLogic;
use App\Common\CommonFunction;
use App\Common\CurlRequest;
use App\Common\ParseDayid;
use App\Common\Service;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;

class WechatCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'WechatCommond {dayid?} {account?}';

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

        define('AD_PLATFORM', '微信');
        define('SCHEMA', 'ff_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'pff04'); // todo 这个需要根据平台信息表确定平台ID
        try {

            // todo 以后从数据库获取  目前写死
            $dayid = $this->argument('dayid') ? $this->argument('dayid') : date('Y-m-d', strtotime('-1 day'));
            $insert_data = [];
            $date = date('Ymd', strtotime($dayid));
            $month = date('Ym', strtotime($dayid));
            $sql = " SELECT
            substr(DATE_FORMAT(a.apply_time,'%Y%m%d%h%i%s'),1,8) AS date_time,
            'wxbb0157308b290e1d' AS appid,
            a.gameid,
            a.imei AS idfa,
            a.version,
            a.channel AS channelid,
            DATE_FORMAT(a.pay_time, '%Y%m%d%H%i%s') AS pay_time,
            a.point_fee AS total_fee,
            a.pay_result AS payresult,
            now() as  now,
            a.address AS position,
            'CN' AS country
            FROM
            zplay_account.singleorder_$month a
            WHERE
            date_format(a.apply_time, '%Y%m%d') = $date
            ";
            $info = DB::connection('mysql_zhifubao')->select($sql);
            $info = Service::data($info);
            if (!$info) return;

            $map = [];
            $map['dayid'] = $dayid;
            $map['source_id'] = SOURCE_ID;
            $bool = DataImportLogic::deleteHistoryData(SCHEMA, TABLE_NAME, $map);

            $create_time = date("Y-m-d H:i:s", time());

            foreach ($info as $k => $v) {
                $insert_data[$k]['type'] = 2;
                $insert_data[$k]['app_id'] = '';
                $insert_data[$k]['app_name'] = '';
                $insert_data[$k]['account'] = '';
                $insert_data[$k]['source_id'] = SOURCE_ID;
                $insert_data[$k]['json_data'] = json_encode($v);
                $insert_data[$k]['dayid'] = $dayid;
                $insert_data[$k]['create_time'] = $create_time;
                $insert_data[$k]['year'] = date("Y", strtotime($dayid));
                $insert_data[$k]['month'] = date("m", strtotime($dayid));

            }


            if ($insert_data) {

                //拆分批次
                $step = array();
                $i = 0;
                foreach ($insert_data as $kkkk => $insert_data_info) {
                    if ($kkkk % 1000 == 0) $i++;
                    if ($insert_data_info) {
                        $step[$i][] = $insert_data_info;
                    }
                }

                $is_success = [];
                if ($step) {
                    foreach ($step as $k => $v) {
                        $result = DataImportLogic::insertChannelData(SCHEMA, TABLE_NAME, $v);
                        if (!$result) {
                            $is_success[] = $k;
                        }
                    }
                }

                Artisan::call('WechatFfHandleProcesses', ['dayid' => $date]);
            }
        }catch (\Exception $e) {
            $error_msg_info = $date.'号,'.AD_PLATFORM.'付费平台程序失败，失败原因：'.$e->getMessage();
            DataImportImp::saveDataErrorLog(5,SOURCE_ID,AD_PLATFORM,2,$error_msg_info);

        }

    }
}