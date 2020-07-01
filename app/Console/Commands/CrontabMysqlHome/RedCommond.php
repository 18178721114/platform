<?php

namespace App\Console\Commands\CrontabMysqlHome;

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

class RedCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'RedCommond {dayid?} {account?}';

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
        try {
            set_time_limit(0);
            $mysql_table = 'zplay_red_data_statistics';
            //红包数据
            $dayid = $this->argument('dayid') ? $this->argument('dayid') : date('Y-m-d', strtotime('-1 day'));
            $sql = " select * from data_statistics where date_time = '$dayid'";
            $info = DB::connection('mysql_zhifubao')->select($sql);
            $info = Service::data($info);
            if (!$info) return;

            DB::beginTransaction();
            $sel_sql = "select count(1) as count  FROM
        $mysql_table
        WHERE
         date_time = '$dayid' ";
            $sel_info = DB::select($sel_sql);
            $sel_info = Service::data($sel_info);
            if ($sel_info[0]['count'] != 0) {
                $del_sql = "DELETE
            FROM
                $mysql_table
            WHERE date_time = '$dayid' ";
                $delete_info = DB::delete($del_sql);

                if (!$delete_info) {
                    DB::rollBack();
                }
            }

            $app_sql = "select id ,app_id  FROM c_app";
            $app_info = DB::select($app_sql);
            $app_info = Service::data($app_info);


            $create_time = date("Y-m-d H:i:s", time());

            foreach ($info as $k => $v) {
                foreach ($app_info as $a => $b) {
                    if ($v['game_id'] == $b['app_id']) {
                        $insert_data[$k]['app_id'] = $b['id'];
                        continue;
                    }
                }
                $insert_data[$k]['date_time'] = $v['date_time'];
                $insert_data[$k]['all_card_count'] = $v['all_card_count'];
                $insert_data[$k]['all_user_count'] = $v['all_user_count'];
                $insert_data[$k]['all_cat_count'] = $v['all_cat_count'];
                $insert_data[$k]['all_cat_user_count'] = $v['all_cat_user_count'];
                $insert_data[$k]['game_total_amount'] = $v['game_total_amount'];
                $insert_data[$k]['all_9cat_user_count'] = $v['all_9cat_user_count'];
                $insert_data[$k]['all_today_total'] = $v['all_today_total'];
                $insert_data[$k]['today_red_bags_user_count'] = $v['today_red_bags_user_count'];
                $insert_data[$k]['tixian_total'] = $v['tixian_total'];
                $insert_data[$k]['all_send_money'] = $v['all_send_money'];
                $insert_data[$k]['create_time'] = $create_time;

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
                        $result = DataImportLogic::insertAdReportInfo($mysql_table, $v);
                        if (!$result) {
                            $is_success[] = $k;
                        }
                    }
                }

                DB::commit();


            }
        }catch (\Exception $e) {
            // 异常报错
            $message = date("Y-m-d")."号,红包数据程序报错,报错原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, 'pad-001', '红包数据', 2, $message);
            $error_msg_arr[] = $message;
            CommonFunction::sendMail($error_msg_arr, '红包数据');
            exit;
        }

    }
}