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

class AdSummaryProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'AdSummaryProcesses {begin_date?}  {end_date?} {platform_id?}';

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
        $begin_date = $this->argument('begin_date') ? $this->argument('begin_date'):date('Y-m-d',strtotime('-1 day'));
        $end_date = $this->argument('end_date') ? $this->argument('end_date'):date('Y-m-d',strtotime('-1 day'));
        $platform_id = $this->argument('platform_id') ? $this->argument('platform_id'):'';

        try {
            if ($platform_id) {
                $where_platform = "  and platform_id = '$platform_id'";
            }

            DB::beginTransaction();
            $sel_sql = "select count(1) as count  FROM
        zplay_basic_report_daily
        WHERE
         plat_type = 'ad' and 
         date_time >= '$begin_date'  and   date_time <= '$end_date' " . $where_platform;
            $sel_info = DB::select($sel_sql);
            $sel_info = Service::data($sel_info);
            if ($sel_info[0]['count'] != 0) {
                $del_sql = "DELETE
            FROM
                zplay_basic_report_daily
            WHERE
                plat_type = 'ad'
            AND date_time >= '$begin_date'
            AND date_time <= '$end_date' " . $where_platform;
                $update_info = DB::delete($del_sql);

                if (!$update_info) {
                    DB::rollBack();
                }
            }

            // 更新 没有请求数 有成功请求数
            $basic_update_sql = "UPDATE zplay_ad_report_daily  set all_request =success_requests WHERE  (all_request is NULL or all_request = 0) and `success_requests` > 0 and plat_type = 'ad' and date >= '$begin_date'  and   date <= '$end_date' ";
            DB::update($basic_update_sql);

            $basic_insert_sql = "INSERT INTO zplay_basic_report_daily (
                game_creator,
                os_id,
                game_category_id,
                game_group,
                plat_type,
                platform_id,
                date_time,
                app_id,
                country_id,
                earning_type,
                request_all,
                request,
                impression,
                click,
                earning_fix_ad,
                income_fix_ad,
                create_time,
                flow_type,
                statistics,
                channel_id, 
                income_usd_ad,
                earning_usd_ad
            )
            SELECT
                app.company_id AS game_creator,
                app.os_id AS os_id,
                app.app_category_id AS game_category_id,
                app.release_group AS game_group,
                'ad' AS plat_type,
                ad.platform_id,
                ad.date,
                app.id,
                ad.country_id,
                ad.ad_type AS earning_type,
                sum(ad.all_request) AS request_all,
                sum(ad.success_requests) AS request,
                sum(ad.impression) AS impression,
                sum(ad.click) AS click,
                sum(ad.earning_flowing) AS earning_fix_ad,
                sum(ad.earning_fix) AS income_fix_ad,
                now(),
                ad.flow_type,
                ad.statistics,
                channel.id AS channel_id,
                sum(ad.earning_exc_usd) AS income_usd_ad,
                sum(ad.earning_usd) AS earning_usd_ad
            FROM
                zplay_ad_report_daily ad
            LEFT JOIN c_app app ON ad.app_id = app.app_id
            LEFT JOIN c_channel channel ON ad.channel_id = channel.channel_id
            WHERE
                ad.date >= '$begin_date'  and   ad.date <= '$end_date'  $where_platform 
            GROUP BY
                ad.date,
                ad.app_id,
                ad.country_id,
                ad.platform_id,
                ad.ad_type,
                ad.flow_type,
                ad.statistics";
            $insert_info_1 = DB::insert($basic_insert_sql);
            if (!$insert_info_1) {
                DB::rollBack();
            }

            DB::commit();
        }catch (\Exception $e) {
            // 异常报错
            if (!$platform_id){
                $platform_id = 'pad-002';
            }
            $source_name = 'AdSummary';
            $message = "{$end_date}号, " . $source_name . "程序报错,报错原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, $platform_id, $source_name, 2, $message);
            $error_msg_arr[] = $message;
//            CommonFunction::sendMail($error_msg_arr, '广告平台程序error');
            exit;
        }

    }
}