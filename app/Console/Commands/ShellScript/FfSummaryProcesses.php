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

class FfSummaryProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'FfSummaryProcesses {begin_date?}  {end_date?} {platform_id?}';

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
        if($platform_id){
            $where_platform = "  and platform_id = '$platform_id'";
        }

        DB::beginTransaction();
        $sel_sql = "select count(1) as count  FROM
        zplay_basic_report_daily
        WHERE
         plat_type = 'ff' and 
         date_time >= '$begin_date'  and   date_time <= '$end_date' ".$where_platform;
        $sel_info = DB::select($sel_sql);
        $sel_info = Service::data($sel_info);
        if($sel_info[0]['count'] !=0){
            $del_sql ="DELETE
            FROM
                zplay_basic_report_daily
            WHERE
                plat_type = 'ff'
            AND date_time >= '$begin_date'
            AND date_time <= '$end_date' ".$where_platform;
            $update_info =DB::delete($del_sql);

            if(!$update_info){
                DB::rollBack();
            }
        }

        $insert_sql ="INSERT INTO zplay_basic_report_daily (
            game_creator,
            os_id,
            game_category_id,
            game_group,
            plat_type,
            date_time,
            app_id,
            channel_id,
            country_id,
            province_id,
            platform_id,
            earning_type,
            pay_user,
            pay_time,
            earning_fix_ff,
            income_fix_ff,
            create_time,
            income_usd_ff,
            earning_usd_ff
        ) SELECT
            app.company_id AS game_creator,
            app.os_id AS os_id,
            app.app_category_id AS game_category_id,
            app.release_group AS game_group,
            'ff' AS plat_type,
            ff.date,
            app.id AS app_id,
            channel.id AS channel_id,
            ff.country_id,
            ff.province_id,
            ff.platform_id,
            (
                CASE
                WHEN ff.pay_type = 1 THEN
                    'D'
                ELSE
                    'M'
                END
            ) AS earning_type,
            (
                CASE
                WHEN sum(ff.pay_user) IS NULL THEN
                    0
                ELSE
                    sum(ff.pay_user)
                END
            ) AS pay_user,
            (
                CASE
                WHEN sum(ff.pay_time) IS NULL THEN
                    0
                ELSE
                    sum(ff.pay_time)
                END
            ) AS pay_time,
            sum(ff.earning_fix) AS earning_fix_ff,
            sum(ff.income_fix) AS income_fix_ff,
            now(),
            sum(ff.income_fix) AS income_usd_ff,
            sum(ff.earning_fix) AS earning_usd_ff
        FROM
            zplay_ff_report_daily ff
        LEFT JOIN c_app app ON ff.app_id = app.app_id
        LEFT JOIN c_channel channel ON ff.channel_id = channel.channel_id
        WHERE
ff.earning_fix !=0 and ff.income_fix !=0 and 
            ff.tongji_type > -1 and 
            ff.date >= '$begin_date'
        AND ff.date <= '$end_date' $where_platform
        GROUP BY
            ff.date,
            ff.app_id,
            ff.channel_id,
            ff.country_id,
            ff.province_id,
            ff.platform_account,
            ff.platform_id,
            ff.publisher_id";
        $insert_info_1 = DB::insert($insert_sql);

        if(!$insert_info_1){
            //var_dump(3);
            DB::rollBack();
        }
        $update_sql= "UPDATE zplay_basic_report_daily usd,
        c_currency_ex cur
        SET usd.earning_usd_ff = usd.earning_fix_ff / cur.currency_ex
        WHERE
        cur.`effective_time` = date_format(usd.date_time, '%Y%m')
        AND cur.`currency_id` = 60
        AND usd.earning_usd_ff = usd.earning_fix_ff
        AND usd.`plat_type` = 'ff' and usd.date_time >= '$begin_date'  and   usd.date_time <= '$end_date' $where_platform";
        $update_sql_res =DB::update($update_sql);
//        if (!$update_sql_res){
//            DB::rollBack();
//        }

        $update_sql_1= "UPDATE zplay_basic_report_daily usd,
        c_currency_ex cur
        SET usd.income_usd_ff = usd.income_fix_ff / cur.currency_ex
        WHERE
        cur.`effective_time` = date_format(usd.date_time, '%Y%m')
        AND cur.`currency_id` = 60
        AND usd.income_usd_ff = usd.income_fix_ff
        AND usd.`plat_type` = 'ff' and usd.date_time >= '$begin_date'  and   usd.date_time <= '$end_date' $where_platform";
        $update_sql_1_res =DB::update($update_sql_1);
//        if (!$update_sql_1_res){
//            DB::rollBack();
//        }

        DB::commit();


    }
}