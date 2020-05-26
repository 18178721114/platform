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

class TgSummaryProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'TgSummaryProcesses {begin_date?}  {end_date?} {platform_id?}';

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
        $where_platform = '';
        if($platform_id){
            $where_platform = "  and platform_id = '$platform_id'";
        }

        DB::beginTransaction();
        $sel_sql = "select count(1) as count  FROM
        zplay_basic_report_daily
        WHERE
         plat_type = 'ct' and 
         date_time >= '$begin_date'  and   date_time <= '$end_date' ".$where_platform;
        $sel_info = DB::select($sel_sql);
        $sel_info = Service::data($sel_info);
        if($sel_info[0]['count'] !=0){
            $del_sql ="DELETE
            FROM
                zplay_basic_report_daily
            WHERE
                plat_type = 'ct'
            AND date_time >= '$begin_date'
            AND date_time <= '$end_date' ".$where_platform;
            $update_info =DB::delete($del_sql);

            if(!$update_info){
                DB::rollBack();
            }
        }

        $insert_sql ="INSERT INTO zplay_basic_report_daily (
        earning_type,
        game_creator,
        os_id,
        game_category_id,
        game_group,
        plat_type,
        date_time,
        app_id,
        cost_new,
        cost_tg,
        create_time,
        platform_id,
        country_id,
        cost_usd_tg,
        channel_id
        ) SELECT
        'T' as earning_type,
        app.company_id AS game_creator,
        app.os_id AS os_id,
        app.app_category_id AS game_category_id,
        app.release_group AS game_group,
        'ct' AS plat_type,
        tg.date,
        app.id AS app_id,
        sum(tg.new) AS cost_new,
        sum(tg.cost_exc) AS cost_tg,
        now(),
        tg.platform_id,
        tg.country_id,
        sum(tg.cost_usd) AS cost_usd_tg,
        c_channel.id
        FROM
        zplay_tg_report_daily tg
        LEFT JOIN c_app app ON tg.app_id = app.app_id
        LEFT JOIN c_channel  ON tg.channel_id = c_channel.channel_id
        WHERE
        tg.cost !=0 and  tg.cost_exc !=0 and
        tg.date >= '$begin_date'
        AND tg.date <= '$end_date'   $where_platform 
        GROUP BY
        tg.date,
        tg.app_id,
        tg.country_id,
        tg.agency_platform_id,
        tg.platform_id,
        tg.data_account,
        tg.ad_type,
		tg.channel_id";
        $insert_info_1 = DB::insert($insert_sql);

//        if(!$insert_info_1){
//            //var_dump(3);
//            DB::rollBack();
//        }
//        $update_sql= "UPDATE zplay_basic_report_daily usd,
//        c_currency_ex cur
//        SET usd.cost_usd_tg = usd.cost_tg / cur.currency_ex
//        WHERE
//        cur.`effective_time` = date_format(usd.date_time, '%Y%m')
//        AND cur.`currency_id` = 60
//        AND usd.cost_usd_tg = usd.cost_tg
//        AND usd.`plat_type` = 'ct' and usd.date_time >='$begin_date'   and   usd.date_time <= '$end_date' $where_platform";
//        $update_info =DB::update($update_sql);
//        if (!$update_info){
//            DB::rollBack();
//        }

        DB::commit();


    }
}