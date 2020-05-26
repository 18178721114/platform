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

class OutTGPlatProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'OutTGPlatProcesses {begin_date?} {end_date?}';

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

        $begin_date = $this->argument('begin_date') ? $this->argument('begin_date') : date('Y-m-d',strtotime('-8 day'));
        $end_date = $this->argument('end_date') ? $this->argument('end_date') : date('Y-m-d');

        DB::beginTransaction();
        $sel_sql = "select count(1) as count  FROM
        zplay_basic_tg_plat_report_total
        WHERE
         date >= '$begin_date'  and   date <= '$end_date' ";
        $sel_info = DB::select($sel_sql);
        $sel_info = Service::data($sel_info);
        if($sel_info[0]['count'] !=0){
            $del_sql ="DELETE
            FROM
            zplay_basic_tg_plat_report_total
            WHERE
            date >= '$begin_date'
            AND date <= '$end_date'";
            $update_info =DB::delete($del_sql);

            if(!$update_info){
                DB::rollBack();
            }
        }

        $insert_sql ="
        INSERT INTO zplay_basic_tg_plat_report_total (
            date,
            app_id,
            os_id,
            country_id,
            device,
            tg_type,
            platform_id,
            impression,
            click,
            new,
            cost,
            new_phone,
            new_pad,
            create_time,
            company_id
        ) SELECT
            t.date,
            app.id,
            app.os_id,
            t.country_id,
            (
                CASE
                WHEN t.device_type = 'tablet' THEN
                    2
                WHEN t.device_type = 'pad' THEN
                    2
                WHEN t.device_type = 'phone' THEN
                    1
                ELSE
                    3
                END
            ) AS device_type,
            t.type,
            t.platform_id,
            sum(t.impression) AS impression,
            sum(t.click) AS click,
            sum(t.new) AS new,
            round(sum(t.cost_exc), 2) AS cost,
            sum(t.new_phone) AS new_phone,
            sum(t.new_pad) AS new_pad,
            now(),
            app.company_id
        FROM
            zplay_tg_report_daily t
        LEFT JOIN c_app app ON t.app_id = app.app_id
        WHERE
            t.tongji_type = 0
        AND t.platform_id <> 'ptg279'
        AND t.date >= '$begin_date'
        AND t.date <= '$end_date'
        and t.app_id not in (select app_id from  c_app_show where promotion_on = 0)
        GROUP BY
            t.date,
            t.app_id,
            t.type,
            t.platform_id,
            app.os_id,
            t.country_id,
            t.device_type";
            $insert_info_1 = DB::insert($insert_sql);

            if(!$insert_info_1){
                //var_dump(3);
                DB::rollBack();
            }
            DB::commit();


    }
}