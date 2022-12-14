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

class OutTGCountryNewProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'OutTGCountryNewProcesses {begin_date?} {end_date?}';

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
        $begin_date = $this->argument('begin_date') ? $this->argument('begin_date') : date('Y-m-d', strtotime('-8 day'));
        $end_date = $this->argument('end_date') ? $this->argument('end_date') : date('Y-m-d');

        try {
            DB::beginTransaction();
            $sel_sql = "select count(1) as count  FROM
        zplay_basic_tg_country_new
        WHERE
         date >= '$begin_date'  and   date <= '$end_date' ";
            $sel_info = DB::select($sel_sql);
            $sel_info = Service::data($sel_info);
            if ($sel_info[0]['count'] != 0) {
                $del_sql = "         DELETE
            FROM
                zplay_basic_tg_country_new
            WHERE
                date BETWEEN '$begin_date'
            AND '$end_date';";
                $update_info = DB::delete($del_sql);

                if (!$update_info) {
                    DB::rollBack();
                }
            }

            $insert_sql = "
        INSERT INTO zplay_basic_tg_country_new (
                date,
                app_id,
                country_id,
                new_nonature,
                new_nature,
                new_total,
                company_id,
                create_time
            ) SELECT
	a.date,
	a.app_id as app_id,
	a.country_id,
	sum( a.new_nonature ) AS new_nonature,
	sum( a.new_total )- sum( a.new_nonature ) AS new_nature,
	sum( a.new_total ) AS new_total,
	a.company_id,
	now() 
FROM
	(
	SELECT
		t.date,
		app.id as app_id,
		country_id,
		sum( t.new ) AS new_nonature,
		0 AS new_total,
		app.company_id,
		now() 
	FROM
		zplay_tg_report_daily t
		LEFT JOIN c_app app ON t.app_id = app.app_id 
	WHERE
		t.tongji_type = 0 
		AND t.platform_id <> 'ptg279'
		AND t.date >= '$begin_date' 
		AND t.date <= '$end_date' 
	GROUP BY
		t.date,
		t.app_id,
		t.country_id 
	UNION ALL
	SELECT
		t.date,
		app.id as app_id,
	IF
		( t.type = 1, 64, t.country_id ) AS country_id,
		0 AS new_nonature,
		sum( t.new_user ) AS new_total,
		app.company_id,
		now() 
	FROM
		zplay_user_tj_report_daily t
		LEFT JOIN c_app app ON t.app_id = app.app_id 
	WHERE
		t.date BETWEEN '$begin_date' 
		AND '$end_date' 
	GROUP BY
		t.date,
		t.app_id,
		t.country_id 
	) a
	where a.new_nonature !=0 or a.new_total  !=0 and a.app_id not in (select app_key from  c_app_show where promotion_on = 0)
GROUP BY
	a.date,
	a.app_id,
	a.country_id  
        ";
            $insert_info_1 = DB::insert($insert_sql);

            if (!$insert_info_1) {
                //var_dump(3);
                DB::rollBack();
            }
            DB::commit();
        }catch (\Exception $e) {
            // ????????????
            $message = date("Y-m-d")."???,????????????????????????,????????????:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, 'ptg-003', '????????????', 4, $message);
            $error_msg_arr[] = $message;
//            CommonFunction::sendMail($error_msg_arr, '??????????????????error');
            exit;
        }
    }
}