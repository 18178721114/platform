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

class TjSummaryProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'TjSummaryProcesses {begin_date?}  {end_date?} {platform_id?}';

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
        $begin_date = $this->argument('begin_date') ? $this->argument('begin_date'):date('Y-m-d',strtotime('-35 day'));
        $end_date = $this->argument('end_date') ? $this->argument('end_date'):date('Y-m-d',strtotime('-1 day'));
        $platform_id = $this->argument('platform_id') ? $this->argument('platform_id'):'';

        try {

            if ($platform_id) {
                $where_del_platform = "  and platform_id = '$platform_id'";
                $where_platform = "  and tj.platform_id = '$platform_id'";
            }

            DB::beginTransaction();
            $sel_sql = "select count(1) as count  FROM
        zplay_basic_report_daily tj
        WHERE
         plat_type = 'tj' and 
         date_time >= '$begin_date'  and   date_time <= '$end_date' " . $where_platform;
            $sel_info = DB::select($sel_sql);
            $sel_info = Service::data($sel_info);
            if ($sel_info[0]['count'] != 0) {
                $del_sql = "DELETE
            FROM
                zplay_basic_report_daily
            WHERE
                plat_type = 'tj'
            AND date_time >= '$begin_date'
            AND date_time <= '$end_date' " . $where_del_platform;
                $update_info = DB::delete($del_sql);

                if (!$update_info) {
                    DB::rollBack();
                }
            }

            $insert_sql = "INSERT INTO zplay_basic_report_daily (
        date_time,
        game_creator,
        os_id,
        game_category_id,
        app_id,
        game_group,
        channel_id,
        version,
        country_id,
        province_id,
        plat_type,
        platform_id,
        earning_type,
        new_ff,
        active_ff,
        new_ad,
        active_ad,
        sessions,
        sessionlength,
        create_time
        ) SELECT
        tj.date,
        app.company_id AS game_creator,
        app.os_id AS os_id,
        app.app_category_id AS game_category_id,
        app.id AS app_id,
        app.release_group AS game_group,
        tj.channel_id,
        tj.version_id,
        if(tj.type = 1,64,tj.country_id) AS country_id,
        if(tj.type = 1,tj.country_id,0) AS province_id,
        'tj' AS plat_type,
        tj.platform_id,
        '' as earning_type,
        sum(tj.new_user) AS new_ff,
        sum(tj.active_user) AS active_ff,
        if(tj.ad_status = 1,sum(tj.new_user),0) AS new_ad,
        if(tj.ad_status = 1,sum(tj.active_user),0) AS active_ad,
        sum(tj.session_time) AS sessions,
        sum(tj.session_length) AS sessionlength,
        now()
        FROM
        zplay_user_tj_report_daily tj
        LEFT JOIN c_app app ON tj.app_id = app.app_id
        WHERE
        tj.date >= '$begin_date'
        AND tj.date <= '$end_date' $where_platform
        GROUP BY
        tj.date,
        tj.app_id,
        tj.account,
        tj.country_id,
        tj.channel_id,
        tj.version_id,
        tj.platform_id,
        tj.ad_status,
        tj.type,
        province_id";
            $insert_info_1 = DB::insert($insert_sql);

            if (!$insert_info_1) {
                //var_dump(3);
                DB::rollBack();
            }


            //td的留存
            $insert_sql_td = "INSERT INTO zplay_basic_report_daily (
        date_time,
        game_creator,
        os_id,
        game_category_id,
        app_id,
        game_group,
        channel_id,
        version,
        country_id,
        province_id,
        plat_type,
        platform_id,
        earning_type,
        retn_new_one,
        retn_new_three,
        retn_new_seven,
        retn_new_fourteen,
        retn_new_thirty,
        create_time
        ) SELECT
        tj.date,
        app.company_id AS game_creator,
        app.os_id AS os_id,
        app.app_category_id AS game_category_id,
        app.id AS app_id,
        app.release_group AS game_group,
        tj.channel_id,
        tj.version_id,
        tj.country_id,
        tj.province_id,
        'tj' AS plat_type,
        tj.platform_id,
        '' as earning_type,
        sum(tk.day1num) AS retn_new_one,
        sum(tk.day3num) AS retn_new_three,
        sum(tk.day7num) AS retn_new_seven,
        sum(tk.day14num) AS retn_new_fourteen,
        sum(tk.day30num) AS retn_new_thirty,
        now()
        FROM
        zplay_keep_tj_report_daily tj
        LEFT JOIN c_app app ON tj.app_id = app.app_id
        LEFT JOIN talkingdata_china_keepuser tk ON tk.id = tj.keep_id
        WHERE
        tj.date >= '$begin_date'
        AND tj.date <= '$end_date' $where_platform
        AND tj.type = 1
        GROUP BY
        tj.date,
        tj.app_id,
        tj.account,
        tj.channel_id,
        tj.platform_id";
            $insert_info_td = DB::insert($insert_sql_td);

            if (!$insert_info_td) {
                //var_dump(3);
                DB::rollBack();
            }
            //flurry的留存
            $insert_sql_flurry = "INSERT INTO zplay_basic_report_daily (
	  date_time,
		game_creator,
		os_id,
		game_category_id,
		app_id,
		game_group,
		channel_id,
		version,
		country_id,
		province_id,
		plat_type,
		platform_id,
		earning_type,
		retn_new_one,
		retn_new_three,
		retn_new_seven,
		retn_new_fourteen,
		retn_new_thirty,
		create_time
	) SELECT
	  tj.date,
    app.company_id AS game_creator,
		app.os_id AS os_id,
		app.app_category_id AS game_category_id,
		app.id AS app_id,
		app.release_group AS game_group,
		tj.channel_id,
		tj.version_id,
		tj.country_id,
		tj.province_id,
		'tj' AS plat_type,
		tj.platform_id,
		'' as earning_type,
		round(sum(fl.dnu * fl.keep_day2)) AS retn_new_one,
		round(sum(fl.dnu * fl.keep_day3)) AS retn_new_three,
		round(sum(fl.dnu * fl.keep_day7)) AS retn_new_seven,
		round(sum(fl.dnu * fl.keep_day14)) AS retn_new_fourteen,
		round(sum(fl.dnu * fl.keep_day30)) AS retn_new_thirty,
		now()
	FROM
		zplay_keep_tj_report_daily tj
	LEFT JOIN c_app app ON tj.app_id = app.app_id
	LEFT JOIN scrapy_flurry_retention_detail fl ON fl.id = tj.keep_id
	WHERE
		tj.date >= '$begin_date'
	AND tj.date <= '$end_date'  $where_platform
	AND tj.type is null
	GROUP BY
		tj.date,
		tj.app_id,
		tj.account,
		tj.channel_id,
		tj.platform_id;";
            $insert_sql_flurry = DB::insert($insert_sql_flurry);

            if (!$insert_sql_flurry) {
                //var_dump(3);
                DB::rollBack();
            }

            DB::commit();

        }catch (\Exception $e) {
            // 异常报错
            if ($platform_id == 'ptj01'){
                $source_name = 'Flurry';
            }elseif($platform_id == 'ptj02'){
                $source_name = 'TalkingData';
            }else{
                $platform_id = 'ptj-000';
                $source_name = '统计平台';
            }

            $message = "{$end_date}号, " . $source_name . "统计平台程序报错,报错原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, $platform_id, $source_name, 1, $message);
            $error_msg_arr[] = $message;
//            CommonFunction::sendMail($error_msg_arr, '统计平台程序error');
            exit;
        }

    }
}