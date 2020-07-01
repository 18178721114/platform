<?php

namespace App\Console\Commands\CrontabMysqlHome;
# 发行数据不分国家数据
use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\Common\CommonFunction;
use App\Common\CurlRequest;
use App\Common\Service;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\DataImportLogic;
# 发行数据不分国家数据
class PublishCommond extends Command
{
    # 发行数据不分国家数据
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'PublishCommond {beginday?} {endday?} ';

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
        try {
            // 入口方法
            $beginday = $this->argument('beginday') ? $this->argument('beginday') : date('Y-m-d', strtotime('-6 day'));
            $endday = $this->argument('endday') ? $this->argument('endday') : date('Y-m-d', strtotime('-2 day'));
            $this->insertBasicDataHomePage($beginday, $endday);
        }catch (\Exception $e) {
            // 异常报错
            $message = date("Y-m-d")."号,发行数据不分国家数据程序报错,报错原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, 'pad-001', '发行数据不分国家数据', 2, $message);
            $error_msg_arr[] = $message;
            CommonFunction::sendMail($error_msg_arr, '发行数据不分国家数据');
            exit;
        }

    }

    public function insertBasicDataHomePage($beginday,$endday){
        DB::beginTransaction();
        $sel_sql = "select count(1) as count  FROM
        zplay_basic_publish_report_total
        WHERE
         date between '$beginday' and '$endday' ";
        $sel_info = DB::select($sel_sql);
        $sel_info = Service::data($sel_info);
        if($sel_info[0]['count'] !=0){
            $del_sql ="DELETE
            FROM
                zplay_basic_publish_report_total
            WHERE date between '$beginday' and '$endday' ";
            $delete_info =DB::delete($del_sql);

            if(!$delete_info){
                DB::rollBack();
            }
        }

        //乘以0.78 国内  不乘0.78 国外
        $sql = "INSERT into zplay_basic_publish_report_total
SELECT 
 '' as id,
 a.date,
 c_app.id as app_id,
 sum(a.new) as new,
 sum(a.active) as active,
 sum(a.tg_new) as tg_new,
 sum(a.session_time) as session_time,
 sum(a.session_length) as session_length,
 sum(keep_day2) as keep_day2,
 sum(keep_day7) as keep_day7,
 sum(a.interst_ad_imp) as interst_ad_imp,
 sum(a.video_ad_imp) as video_ad_imp,
 sum(a.ad_income) as ad_income,
 sum(a.ff_income) as ff_income,
 sum(a.tg_cost) as tg_cost,
 c_app.company_id,
 now()
FROM

( SELECT 
 date,
 app_id,
 0 as new,
 0 as active,
 0 as tg_new,
 0 as session_time,
 0 as session_length,
 0 as keep_day2,
 0 as keep_day7,
 0 as interst_ad_imp,
 sum(impression) as video_ad_imp,
 0 as ad_income,
 0 as ff_income,
 0 as tg_cost
FROM
	zplay_ad_report_daily 
WHERE
	flow_type = 1 
	AND statistics = 0 
	AND ad_type = 3   -- 视频数据
	AND date BETWEEN '$beginday' 
	AND '$endday'
	GROUP BY date,app_id
	union all 
SELECT 
 date,
 app_id,
 0 as new,
 0 as active,
 0 as tg_new,
 0 as session_time,
 0 as session_length,
  0 as keep_day2,
 0 as keep_day7,
 sum(impression) as interst_ad_imp,
 0 as video_ad_imp,
 0 as ad_income,
 0 as ff_income,
 0 as tg_cost
FROM
	zplay_ad_report_daily 
WHERE
	flow_type = 1 
	AND statistics = 0 
	AND ad_type in (1,41)    -- 插屏数据
	AND date BETWEEN '$beginday' 
	AND '$endday'
	GROUP BY date,app_id
		union all 
SELECT 
 date,
 app_id,
 0 as new,
 0 as active,
 0 as tg_new,
 0 as session_time,
 0 as session_length,
  0 as keep_day2,
 0 as keep_day7,
 0 as interst_ad_imp,
 0 as video_ad_imp,
 sum(earning_exc_usd) as ad_income,
 0 as ff_income,
 0 as tg_cost
FROM
	zplay_ad_report_daily 
WHERE
	flow_type = 1 
	AND statistics = 0 
	AND date BETWEEN '$beginday'  -- 	广告收入
	AND '$endday'
	GROUP BY date,app_id
union all 
SELECT 
 date,
 app_id,
 0 as new,
 0 as active,
 0 as tg_new,
 0 as session_time,
 0 as session_length,
  0 as keep_day2,
 0 as keep_day7,
 0 as interst_ad_imp,
 0 as video_ad_imp,
 0 as ad_income,
  sum(income_fix/currency_ex) as ff_income,
 0 as tg_cost
FROM
	zplay_ff_report_daily ,c_currency_ex
WHERE
`effective_time` = date_format(date, '%Y%m')
  and  currency_id = 60
  and  tongji_type > -1
	and date BETWEEN '$beginday' 
	AND '$endday'
	GROUP BY date,app_id
	union all 
SELECT 
 date,
 app_id,
 0 as new,
 0 as active,
 sum(new) as tg_new,
 0 as session_time,
 0 as session_length,
  0 as keep_day2,
 0 as keep_day7,
 0 as interst_ad_imp,
 0 as video_ad_imp,
 0 as ad_income,
 0 as ff_income,
 sum(cost_usd) as tg_cost
FROM
	zplay_tg_report_daily 
WHERE
 date BETWEEN '$beginday' 
	AND '$endday' and tongji_type = 0
	GROUP BY date,app_id
		union all 
SELECT 
 date,
 app_id,
 sum(new_user) as new,
 sum(active_user) as active,
 0 as tg_new,
 sum(session_time) as session_time,
 sum(session_length) as session_length,
 0 as keep_day2,
 0 as keep_day7,
 0 as interst_ad_imp,
 0 as video_ad_imp,
 0 as ad_income,
 0 as ff_income,
 0 as tg_cost
FROM
	zplay_user_tj_report_daily
WHERE
  date BETWEEN '$beginday' 
	AND '$endday'
	GROUP BY date,app_id 
			union all 
SELECT 
 a.date,
 a.app_id,
 0 as new,
 0 as active,
 0 as tg_new,
 0 as session_time,
 0 as session_length,
 sum( a.retn_new_one ) as keep_day2,
 sum( a.retn_new_seven ) as keep_day7,
 0 as interst_ad_imp,
 0 as video_ad_imp,
 0 as ad_income,
 0 as ff_income,
 0 as tg_cost
FROM
(
	SELECT
		tj.date,
		tj.app_id,
		sum( tk.day1num ) AS retn_new_one,
		sum( tk.day7num ) AS retn_new_seven 
	FROM
		zplay_keep_tj_report_daily tj,
		talkingdata_china_keepuser tk 
	WHERE
		tk.id = tj.keep_id 
		and tj.platform_id ='ptj02'
		AND tj.date >= '$beginday' 
		AND tj.date <= '$endday' 
		AND tj.type = 1 
	GROUP BY
		tj.date,
		tj.app_id UNION ALL
	SELECT
		tj.date,
		tj.app_id,
		round(
		sum( fl.dnu * fl.keep_day2 )) AS retn_new_one,
		round(
		sum( fl.dnu * fl.keep_day7 )) AS retn_new_seven 
	FROM
		zplay_keep_tj_report_daily tj,
		scrapy_flurry_retention_detail fl 
	WHERE
		fl.id = tj.keep_id 
		and tj.platform_id ='ptj01'
		AND tj.date >= '$beginday' 
		AND tj.date <= '$endday' 
		AND tj.type IS NULL 
	GROUP BY
		tj.date,
		tj.app_id 
	) a 
GROUP BY
	a.date,
	a.app_id

	) a
	LEFT JOIN c_app on a.app_id =c_app.app_id 
	GROUP BY a.app_id,a.date";

        $info = DB::insert($sql);
        if(!$info){
            DB::rollBack();
        }
        DB::commit();

    }

}
