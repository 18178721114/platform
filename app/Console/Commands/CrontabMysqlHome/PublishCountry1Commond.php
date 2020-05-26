<?php

namespace App\Console\Commands\CrontabMysqlHome;
# 发行数据分国家数据
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
# 发行数据分国家数据
class PublishCountry1Commond extends Command
{
    # 发行数据分国家数据
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'PublishCountry1Commond {beginday?} {endday?} ';

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

        // 入口方法
        $beginday = $this->argument('beginday')?$this->argument('beginday'):date('Y-m-d',strtotime('-6 day'));
        $endday = $this->argument('endday')?$this->argument('endday'):date('Y-m-d');
        $this->insertBasicDataHomePage($beginday,$endday);

    }

    public function insertBasicDataHomePage($beginday,$endday){
        DB::beginTransaction();
        $sel_sql = "select count(1) as count  FROM
        zplay_basic_publish_country_report_total
        WHERE
         date between '$beginday' and '$endday' ";
        $sel_info = DB::select($sel_sql);
        $sel_info = Service::data($sel_info);
        if($sel_info[0]['count'] !=0){
            $del_sql ="DELETE
            FROM
                zplay_basic_publish_country_report_total
            WHERE date between '$beginday' and '$endday' ";
            $delete_info =DB::delete($del_sql);

            if(!$delete_info){
                DB::rollBack();
            }
        }

        //乘以0.78 国内  不乘0.78 国外
        $sql = "INSERT into zplay_basic_publish_country_report_total
SELECT 
 '' as id,
 a.date,
 c_app.id as app_id,
 a.country_id,
 sum(a.new) as new,
 sum(a.active) as active,
 sum(a.tg_new) as tg_new,
 sum(a.session_time) as session_time,
 sum(a.session_length) as session_length,
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
 country_id,
 0 as new,
 0 as active,
 0 as tg_new,
 0 as session_time,
 0 as session_length,
 0 as interst_ad_imp,
 sum(impression) as video_ad_imp,
 0 as interst_ad_income,
 sum(earning_exc_usd) as video_ad_incom,
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
	GROUP BY date,app_id,country_id
	union all 
SELECT 
 date,
 app_id,
 country_id,
 0 as new,
 0 as active,
 0 as tg_new,
 0 as session_time,
 0 as session_length,
 sum(impression) as interst_ad_imp,
 0 as video_ad_imp,
 sum(earning_exc_usd) as interst_ad_income,
 0 as video_ad_incom,
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
	GROUP BY date,app_id,country_id
		union all 
SELECT 
 date,
 app_id,
 country_id,
 0 as new,
 0 as active,
 0 as tg_new,
 0 as session_time,
 0 as session_length,
 0 as interst_ad_imp,
 0 as video_ad_imp,
 0 as interst_ad_income,
 0 as video_ad_incom,
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
	GROUP BY date,app_id,country_id
union all 
SELECT 
 date,
 app_id,
 country_id,
 0 as new,
 0 as active,
 0 as tg_new,
 0 as session_time,
 0 as session_length,
 0 as interst_ad_imp,
 0 as video_ad_imp,
 0 as interst_ad_income,
 0 as video_ad_incom,
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
	GROUP BY date,app_id,country_id
	union all 
SELECT 
 date,
 app_id,
 country_id,
 0 as new,
 0 as active,
 sum(new) as tg_new,
 0 as session_time,
 0 as session_length,
 0 as interst_ad_imp,
 0 as video_ad_imp,
 0 as interst_ad_income,
 0 as video_ad_incom,
 0 as ad_income,
 0 as ff_income,
 sum(cost_usd) as tg_cost
FROM
	zplay_tg_report_daily 
WHERE
 date BETWEEN '$beginday' 
	AND '$endday' and tongji_type = 0
	GROUP BY date,app_id,country_id
		union all 
SELECT 
 date,
 app_id,
 country_id,
 sum(new_user) as new,
 sum(active_user) as active,
 0 as tg_new,
 sum(session_time) as session_time,
 sum(session_length) as session_length,
 0 as interst_ad_imp,
 0 as video_ad_imp,
 0 as interst_ad_income,
 0 as video_ad_incom,
 0 as ad_income,
 0 as ff_income,
 0 as tg_cost
FROM
	zplay_user_tj_report_daily
WHERE
  date BETWEEN '$beginday' 
	AND '$endday'
	GROUP BY date,app_id,country_id ) a
	LEFT JOIN c_app on a.app_id =c_app.app_id 
	GROUP BY a.app_id,a.date,a.country_id";

        $info = DB::insert($sql);
        if(!$info){
            DB::rollBack();
        }
        DB::commit();

    }

}
