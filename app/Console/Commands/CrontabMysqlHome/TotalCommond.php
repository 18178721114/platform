<?php

namespace App\Console\Commands\CrontabMysqlHome;

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

class TotalCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'TotalCommond {month?}  ';

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
        $month = $this->argument('month')?$this->argument('month'):date('Y-m');
        $firstday = $month.'-01';
        $lastday = date('Y-m-d', strtotime("$firstday +1 month -1 day"));
        var_dump($month,$firstday,$lastday);


        $this->insertBasicDataHomePage($firstday,$lastday,$month);




    }

    public function insertBasicDataHomePage($firstday,$lastday,$month){
        DB::beginTransaction();
        $curreny_month =  str_replace('-','',$month);
        //  获取单月汇率
        $curreny_sql = "select currency_ex from  c_currency_ex where currency_id = 60 and effective_time = '$curreny_month'";
        $currency_ex = DB::select($curreny_sql);
        $currency_ex = Service::data($currency_ex);
        if(empty($currency_ex)){
            DB::rollBack();
            return;
        }
        $currency = $currency_ex[0]['currency_ex'];
        $sel_sql = "select count(1) as count  FROM
        zplay_app_total_report
        WHERE
         date_time = '$month' ";
        $sel_info = DB::select($sel_sql);
        $sel_info = Service::data($sel_info);
        if(!empty($sel_info) && $sel_info[0]['count'] !=0){
            $del_sql ="DELETE
            FROM
                zplay_app_total_report
            WHERE date_time = '$month' ";
            $delete_info =DB::delete($del_sql);

            if(!$delete_info){
                DB::rollBack();
            }
        }


        $sql = "INSERT INTO zplay_app_total_report ( app_id, app_name, APP_FULL_NAME, company_id, date_time, ad_income, ff_income, tg_cost, developer_divide, new_user, active_user, total_income, gross_profit, total_profit,create_time ) SELECT
	b.id AS app_id,
	concat((CASE 
	WHEN b.release_region_id = 1 THEN '全球'
	WHEN b.release_region_id = 2 THEN '国外'
	WHEN b.release_region_id = 3 THEN '国内'	
	ELSE '线下' END)
 ,'-', (CASE 
	WHEN b.os_id = 1 THEN 'IOS'
	WHEN b.os_id = 2 THEN '安卓'	
	WHEN b.os_id = 3 THEN 'H5'
	ELSE '亚马逊' END), '-', b.app_name, '-', b.app_id ) AS app_name,
	b.APP_FULL_NAME,
	b.company_id,
	date,
	sum(ad_income) as  ad_income,
	sum(ff_income) as  ff_income,
	sum(tg_cost) as  tg_cost,
	sum(developer_divide) as  developer_divide,
	sum(new_user) as  new_user,
	sum(active_user) as  active_user,
	sum(ad_income)+sum(ff_income) as total_income,
	sum(ad_income)+sum(ff_income)-sum(tg_cost) as gross_profit,
	sum(ad_income)+sum(ff_income)-sum(tg_cost)-sum(developer_divide) as total_profit,
	now()
	from
	(
	SELECT
	app_id,
	DATE_FORMAT( date, '%Y-%m' ) as date,
	round(sum(earning_exc_usd),2) AS ad_income,
	0 AS ff_income,
	0 AS tg_cost,
  0 as developer_divide,	
	0 as new_user,
	0 as active_user
FROM
	zplay_ad_report_daily  -- 广告收入
WHERE
	 date >= '$firstday'  and date<= '$lastday'
	AND flow_type = 1 
	AND statistics = 0
	and earning_exc_usd>0
	GROUP BY app_id,DATE_FORMAT( date, '%Y-%m' )
	union all
	SELECT
	app_id,
	DATE_FORMAT( date, '%Y-%m' ) as date,
	0 AS ad_income,
	round(sum(INCOME_FIX*{$currency}),2) AS ff_income,
	0 AS tg_cost,
  0 as developer_divide,	
	0 as new_user,
	0 as active_user
FROM
	zplay_ff_report_daily   -- 付费收入
WHERE
	 date >= '$firstday'  and date<= '$lastday'
	AND tongji_type>-1
	and INCOME_FIX>0
	GROUP BY app_id,DATE_FORMAT( date, '%Y-%m' )
union all
	SELECT
	app_id,
	DATE_FORMAT( date, '%Y-%m' ) as date,
	0 AS ad_income,
	0 AS ff_income,
	round(sum(cost_usd),2) AS tg_cost,
  0 as developer_divide,	
	0 as new_user,
	0 as active_user
FROM
	zplay_tg_report_daily   -- tg成本
WHERE
	date >= '$firstday'  and date<= '$lastday'
	AND tongji_type=0
	and cost_usd>0
	GROUP BY app_id,DATE_FORMAT( date, '%Y-%m' )
	union all
	SELECT
	c_app.app_id,
	DATE_FORMAT( date, '%Y-%m' ) as date,
	0 AS ad_income,
	0 AS ff_income,
	0 AS tg_cost,
  round(sum(develop_cost),2) as developer_divide,	
	0 as new_user,
	0 as active_user
FROM
	zplay_divide_develop,c_app   -- 开发者成本成本
WHERE
 zplay_divide_develop.app_id = c_app.id and 
	date >= '$firstday'  and date<= '$lastday'
	and develop_cost>0
	GROUP BY app_id,DATE_FORMAT( date, '%Y-%m' )
		union all
	SELECT
	app_id,
	DATE_FORMAT( date, '%Y-%m' ) as date,
	0 AS ad_income,
	0 AS ff_income,
	0 AS tg_cost,
  0 as developer_divide,	
	sum(new_user) as new_user,
	0 as active_user
FROM
	zplay_user_tj_report_daily  -- 新增用户
WHERE
	date >= '$firstday'  and date<= '$lastday'
	and new_user>0
	GROUP BY app_id,DATE_FORMAT( date, '%Y-%m' )
		union all
	SELECT
	c_app.app_id,
	DATE_FORMAT( date, '%Y-%m' ) as date,
	0 AS ad_income,
	0 AS ff_income,
	0 AS tg_cost,
  0 as developer_divide,	
	0 as new_user,
	sum(active_user) as active_user
FROM
	zplay_user_tj_report_month,c_app   -- 活跃用户
WHERE
 zplay_user_tj_report_month.app_id = c_app.id and 
	date >= '$firstday'  and date<= '$lastday'
	and active_user>0
	GROUP BY app_id,DATE_FORMAT( date, '%Y-%m' )) a, c_app b WHERE a.app_id = b.app_id GROUP BY a.app_id";
        $info = DB::insert($sql);
        if(!$info){
            DB::rollBack();
        }
        DB::commit();

    }

}
