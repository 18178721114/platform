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

class DevelopDivideCnyCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'DevelopDivideCnyCommond {beginday?} {endday?} ';

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
            for ($i = strtotime($beginday); $i <= strtotime($endday); $i += 86400) {
                $dayid = date('Y-m-d', $i);
                var_dump($dayid);
                $this->insertBasicDataHomePage($dayid);

            }
        }catch (\Exception $e) {
            // 异常报错
            $message = date("Y-m-d")."号,开发者人命币数据程序报错,报错原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, 'pad-001', '开发者人命币数据', 2, $message);
            $error_msg_arr[] = $message;
            CommonFunction::sendMail($error_msg_arr, '开发者人命币数据');
            exit;
        }


    }

    public function insertBasicDataHomePage($dayid){
        DB::beginTransaction();
        $sel_sql = "select count(1) as count  FROM
        zplay_divide_develop_cny
        WHERE
         date = '$dayid' ";
        $sel_info = DB::select($sel_sql);
        $sel_info = Service::data($sel_info);
        if($sel_info[0]['count'] !=0){
            $del_sql ="DELETE
            FROM
                zplay_divide_develop_cny
            WHERE date = '$dayid' ";
            $delete_info =DB::delete($del_sql);

            if(!$delete_info){
                DB::rollBack();
            }
        }

//        //乘以0.78 国内  不乘0.78 国外
//        $sql = "INSERT INTO zplay_divide_develop_cny ( os_id,developer_name,app_name,new_user, active_user, ff_income, ff_divide, ad_income, ad_divide, tg_cost, tg_divide, date, game_creator, app_id, developer_id, create_time, develop_cost,
//                ff_income_taxAfter,ff_divide_taxAfter,ad_income_taxAfter,ad_divide_taxAfter,develop_cost_taxAfter
//            ) SELECT
//            c_app.os_id,
//            c_developer.developer_name,
//            c_app.app_name,
//            a.new_ff AS new_user,
//            a.active_ff AS active_user,
//            round(a.income_fix_ff,2) AS ff_income,
//            round(a.income_fix_ff * b.divide_billing / 100,2) AS ff_divide,
//            round(a.income_fix_ad,2) AS ad_income,
//            round(a.income_fix_ad * b.divide_ad / 100,2) AS ad_divide,
//            round(a.cost_tg,2) AS tg_cost,
//            round(a.cost_tg * b.divide_cost / 100,2) AS tg_divide,
//            a.date_time AS date,
//            a.game_creator,
//            c_app.id,
//            c_app.developer_id,
//            NOW() AS create_time,
//            round(a.income_fix_ff * b.divide_billing / 100 + a.income_fix_ad * b.divide_ad / 100 - a.cost_tg * b.divide_cost / 100 ,2)AS develop_cost,
//            round(a.income_fix_ff_taxAfter,2) AS ff_income_taxAfter,
//            round(a.income_fix_ff_taxAfter * b.divide_billing / 100,2) AS ff_divide_taxAfter,
//            round(a.income_fix_ad_taxAfter ,2)AS ad_income_taxAfter,
//            round(a.income_fix_ad_taxAfter * b.divide_ad / 100 ,2)AS ad_divide_taxAfter,
//            round(a.income_fix_ff_taxAfter * b.divide_billing / 100 + a.income_fix_ad_taxAfter * b.divide_ad / 100 - a.cost_tg * b.divide_cost / 100 ,2)AS develop_cost_taxAfter
//            FROM
//            (
//            select  sum( a_info.income_fix_ff ) AS income_fix_ff,
//                sum( a_info.income_fix_ff_taxAfter ) AS income_fix_ff_taxAfter,
//                sum( a_info.income_fix_ad ) AS income_fix_ad,
//                sum( a_info.income_fix_ad_taxAfter ) AS income_fix_ad_taxAfter,
//                sum( a_info.cost_tg ) AS cost_tg,
//                sum( a_info.new_ff ) AS new_ff,
//                sum( a_info.active_ff ) AS active_ff,
//                a_info.app_id,
//                a_info.game_creator,
//                a_info.date_time from (
//            SELECT
//                sum( income_fix_ff ) AS income_fix_ff,
//                sum( income_fix_ff )*0.78 AS income_fix_ff_taxAfter,
//                sum( income_fix_ad ) AS income_fix_ad,
//                sum( income_fix_ad )*0.78 AS income_fix_ad_taxAfter,
//                sum( cost_tg ) AS cost_tg,
//                sum( new_ff ) AS new_ff,
//                sum( active_ff ) AS active_ff,
//                app_id,
//                game_creator,
//                date_time
//            FROM
//                zplay_basic_report_daily
//            WHERE
//                (app_id is not  null  or  app_id != 0) and
//                flow_type = 1
//                AND statistics = 0
//                AND date_time = '$dayid'
//                AND ( income_usd_ff != 0 OR income_usd_ad != 0 OR cost_usd_tg != 0 OR new_ff != 0 OR active_ff != 0 )
//                and platform_id  in (SELECT DISTINCT platform_id  from c_platform WHERE company_id in (1,3))
//            GROUP BY
//                app_id,
//                game_creator
//            UNION ALL
//            SELECT
//                sum( income_fix_ff ) AS income_fix_ff,
//                sum( income_fix_ff ) AS income_fix_ff_taxAfter,
//                sum( income_fix_ad ) AS income_fix_ad,
//                sum( income_fix_ad ) AS income_fix_ad_taxAfter,
//                sum( cost_tg ) AS cost_tg,
//                sum( new_ff ) AS new_ff,
//                sum( active_ff ) AS active_ff,
//                app_id,
//                game_creator,
//                date_time
//            FROM
//                zplay_basic_report_daily
//            WHERE
//                (app_id is not  null  or  app_id != 0) and
//                flow_type = 1
//                AND statistics = 0
//                AND date_time = '$dayid'
//                AND ( income_usd_ff != 0 OR income_usd_ad != 0 OR cost_usd_tg != 0 OR new_ff != 0 OR active_ff != 0 )
//                and platform_id not in (SELECT DISTINCT platform_id  from c_platform WHERE company_id in (1,3))
//            GROUP BY
//                app_id,
//                game_creator
//            UNION ALL
//                SELECT
//                0 AS income_fix_ff,
//                0 AS income_fix_ff_taxAfter,
//                0 AS income_fix_ad,
//                0 AS income_fix_ad_taxAfter,
//                sum( ad_divide ) AS cost_tg,
//                0 AS new_ff,
//                0 AS active_ff,
//                c_app.id,
//                c_app.company_id as game_creator,
//                date_time
//            FROM
//                d_channel_ad ,c_app
//            WHERE
//
//                d_channel_ad.app_id = c_app.app_id
//                AND date_time = '$dayid'
//                AND  ad_divide != 0
//            GROUP BY
//                c_app.id,
//                c_app.company_id
//                ) a_info   GROUP BY
//                a_info.app_id,
//                a_info.game_creator
//            ) a
//            LEFT JOIN (
//            SELECT
//                *
//            FROM
//                ( SELECT effective_date, app_channel_id, divide_billing, divide_ad, divide_cost FROM c_divide WHERE effective_date <= '$dayid' AND type = 1 ORDER BY effective_date DESC ) t
//            GROUP BY
//                t.app_channel_id
//            ) AS b ON a.app_id = b.app_channel_id
//            LEFT JOIN c_app ON c_app.id = a.app_id
//            LEFT JOIN c_developer ON c_app.developer_id = c_developer.id
//            ";

        //乘以0.78 国内  不乘0.78 国外
        $sql = "INSERT INTO zplay_divide_develop_cny ( os_id,developer_name,app_name,new_user, active_user, ff_income, ff_divide, ad_income, ad_divide, tg_cost, tg_divide, date, game_creator, app_id, developer_id, create_time, develop_cost,
                ff_income_taxAfter,ff_divide_taxAfter,ad_income_taxAfter,ad_divide_taxAfter,develop_cost_taxAfter 
            ) SELECT
            c_app.os_id,
            c_developer.developer_name,
            c_app.app_name,
            a.new_ff AS new_user,
            a.active_ff AS active_user,
            round(a.income_fix_ff,2) AS ff_income,
            round(a.income_fix_ff * b.divide_billing / 100,2) AS ff_divide,
            round(a.income_fix_ad,2) AS ad_income,
            round(a.income_fix_ad * b.divide_ad / 100,2) AS ad_divide,
            round(a.cost_tg,2) AS tg_cost,
            round(a.cost_tg * b.divide_cost / 100,2) AS tg_divide,
            a.date_time AS date,
            a.game_creator,
            c_app.id,
            c_app.developer_id,
            NOW() AS create_time,
            round(a.income_fix_ff * b.divide_billing / 100 + a.income_fix_ad * b.divide_ad / 100 - a.cost_tg * b.divide_cost / 100 ,2)AS develop_cost,
            round(a.income_fix_ff_taxAfter,2) AS ff_income_taxAfter,
            round(a.income_fix_ff_taxAfter * b.divide_billing / 100,2) AS ff_divide_taxAfter,
            round(a.income_fix_ad_taxAfter ,2)AS ad_income_taxAfter,
            round(a.income_fix_ad_taxAfter * b.divide_ad / 100 ,2)AS ad_divide_taxAfter,
            round(a.income_fix_ff_taxAfter * b.divide_billing / 100 + a.income_fix_ad_taxAfter * b.divide_ad / 100 - a.cost_tg * b.divide_cost / 100 ,2)AS develop_cost_taxAfter
            FROM
            (
            select  sum( a_info.income_fix_ff ) AS income_fix_ff,
                sum( a_info.income_fix_ff_taxAfter ) AS income_fix_ff_taxAfter,
                sum( a_info.income_fix_ad ) AS income_fix_ad,
                sum( a_info.income_fix_ad_taxAfter ) AS income_fix_ad_taxAfter,
                sum( a_info.cost_tg ) AS cost_tg,
                sum( a_info.new_ff ) AS new_ff,
                sum( a_info.active_ff ) AS active_ff,
                a_info.app_id,
                a_info.game_creator,
                a_info.date_time from (
            SELECT
                sum( income_fix_ff ) AS income_fix_ff,
                sum( income_fix_ff ) AS income_fix_ff_taxAfter,
                sum( income_fix_ad ) AS income_fix_ad,
                sum( income_fix_ad ) AS income_fix_ad_taxAfter,
                sum( cost_tg ) AS cost_tg,
                sum( new_ff ) AS new_ff,
                sum( active_ff ) AS active_ff,
                app_id,
                game_creator,
                date_time 
            FROM
                zplay_basic_report_daily 
            WHERE
                (app_id is not  null  or  app_id != 0) and 
                flow_type = 1 
                AND statistics = 0 
                AND date_time = '$dayid' 
                AND ( income_usd_ff != 0 OR income_usd_ad != 0 OR cost_usd_tg != 0 OR new_ff != 0 OR active_ff != 0 ) 
                and platform_id  in (SELECT DISTINCT platform_id  from c_platform WHERE company_id in (1,3))
            GROUP BY
                app_id,
                game_creator
            UNION ALL 
            SELECT
                sum( income_fix_ff ) AS income_fix_ff,
                sum( income_fix_ff ) AS income_fix_ff_taxAfter,
                sum( income_fix_ad ) AS income_fix_ad,
                sum( income_fix_ad ) AS income_fix_ad_taxAfter,
                sum( cost_tg ) AS cost_tg,
                sum( new_ff ) AS new_ff,
                sum( active_ff ) AS active_ff,
                app_id,
                game_creator,
                date_time 
            FROM
                zplay_basic_report_daily 
            WHERE
                (app_id is not  null  or  app_id != 0) and 
                flow_type = 1 
                AND statistics = 0 
                AND date_time = '$dayid' 
                AND ( income_usd_ff != 0 OR income_usd_ad != 0 OR cost_usd_tg != 0 OR new_ff != 0 OR active_ff != 0 ) 
                and platform_id not in (SELECT DISTINCT platform_id  from c_platform WHERE company_id in (1,3))
            GROUP BY
                app_id,
                game_creator 
            UNION ALL    
                SELECT
                0 AS income_fix_ff,
                0 AS income_fix_ff_taxAfter,
                0 AS income_fix_ad,
                0 AS income_fix_ad_taxAfter,
                sum( ad_divide ) AS cost_tg,
                0 AS new_ff,
                0 AS active_ff,
                c_app.id,
                c_app.company_id as game_creator,
                date_time 
            FROM
                d_channel_ad ,c_app 
            WHERE
						
                d_channel_ad.app_id = c_app.app_id   
                AND date_time = '$dayid' 
                AND  ad_divide != 0  
            GROUP BY
                c_app.id,
                c_app.company_id
                ) a_info   GROUP BY
                a_info.app_id,
                a_info.game_creator
            ) a
            LEFT JOIN (
            SELECT
                * 
            FROM
                ( SELECT effective_date, app_channel_id, divide_billing, divide_ad, divide_cost FROM c_divide WHERE effective_date <= '$dayid' AND type = 1 ORDER BY effective_date DESC ) t 
            GROUP BY
                t.app_channel_id 
            ) AS b ON a.app_id = b.app_channel_id
            LEFT JOIN c_app ON c_app.id = a.app_id
            LEFT JOIN c_developer ON c_app.developer_id = c_developer.id
            ";

        $info = DB::insert($sql);
        if(!$info){
            DB::rollBack();
        }
        DB::commit();

    }

}
