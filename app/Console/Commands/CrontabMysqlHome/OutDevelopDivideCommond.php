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

class OutDevelopDivideCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'OutDevelopDivideCommond {beginday?} {endday?} ';

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
        $endday = $this->argument('endday')?$this->argument('endday'):date('Y-m-d',strtotime('-2 day'));
         for ($i=strtotime($beginday); $i <=strtotime($endday) ; $i+=86400) {
            $dayid = date('Y-m-d',$i);
            var_dump($dayid);
            $this->insertBasicDataHomePage($dayid);

         }


    }

    public function insertBasicDataHomePage($dayid){
        DB::beginTransaction();
        $sel_sql = "select count(1) as count  FROM
        zplay_divide_develop_report
        WHERE
         date = '$dayid' ";
        $sel_info = DB::select($sel_sql);
        $sel_info = Service::data($sel_info);
        if($sel_info[0]['count'] !=0){
            $del_sql ="DELETE
            FROM
                zplay_divide_develop_report
            WHERE date = '$dayid' ";
            $delete_info =DB::delete($del_sql);

            if(!$delete_info){
                DB::rollBack();
            }
        }
        
        // 获取铁头的总利润与总收入比例
        $profitRate= $this->getWillHeroProfitRate($dayid);
        
        $sql = "INSERT into zplay_divide_develop_report 
                SELECT
                '' as id ,
                b.app_id,
                b.developer_id,
                b.app_name,
                (CASE 
                  WHEN app.release_region_id = 1 and app.os_id =1  THEN '1'   -- 操作系统(1、iOS-Global2、Android-Globa3、iOS-CN4、Android-CN)
                    WHEN app.release_region_id = 1 and app.os_id = 2 THEN '2'
                    WHEN app.release_region_id = 3 and app.os_id = 1 THEN '3'
                    WHEN app.release_region_id = 3 and app.os_id = 2 THEN '4'
                    ELSE ''
                END) as app_os,
                b.new_user,
                b.active_user,
                b.ff_income_taxAfter as ff_earning,
                b.ff_income_taxAfter as ff_income,
                b.ff_divide_taxAfter as ff_divide,
                b.ad_income_taxAfter as ad_earning,
                b.ad_income_taxAfter as ad_income,
                b.ad_divide_taxAfter as ad_divide,
                b.tg_cost as tg_cost,
                b.tg_divide as tg_expense,
                b.date as stats_date
                FROM
                    zplay_divide_develop b,
                    c_app app     	
                WHERE
                b.app_id = app.id -- and app.is_dev_show = 2
                and b.date = '$dayid' and b.app_id not in (955,131,912)";
        $info = DB::insert($sql);
        if(!$info){
            DB::rollBack();
        }
        
        // 合并ga042001、ga008037、ga042004 国内铁头应用的的数据
        $sql = "INSERT into zplay_divide_develop_report 
                SELECT
                '' as id ,
                131 AS app_id,
                42 AS developer_id,
                '铁头英雄_王牌大作战' AS app_name,
                4 as app_os,
                SUM(b.new_user) AS new_user,
                SUM(b.active_user) AS active_user,
                SUM(b.ff_income_taxAfter) as ff_earning,
                SUM(b.ff_income_taxAfter) as ff_income,
                SUM(b.ff_divide_taxAfter) as ff_divide,
                round(SUM(b.ad_income_taxAfter) * m.profit_rate, 2) as ad_earning,
                round(SUM(b.ad_income_taxAfter) * m.profit_rate, 2) as ad_income,
                round(SUM(b.ad_income_taxAfter) * m.profit_rate, 2) * 0.5 as ad_divide,
                0 as tg_cost,
                0 as tg_expense,
                b.date as stats_date
                FROM
                    zplay_divide_develop b
                    left join
                    (select sum(ad_income_taxAfter-tg_cost)/sum(ad_income_taxAfter) AS profit_rate from zplay_divide_develop where app_id in (955,131,912) and '". substr($dayid, 0, 7)."'=SUBSTR(date,1 ,7)) m
                    on 1=1
                where b.date = '$dayid' and b.app_id in (955,131,912)
                GROUP BY b.date";
        $info = DB::insert($sql);
        if(!$info){
            DB::rollBack();
        }
        DB::commit();

    }

}
