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
        out_divide_develop
        WHERE
         stats_date = '$dayid' ";
        $sel_info = DB::select($sel_sql);
        $sel_info = Service::data($sel_info);
        if($sel_info[0]['count'] !=0){
            $del_sql ="DELETE
            FROM
                out_divide_develop
            WHERE stats_date = '$dayid' ";
            $delete_info =DB::delete($del_sql);

            if(!$delete_info){
                DB::rollBack();
            }
        }
        $sql = "INSERT into out_divide_develop
                SELECT
                '' as id ,
                application.gameid as app_id,
                a.developer_id as user_id,
                b.app_name,
                (CASE 
                    WHEN app.release_region_id = 1 and app.os_id =1  THEN 'iOS'
                    WHEN app.release_region_id = 1 and app.os_id = 2 THEN 'GooglePlay'
                    WHEN app.release_region_id = 3 and app.os_id = 1 THEN 'iOS'
                    WHEN app.release_region_id = 3 and app.os_id = 2 THEN 'Android'
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
                    c_developer_app_divide a,
                    zplay_divide_develop b,
                    c_app app,
                 application 	
                WHERE
                b.app_id = app.id 
                and a.app_id =  app.app_id
                and a.new_developer_id_key = b.developer_id 
                and application.new_app_id = a.app_id
                and b.date = '$dayid'
            ";
        $info = DB::insert($sql);
        if(!$info){
            DB::rollBack();
        }
        DB::commit();

    }

}
