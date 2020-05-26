<?php

namespace App\Console\Commands\DeveloperPushOldprocesses;

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

class DeveloperDayProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'DeveloperDayProcesses {dayid?} ';



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
        $dayid = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-4 day'));
        $date = date('Ymd',strtotime($dayid));
        // 开发者页面数据由新平台往老平台上推数
        $sql = "SELECT
        (CASE
        WHEN c.game_id_z IS NULL THEN
        b.app_id
        ELSE
        c.game_id_z
        END
        ) as  app_id,
        (CASE
        WHEN c.APP_NAME IS NULL THEN
        b.app_name
        ELSE
        c.APP_NAME
        END
        )  app_name,
        (CASE
        WHEN c.DEVELOPER_ID IS NULL THEN
        d.DEVELOPER_ID
        ELSE
        c.DEVELOPER_ID
        END
        ) AS user_id,
        (
        CASE
        WHEN b.os_id = 1 THEN
        'ios'
        WHEN b.os_id = 2 THEN
        'Android'
        END
        ) AS app_os,
        (
        CASE
        WHEN a.ff_earning IS NULL THEN
        0
        ELSE
        a.ff_earning / cur.currency_ex
        END
        ) ff_earning,
        (
        CASE
        WHEN a.ff_income IS NULL THEN
        0
        ELSE
        a.ff_income / cur.currency_ex
        END
        ) ff_income,
        (
        CASE
        WHEN a.ff_divide IS NULL THEN
        0
        ELSE
        a.ff_divide / cur.currency_ex
        END
        ) ff_divide,
        (
        CASE
        WHEN a.ad_earning IS NULL THEN
        0
        ELSE
        a.ad_earning / cur.currency_ex
        END
        ) ad_earning,
        (
        CASE
        WHEN a.ad_income IS NULL THEN
        0
        ELSE
        a.ad_income / cur.currency_ex
        END
        ) ad_income,
        (
        CASE
        WHEN a.ad_divide IS NULL THEN
        0
        ELSE
        a.ad_divide / cur.currency_ex
        END
        ) ad_divide,
        (
        CASE
        WHEN a.ff_active IS NULL THEN
        0
        ELSE
        a.ff_active
        END
        ) active_user,
        (
        CASE
        WHEN a.ff_new IS NULL THEN
        0
        ELSE
        a.ff_new
        END
        ) new_user,
        (
        CASE
        WHEN a.tg_cost IS NULL THEN
        0
        ELSE
        a.tg_cost / cur.currency_ex
        END
        ) tg_cost,
        (
        CASE
        WHEN a.tg_expense IS NULL THEN
        0
        ELSE
        a.tg_expense / cur.currency_ex
        END
        ) tg_expense,
        date_time as stats_date
        FROM
        d_developer_day AS a
        LEFT JOIN c_app AS b ON a.app_id = b.id
        LEFT JOIN haha_app_new_app_id AS c ON b.app_id = c.new_app_id
        LEFT JOIN c_developer AS d ON b.developer_id = d.id
        LEFT JOIN `c_currency_ex` cur on cur.`effective_time` = date_format(a.date_time, '%Y%m') and cur.`currency_id` = 60
        WHERE
        a.date_time = '$date'

        ";

        //  and b.app_id in ('gi195001','gg195002')  测试对数用
        $insert_info = DB::connection('mysql')->select($sql);
        $info = Service::data($insert_info);

        var_dump($date);
//        $result = DB::connection('mysql_developer')->table('out_divide_develop')->where(['stats_date' => $date])->delete();
        $result = DB::connection('mysql')->table('out_divide_develop')->where(['stats_date' => $date])->delete();
        var_dump($result);

        $step = array();
        $i = 0;
        foreach ($info as $k => $v) {
            $insert_sql = "insert  into out_divide_develop (`app_id`,`user_id`,`app_name`,`app_os`,`new_user`,`active_user`,`ff_earning`,`ff_income`,`ff_divide`,`ad_earning`,`ad_income`,`ad_divide`,`tg_cost`,`tg_expense`,`stats_date`) values('{$v['app_id']}','{$v['user_id']}','{$v['app_name']}','{$v['app_os']}','{$v['new_user']}','{$v['active_user']}','{$v['ff_earning']}','{$v['ff_income']}','{$v['ff_divide']}','{$v['ad_earning']}','{$v['ad_income']}','{$v['ad_divide']}','{$v['tg_cost']}','{$v['tg_expense']}','{$v['stats_date']}')";
//            DB::connection('mysql_developer')->insert($insert_sql);
            DB::connection('mysql')->insert($insert_sql);

        }

        //var_dump($info);die;
        echo '处理完成';
    }
}