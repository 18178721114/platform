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

class CheckFfConfigProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'CheckFfConfigProcesses {dayid?} ';

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
        $pgsql_info = self::pgsql();// pgsql  报错信息
        $mysql_info = self::mysql();// mysql  配置信息

        //  如果 报错信息 和 配置信息 能匹配上 那就从新跑处理过程
        $arr = [];
        $arr_1 = [];
        $num = 0;
        if (!empty($pgsql_info)) {
            foreach ($pgsql_info as $p_k => $p_v) {
                foreach ($mysql_info as $k => $v) {

                    if (!empty(trim($p_v['first_level_id'])) && !empty(trim($p_v['second_level_id']))) {
                        if (($p_v['first_level_id'] == $v['app_package_name'] || $p_v['first_level_id'] == $v['billing_app_id'] || $p_v['first_level_id'] == $v['billing_app_name'] )&&( $p_v['second_level_id'] == $v['billing_point_id'] || $p_v['second_level_id'] == $v['billing_point_name'])) {
                            $arr[] = $p_v['err_date'].'lishuyang@lishuyang'.$p_v['platform_id'].'lishuyang@lishuyang'.$p_v['first_level_id'].'lishuyang@lishuyang'.$p_v['second_level_id'];
//                            $arr[$num]['date'] = $p_v['err_date'];
//                            $arr[$num]['platform_id'] = $p_v['platform_id'];
//                            $arr[$num]['platform_app_id'] = $p_v['first_level_id'];
//                            $arr[$num]['ad_slot_id'] = $p_v['second_level_id'];
//                            $num++;
                            $arr_1[$p_v['err_date'] . '_' . $p_v['platform_id']] = $p_v['platform_id'];
                        }
                    } elseif (empty(trim($p_v['first_level_id']))) {
                        if ($p_v['second_level_id'] == $v['app_package_name'] || $p_v['second_level_id'] == $v['billing_point_id'] || $p_v['second_level_id'] == $v['billing_point_name'] ) {
                            $arr[] = $p_v['err_date'].'lishuyang@lishuyang'.$p_v['platform_id'].'lishuyang@lishuyang'.$p_v['first_level_id'].'lishuyang@lishuyang'.$p_v['second_level_id'];
//                            $arr[$num]['date'] = $p_v['err_date'];
//                            $arr[$num]['platform_id'] = $p_v['platform_id'];
//                            $arr[$num]['platform_app_id'] = $p_v['first_level_id'];
//                            $arr[$num]['ad_slot_id'] = $p_v['second_level_id'];
//                            $num++;
                            $arr_1[$p_v['err_date'] . '_' . $p_v['platform_id']] = $p_v['platform_id'];
                        }

                    } elseif (empty(trim($p_v['second_level_id']))) {
                        if ($p_v['first_level_id'] == $v['app_package_name'] || $p_v['first_level_id'] == $v['billing_app_id'] || $p_v['first_level_id'] == $v['billing_app_name'] ) {
                            $arr[] = $p_v['err_date'].'lishuyang@lishuyang'.$p_v['platform_id'].'lishuyang@lishuyang'.$p_v['first_level_id'].'lishuyang@lishuyang'.$p_v['second_level_id'];
//                            $arr[$num]['date'] = $p_v['err_date'];
//                            $arr[$num]['platform_id'] = $p_v['platform_id'];
//                            $arr[$num]['platform_app_id'] = $p_v['first_level_id'];
//                            $arr[$num]['ad_slot_id'] = $p_v['second_level_id'];
//                            $num++;
                            $arr_1[$p_v['err_date'] . '_' . $p_v['platform_id']] = $p_v['platform_id'];
                        }


                    }

                }
            }
        }

        if (!empty($arr_1)) {
            $sql = 'select platform_id,commond_name from c_data_commond where type  =2';
            $info = DB::select($sql);
            $info = Service::data($info);
            foreach ($arr_1 as $plat_k => $plat_v) {
                foreach ($info as $plat_k_1 => $plat_v_1) {
                    $plat_date = explode('_', $plat_k);
                    if ($plat_v == $plat_v_1['platform_id']) {
                        Artisan::call($plat_v_1['commond_name'], ['dayid' => $plat_date[0]]);
                    }
                }
            }

        }
        if (!empty($arr)) {
            $arr = array_unique($arr);
            foreach ($arr as $key => $value) {
                $value = explode('lishuyang@lishuyang',$value);
                if ($value) {
                    $sel_sql = " select * from error_log.error_info    where status = 0 and err_date ='" . $value[0] . "' and platform_id ='" . $value[1] . "'  and first_level_id ='" . $value[2] . "' and second_level_id ='" . $value[3] . "' ";
                    $sel_info = DB::connection('pgsql')->select($sel_sql);
                    $sel_info = Service::data($sel_info);
                    if (count($sel_info) > 0) {
                        $update_sql = " update error_log.error_info set status = 1   where status = 0 and err_date ='" . $value[0] . "' and platform_id ='" . $value[1] . "'  and first_level_id ='" . $value[2] . "' and second_level_id ='" . $value[3] . "' and status = 0 ";
                        DB::connection('pgsql')->update($update_sql);
                    }
                }
            }

        }
        //var_dump($arr);
        //die;


    }

    public function pgsql()
    {
        //抓回来的原始数据的求和
        $pgsql = "SELECT
        distinct
        first_level_id,
        second_level_id,
        platform_id,
        err_date
        FROM
        error_log.error_info
        WHERE
        platform_type =3
        and  status =0 and account !='noodlecake' and platform_id='pff03'";
        $pgsql_info = DB::connection('pgsql')->select($pgsql);
        $pgsql_info = Service::data($pgsql_info);
        return $pgsql_info;

    }


    public function mysql()
    {
        $mysql_sql = "SELECT DISTINCT
         `c_billing_point`.`billing_point_name`,
         `c_billing_point`.`billing_point_id`,
          c_billing.billing_app_id,
          c_billing.billing_app_name,
         `c_billing`.`app_package_name`
        FROM
            `c_app`
        LEFT JOIN `c_billing` ON `c_billing`.`app_id` = `c_app`.`id`
        LEFT JOIN `c_billing_point` ON `c_billing_point`.`app_id` = `c_app`.`id`
        WHERE
            `c_billing`.`app_id` IS NOT NULL
        OR `c_billing_point`.`app_id` IS NOT NULL ";
        $mysql_info = DB::select($mysql_sql);
        $mysql_info = Service::data($mysql_info);
        return $mysql_info;

    }

}