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

class CheckTjConfigProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'CheckTjConfigProcesses {dayid?} ';

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
        $flurry_conf = self::flurryMysql();// flurry mysql  配置信息
        $td_foreign_user_conf = self::tdForeignUserMysql();
        $td_china_user_conf = self::tdChinaUserMysql();
        $td_keep_conf = self::tdKeepMysql();
        //  如果 报错信息 和 配置信息 能匹配上 那就从新跑处理过程
        self::checkFlurryUser($pgsql_info,$flurry_conf);

        self::checkTalkDataUser($pgsql_info,$td_foreign_user_conf);

    }

    public static function checkTalkDataUser($pgsql_info,$td_foreign_user_conf){
        $arr =[];
        $arr_1 =[];
        $num =0;
        if(!empty($pgsql_info)){
            foreach ($pgsql_info as $p_k => $p_v ){
                foreach ($td_foreign_user_conf as $k => $v){
                    if(!empty($p_v['first_level_id']) && !empty($p_v['second_level_id']) && !empty($p_v['second_level_name'])){
                        if($p_v['first_level_id'] == $v['td_app_id'] && $p_v['second_level_id'] == $v['statistic_version'] && ($p_v['second_level_name'] == $v['td_channel_id'] || $p_v['second_level_name'] == $v['channel_id'])){
                            $arr[$num]['date'] = $p_v['err_date'];
                            $arr[$num]['platform_id'] = $p_v['platform_id'];
                            $arr[$num]['app_name'] = $p_v['first_level_id'];
                            $arr[$num]['version'] = $p_v['second_level_id'];
                            $arr[$num]['channel_id'] = $p_v['second_level_name'];
                            $num ++;
                            $arr_1[] = $p_v['err_date'];
                        }
                    }
                }
            }
        }

        if(!empty($arr_1)){
            $arr_1 = array_unique($arr_1);
            foreach ($arr_1 as $plat_v) {
                Artisan::call("TdForeignUserTjHandleProcesses",['dayid' => $plat_v]);
                Artisan::call("TdUserTjHandleProcesses",['dayid' => $plat_v]);
                Artisan::call("TdKeepTjHandleProcesses",['dayid' => $plat_v]);
            }

        }
        if(!empty($arr)){
            foreach ($arr as $key => $value) {
                $update_sql = " update error_log.error_info set status = 1   where  err_date ='".$value['date']."' and platform_id ='".$value['platform_id']."'  and first_level_id ='".$value['app_name']."' and second_level_id ='".$value['version']."' and second_level_name ='".$value['channel_id']."' and td_err_type = 1 and status = 0";
                DB::connection('pgsql')->update($update_sql);

                $update_sql_1 = " update error_log.error_info set status = 1   where  err_date ='".$value['date']."' and platform_id ='".$value['platform_id']."'  and first_level_id ='".$value['app_name']."' and second_level_id ='".$value['version']."' and second_level_name ='".$value['channel_id']."' and td_err_type = 2 and status = 0";
                DB::connection('pgsql')->update($update_sql_1);

                $update_sql_2 = " update error_log.error_info set status = 1   where  err_date ='".$value['date']."' and platform_id ='".$value['platform_id']."'  and first_level_id ='".$value['app_name']."' and second_level_name ='".$value['channel_id']."' and td_err_type = 3 and status = 0";
                DB::connection('pgsql')->update($update_sql_2);
            }
        }
    }

    public static function checkFlurryUser($pgsql_info,$flurry_conf){
        $arr =[];
        $arr_1 =[];
        $num =0;
        if(!empty($pgsql_info)){
            foreach ($pgsql_info as $p_k => $p_v ){
                foreach ($flurry_conf as $k => $v){
                    if(!empty($p_v['first_level_id']) && !empty($p_v['second_level_id'])){
                        if($p_v['first_level_id'] == $v['statistic_app_name'] && $p_v['second_level_id'] == $v['statistic_version']){
                            $arr[$num]['date'] = $p_v['err_date'];
                            $arr[$num]['platform_id'] = $p_v['platform_id'];
                            $arr[$num]['app_name'] = $p_v['first_level_id'];
                            $arr[$num]['version'] = $p_v['second_level_id'];
                            $num ++;
                            $arr_1[] = $p_v['err_date'];
                        }
                    }
                }
            }
        }
        if(!empty($arr_1)){
            $arr_1 = array_unique($arr_1);
            foreach ($arr_1 as $plat_v) {
                Artisan::call("FlurryTjHandleProcesses",['dayid' => $plat_v]);
                Artisan::call("FlurryKeepTjHandleProcesses",['stime' => $plat_v, 'etime' => $plat_v]);
            }

        }
        if(!empty($arr)){
            foreach ($arr as $key => $value) {
                $update_sql = " update error_log.error_info set status = 1   where  err_date ='".$value['date']."' and platform_id ='".$value['platform_id']."'  and first_level_id ='".$value['app_name']."' and second_level_id ='".$value['version']."' and td_err_type = 1 and status = 0";
                DB::connection('pgsql')->update($update_sql);

                $update_sql_1 = " update error_log.error_info set status = 1   where  err_date ='".$value['date']."' and platform_id ='".$value['platform_id']."'  and first_level_id ='".$value['app_name']."' and td_err_type = 3 and status = 0";
                DB::connection('pgsql')->update($update_sql_1);
            }

        }
    }

    public static function pgsql()
    {
        //抓回来的原始数据的求和
        $pgsql = "SELECT distinct
                first_level_id,
                second_level_id,
                second_level_name,
                platform_id,
                platform_name,
                err_date
                FROM
                error_log.error_info
                WHERE
                platform_type =1
                and  status =0 
                and money > 0 ";
        $pgsql_info = DB::connection('pgsql')->select($pgsql);
        $pgsql_info = Service::data($pgsql_info);
        return $pgsql_info;

    }

    // flurry 用户配置
    public static function flurryMysql(){
        $flurry_sql = "SELECT DISTINCT
            c_app.id,
            c_app.app_id,
            c_app_statistic.api_key,
            c_app_statistic.statistic_app_name,
            c_app_statistic_version.app_version,
            c_app_statistic_version.statistic_version,
            c_app_statistic_version.ad_status,
            c_app_statistic_version.channel_id
            FROM
            c_app
            LEFT JOIN c_app_statistic ON c_app.id = c_app_statistic.app_id
            LEFT JOIN c_app_statistic_version ON c_app_statistic.id = c_app_statistic_version.app_statistic_id AND c_app_statistic_version.ad_status  != 2
            WHERE
            c_app_statistic.statistic_type = 1";
        $flurry_conf = DB::select($flurry_sql);
        $flurry_conf = Service::data($flurry_conf);
        return $flurry_conf;
    }



    // td 国家用户数据
    public static function tdForeignUserMysql(){
        $td_foreign_user_sql = "SELECT DISTINCT
        c_app.id,
        c_app.app_id,
        c_app.os_id,
        if(c_app.os_id = 1,2,1) as td_os_id,
        c_app_statistic.td_app_id,
        c_app_statistic.api_key,
        c_app_statistic_version.statistic_app_name,
        c_app_statistic_version.app_version,
        c_app_statistic_version.statistic_version,
        c_app_statistic_version.ad_status,
        c_app_statistic_version.channel_id,
        c_channel.td_channel_id
        FROM
        c_app
        LEFT JOIN c_app_statistic ON c_app.id = c_app_statistic.app_id
        LEFT JOIN c_app_statistic_version ON c_app_statistic.id = c_app_statistic_version.app_statistic_id
        LEFT JOIN c_channel ON c_channel.id = c_app_statistic_version.channel_id
        WHERE
        c_app_statistic.statistic_type = 2
        AND c_app_statistic_version.ad_status != 2";
        $td_foreign_user_conf = DB::select($td_foreign_user_sql);
        $td_foreign_user_conf = Service::data($td_foreign_user_conf);
        return $td_foreign_user_conf;
    }


    // td 国内用户数据
    public static function tdChinaUserMysql(){
        $td_china_user_sql = "SELECT DISTINCT
        c_app.id,
        c_app.app_id,
        c_app.os_id,
        if(c_app.os_id = 1,2,1) as td_os_id,
        c_app_statistic.td_app_id,
        c_app_statistic.api_key,
        c_app_statistic_version.statistic_app_name,
        c_app_statistic_version.app_version,
        c_app_statistic_version.statistic_version,
        c_app_statistic_version.ad_status,
        c_app_statistic_version.channel_id,
        c_channel.td_channel_id
        FROM
        c_app
        LEFT JOIN c_app_statistic ON c_app.id = c_app_statistic.app_id
        LEFT JOIN c_app_statistic_version ON c_app_statistic.id = c_app_statistic_version.app_statistic_id
        LEFT JOIN c_channel ON c_channel.id = c_app_statistic_version.channel_id
        WHERE
        c_app_statistic.statistic_type = 2
        AND c_app_statistic_version.ad_status != 2
        AND c_app.`release_region_id` in (1,3)";
        $td_china_user_conf = DB::select($td_china_user_sql);
        $td_china_user_conf = Service::data($td_china_user_conf);
        return $td_china_user_conf;
    }

    // td 留存配置
    public static function tdKeepMysql(){
        $td_keep_sql = "SELECT DISTINCT
        c_app.id,
        c_app.app_id,
        c_app_statistic.api_key,
        c_app_statistic_version.statistic_app_name,
        c_app_statistic_version.app_version,
        c_app_statistic_version.statistic_version,
        c_app_statistic_version.ad_status,
        c_app_statistic_version.channel_id,
        c_channel.td_channel_id
        FROM
        c_app
        LEFT JOIN c_app_statistic ON c_app.id = c_app_statistic.app_id
        LEFT JOIN c_app_statistic_version ON c_app_statistic.id = c_app_statistic_version.app_statistic_id
        LEFT JOIN c_channel ON c_channel.id = c_app_statistic_version.channel_id
        WHERE
        c_app_statistic.statistic_type = 2
        AND c_app_statistic_version.ad_status != 2";
        $td_keep_conf = DB::select($td_keep_sql);
        $td_keep_conf = Service::data($td_keep_conf);
        return $td_keep_conf;
    }

}