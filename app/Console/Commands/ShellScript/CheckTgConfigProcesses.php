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

class CheckTgConfigProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'CheckTgConfigProcesses {dayid?} ';

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

        $arr =[];
        $arr_1 =[];
        $num =0;
        if(!empty($pgsql_info)){
            foreach ($pgsql_info as $p_k => $p_v ){
                foreach ($mysql_info as $k => $v){
                    if(!empty($p_v['first_level_id']) && !empty($p_v['second_level_id'])){
                        if(($p_v['first_level_id'] == $v['application_id'] && $p_v['second_level_id'] == $v['campaign_id'] )|| ($p_v['first_level_id'] == $v['application_name'] && $p_v['second_level_id'] == $v['campaign_name']) ){
                            $arr[] = $p_v['err_date'].'lishuyang@lishuyang'.$p_v['platform_id'].'lishuyang@lishuyang'.$p_v['first_level_id'].'lishuyang@lishuyang'.$p_v['second_level_id'];
//                            $arr[$num]['date'] = $p_v['err_date'];
//                            $arr[$num]['platform_id'] = $p_v['platform_id'];
//                            $arr[$num]['platform_app_id'] = $p_v['first_level_id'];
//                            $arr[$num]['ad_slot_id'] = $p_v['second_level_id'];
//                            $num ++;
                            $arr_1[$p_v['err_date'].'_'.$p_v['platform_id']] = $p_v['platform_id'];
                        }
                    }elseif(empty($p_v['first_level_id']) ){
                        if($p_v['second_level_id'] == $v['campaign_id'] || $p_v['second_level_id'] == $v['campaign_name']  ){
                            $arr[] = $p_v['err_date'].'lishuyang@lishuyang'.$p_v['platform_id'].'lishuyang@lishuyang'.$p_v['first_level_id'].'lishuyang@lishuyang'.$p_v['second_level_id'];
//                            $arr[$num]['date'] = $p_v['err_date'];
//                            $arr[$num]['platform_id'] = $p_v['platform_id'];
//                            $arr[$num]['platform_app_id'] = $p_v['first_level_id'];
//                            $arr[$num]['ad_slot_id'] = $p_v['second_level_id'];
//                            $num ++;
                            $arr_1[$p_v['err_date'].'_'.$p_v['platform_id']] = $p_v['platform_id'];
                        }

                    }elseif (empty($p_v['second_level_id'])){
                        if($p_v['first_level_id'] == $v['application_id'] || $p_v['first_level_id'] == $v['application_name']  ){
                            $arr[] = $p_v['err_date'].'lishuyang@lishuyang'.$p_v['platform_id'].'lishuyang@lishuyang'.$p_v['first_level_id'].'lishuyang@lishuyang'.$p_v['second_level_id'];
//                            $arr[$num]['date'] = $p_v['err_date'];
//                            $arr[$num]['platform_id'] = $p_v['platform_id'];
//                            $arr[$num]['platform_app_id'] = $p_v['first_level_id'];
//                            $arr[$num]['ad_slot_id'] = $p_v['second_level_id'];
//                            $num ++;
                            $arr_1[$p_v['err_date'].'_'.$p_v['platform_id']] = $p_v['platform_id'];
                        }


                    }

                }
            }
        }
        var_dump($arr_1);
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

        if(!empty($arr)){
            $arr = array_unique($arr);
            foreach ($arr as $key => $value) {
                $value = explode('lishuyang@lishuyang',$value);
                if ($value){
                    $update_sql = " update error_log.error_info set status = 1   where  err_date ='".$value[0]."' and platform_id ='".$value[1]."'  and first_level_id ='".$value[2]."' and second_level_id ='".$value[3]."' and status = 0 ";
                    DB::connection('pgsql')->update($update_sql);
                }

            }

        }


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
        platform_type =4
        and  status =0 and account !='noodlecake' and  platform_id ='ptg40' ";
        $pgsql_info = DB::connection('pgsql')->select($pgsql);
        $pgsql_info = Service::data($pgsql_info);
        return $pgsql_info;

    }


    public function mysql()
    {
        $mysql_sql = "SELECT DISTINCT
        c_app.app_id,
        c_generalize.platform_id,
        c_generalize.application_id,
        c_generalize.application_name,
        c_generalize_ad_app.campaign_id,
        c_generalize_ad_app.campaign_name
        FROM
        c_app
        LEFT JOIN c_generalize ON c_app.id = c_generalize.app_id
        LEFT JOIN c_generalize_ad_app ON c_generalize.id = c_generalize_ad_app.generalize_id";
        $mysql_info = DB::select($mysql_sql);
        $mysql_info = Service::data($mysql_info);

        return $mysql_info;

    }
}