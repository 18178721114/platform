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

class CheckTgOriginalDataProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'CheckTgOriginalDataProcesses {begin_date?}  {end_date?}';

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
        echo '<pre>';
        set_time_limit(0);
        $begin_date = $this->argument('begin_date') ? $this->argument('begin_date') : date('Y-m-d',strtotime('-11 day'));
        $end_date = $this->argument('end_date') ? $this->argument('end_date') : date('Y-m-d',strtotime('-9 day'));
        //抓回来的原始数据的求和
        $pgsql = "SELECT
        dayid,
        source_id,
        sum(cost) as cost
        FROM
        tg_data.erm_data
        WHERE
        type =2
        and  dayid between '$begin_date' and '$end_date' and cost !=0  group by dayid,source_id  HAVING  sum(cost)>0";
        $pgsql_info = DB::connection('pgsql')->select($pgsql);
        $pgsql_info = Service::data($pgsql_info);
        $pgsql_info_test = DB::connection('pgsql_test')->select($pgsql);
        $pgsql_info_test = Service::data($pgsql_info_test);
        //$mysql_info = self::pgsql_test($pgsql);// 回来的原始数据存在测式环境
        //  日期相等 平台相等 如果 收入 不相等 记录下来  吧数据覆盖
       $arr_1 =[];
        if ($pgsql_info &&  $pgsql_info_test) {
            foreach ($pgsql_info as $key => $value) {
                foreach ($pgsql_info_test as $k => $v) {
                    if($value['dayid'] == $v['dayid'] && $value['source_id'] == $v['source_id'] && $value['cost'] < $v['cost'] ){
                        $arr_1[$v['dayid'].'_'.$v['source_id']] = $v['source_id'].'-'.$value['cost'].'-'.$v['cost'];

                    }

                }
                
            }
        }
        
        //var_dump($arr_1);
        if(!empty($arr_1)){
            $sql = 'select platform_id,commond_name from c_data_commond where type  =2';
            $info_plat = DB::select($sql);
            $info_plat = Service::data($info_plat);
            foreach ($arr_1 as $plat_k => $plat_v) {
                foreach ($info_plat as $plat_k_1 => $plat_v_1) {
                    $plat_date  = explode('_',$plat_k);
                    if($plat_date[1] == $plat_v_1['platform_id'] ){
                        $sel_sql = "SELECT count(1) as count from  tg_data.erm_data where dayid = '$plat_date[0]' and source_id = '$plat_date[1]'";
                        $count = DB::connection('pgsql')->select($sel_sql);
                        $count = Service::data($count);
                        //var_dump($count);
                        if($count[0]['count']){
                            $delete_sql = "delete from tg_data.erm_data where dayid = '$plat_date[0]' and source_id = '$plat_date[1]'";
                            DB::connection('pgsql')->delete($delete_sql);

                        }
                        $sel_sql = "SELECT * from  tg_data.erm_data where cost !=0 and  dayid = '$plat_date[0]' and source_id = '$plat_date[1]'";
                        //echo  $sel_sql;
                        $info = DB::connection('pgsql_test')->select($sel_sql);
                        $info = Service::data($info);
                        //var_dump(count($info));
                        $step = array();
                        $i = 0;
                        foreach ($info as $kkkk => $insert_data_info) {
                            if ($kkkk % 2000 == 0) $i++;
                            if ($insert_data_info) {
                                $step[$i][] = $insert_data_info;
                            }
                        }
                        if ($step) {
                            foreach ($step as $k => $v) {
                                $result = DataImportLogic::insertChannelData('tg_data','erm_data',$v);
                                if (!$result) {
                                   echo 'mysql_error'. PHP_EOL;
                               }
                           }
                       }
                        Artisan::call($plat_v_1['commond_name'],['dayid' => $plat_date[0]]);
                    }
                }
            }

        }
        //var_dump($arr);
        //die;


    }

    public function pgsql_test($pgsql)
    {
        $pgsql_info = DB::connection('pgsql')->select($pgsql);
        $pgsql_info = Service::data($pgsql_info);
        return $pgsql_info;

    }


    public function mysql($pgsql)
    {
        $pgsql_info = DB::connection('pgsql_test')->select($pgsql);
        $pgsql_info = Service::data($pgsql_info);
        return $pgsql_info;

    }

}