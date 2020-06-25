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

class RedisTgProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'RedisTgProcesses {dayid?} ';

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


        $ad_info = env('REDIS_AD_KEYS');
        //获取广告数据长度
        $ad_len = Redis::llen($ad_info);
        if($ad_len>0){
            die;
        }
    	set_time_limit(0);
    	Redis::select(0);
    	define('MYSQL_TG_TABLE_NAME','zplay_tg_report_daily');
        $tg_info = env('REDIS_TG_KEYS');
        
        $dayid = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-4 day'));
        $date = date('Ymd',strtotime($dayid));
        //获取广告数据长度
        $tg_len = Redis::llen($tg_info);


        if($tg_len>0){

            DB::beginTransaction();
            echo "<pre>";
            $be_time = time();
            var_dump($be_time);


            $date_arr = [];
            $platform_arr = [];
        	//需要修改
        	 $ad_sql = "insert into ".MYSQL_TG_TABLE_NAME." (`date`,`app_id`,`channel_id`,`country_id`,`platform_id`,`agency_platform_id`,`data_platform_id`,`type`,`platform_account`,`data_account`,`cost_type`,`platform_app_id`,`platform_app_name`,`ad_id`,`ad_name`,`ad_type`,`tongji_type`,`impression`,`click`,`new`,`new_phone`,`new_pad`,`cost`,`cost_exc`,`device_type`,`remark`,`create_time`,`update_time`,`cost_usd`)values";
        	 for ($i=1; $i <=$tg_len ; $i++) { 
                $str = Redis::lpop($tg_info);

                if(strpos($str,'lishuyang@lishuyang') !=false){
                    var_dump($str);
                    $plat_date = explode( 'lishuyang@lishuyang',$str);
                    $date_arr[$i] =$plat_date[1];
                    $platform_arr[$i] = $plat_date[0];
                    $sel_info = DB::table('zplay_tg_report_daily')->where(["platform_id" => $plat_date[0], "date" => $plat_date[1]])->count();
                    var_dump($plat_date[0].'-'.$plat_date[1].'-'.'数据条数'.$sel_info);

                    if($sel_info){
                        var_dump('delete');
                        $del_sql = "delete  from zplay_tg_report_daily where platform_id = '$plat_date[0]' and date ='$plat_date[1]'" ;
                        $del_info =DB::delete($del_sql);
                        if(!$del_info){
                            //var_dump(1);
                            DB::rollBack();
                        }
                    }

                }else{
                    var_dump($str);
                    $insert_info =DB::insert($ad_sql.$str);
                    if(!$insert_info){
                        //var_dump(2);
                        DB::rollBack();
                    }
                }
        	 }
            DB::commit();
            /**************************处理到显示数据的表格******************************/
            DB::beginTransaction();
        	$platform_date = array_unique($date_arr);
            $platform_id = array_unique($platform_arr);
            sort($platform_date);
        	if(count($platform_date) >1){
        		
        		$end = count($platform_date)-1;
                $begin_date = $platform_date[0];
                $end_date = $platform_date[$end];
        		

        	}else{
        	 	$begin_date = $platform_date[0];
                $end_date = $platform_date[0];
            }
            $platform_str = '';
            foreach ($platform_id as  $v){
                $platform_str .= "'".$v."',";
            }
            $platform_str = rtrim($platform_str,",");

            $sel_sql = "select count(1) as count  FROM
            zplay_basic_report_daily
            WHERE
            plat_type = 'ct'
            AND date_time >= '$begin_date'  and   date_time <= '$end_date' and platform_id in ($platform_str) ";
            $sel_info = DB::select($sel_sql);
            $sel_info = Service::data($sel_info);
            if($sel_info[0]['count'] !=0){

                $basic_del_sql ="   DELETE
                FROM
                zplay_basic_report_daily
                WHERE
                plat_type = 'ct'
                AND date_time >= '$begin_date'  and   date_time <= '$end_date' and platform_id in ($platform_str)" ;
                $update_info =DB::delete($basic_del_sql);

                if(!$update_info){
                var_dump('delete-失败');
                    DB::rollBack();
                }
            }


            $basic_insert_sql ="INSERT INTO zplay_basic_report_daily (
            earning_type,
            game_creator,
            os_id,
            game_category_id,
            game_group,
            plat_type,
            date_time,
            app_id,
            cost_new,
            cost_tg,
            create_time,
            platform_id,
            country_id,
            cost_usd_tg,
            channel_id
            ) SELECT
            'T' as earning_type,
            app.company_id AS game_creator,
            app.os_id AS os_id,
            app.app_category_id AS game_category_id,
            app.release_group AS game_group,
            'ct' AS plat_type,
            tg.date,
            app.id AS app_id,
            sum(tg.new) AS cost_new,
            sum(tg.cost_exc) AS cost_tg,
            now(),
            tg.platform_id,
            tg.country_id,
            sum(tg.cost_usd) AS cost_usd_tg,
            c_channel.id
            FROM
            zplay_tg_report_daily tg
            LEFT JOIN c_app app ON tg.app_id = app.app_id
            LEFT JOIN c_channel  ON tg.channel_id = c_channel.channel_id
            WHERE
            tg.date >= '$begin_date'
            AND tg.date <= '$end_date' and tg.platform_id in ($platform_str)  -- and tg.tongji_type = 0
            GROUP BY
            tg.date,
            tg.app_id,
            tg.country_id,
            tg.agency_platform_id,
            tg.platform_id,
            tg.data_account,
            tg.ad_type,
            tg.channel_id";
            $insert_info_1 = DB::insert($basic_insert_sql);
            if(!$insert_info_1){
                var_dump('insert-失败');
                DB::rollBack();
            }


//            $update_sql_1 ="UPDATE zplay_basic_report_daily usd,
//            c_currency_ex cur
//            SET usd.cost_usd_tg = usd.cost_tg / cur.currency_ex
//            WHERE
//            cur.`effective_time` = date_format(usd.date_time, '%Y%m')
//            AND cur.`currency_id` = 60
//            AND usd.cost_usd_tg = usd.cost_tg
//            AND usd.`plat_type` = 'ct' and usd.date_time >= '$begin_date'  and   usd.date_time <= '$end_date'";
//
//            $update_info_1 = DB::UPDATE($update_sql_1);
//            if (!$update_info_1){
//                DB::rollBack();
//            }
            
            $sql = "select sum(cost) as cost,platform_id,date,data_account from  ".MYSQL_TG_TABLE_NAME." where date between '$begin_date' and '$end_date' group by  platform_id,date,data_account ";
            $report_list = DB::select($sql);
            $report_list = Service::data($report_list);
            if ($report_list){
                // 保存广告平台
                foreach ($report_list as $value){
                    if ($value['data_account']){
                        $info = PlatformImp::add_platform_status($value['platform_id'],$value['data_account'],$value['cost'],$value['date']);
                        if(!$info){
                            var_dump('status-失败');
                            DB::rollBack();
                        }
                    }
                }
            }

            DB::commit();
            $en_time = time();
            var_dump($en_time);
            var_dump($be_time-$en_time);




        }


        


        //var_dump($info);die;
        //echo '处理完成';
    }
}