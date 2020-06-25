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

class RedisAdProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'RedisAdProcesses {dayid?} ';

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
    	Redis::select(0);
    	define('MYSQL_AD_TABLE_NAME','zplay_ad_report_daily');
        $ad_info = env('REDIS_AD_KEYS');

        //获取广告数据长度
        $ad_len = Redis::llen($ad_info);

        if($ad_len>0){

            DB::beginTransaction();
            $be_time = time();
            var_dump($be_time);

            $date_arr = [];
            $platform_arr = [];

            $ad_sql = "insert into ".MYSQL_AD_TABLE_NAME." (`date`,`app_id`,`version`,`channel_id`,`country_id`,`data_platform_id`,`data_account`,`platform_id`,`ad_type`,`statistics`,`platform_app_id`,`platform_app_name`,`ad_unit_id`,`ad_unit_name`,`round`,`all_request`,`success_requests`,`fail_requests`,`impression_port`,`impression_begin`,`impression`,`click`,`download`,`activate`,`reward`,`earning`,`earning_exc`,`earning_flowing`,`earning_fix`,`flow_type`,`remark`,`create_time`,`update_time`,`earning_usd`,`earning_exc_usd`)values";
             for ($i=1; $i <=$ad_len ; $i++) {
                $str = Redis::lpop($ad_info);

                if(strpos($str,'lishuyang@lishuyang') !=false){
                    var_dump($str);
                    $plat_date = explode( 'lishuyang@lishuyang',$str);
                    $date_arr[$i] =$plat_date[1];
                    $platform_arr[$i] = $plat_date[0];
                    $sel_info = DB::table('zplay_ad_report_daily')->where(["platform_id" => $plat_date[0], "date" => $plat_date[1]])->count();
                    var_dump($plat_date[0].'-'.$plat_date[1].'-'.'数据条数'.$sel_info);

                    if($sel_info){
                        $del_sql = "delete  from zplay_ad_report_daily where platform_id = '$plat_date[0]' and date ='$plat_date[1]' and data_platform_id <> 'pad262' " ;
                        $del_info =DB::delete($del_sql);
                        if(!$del_info){
                            var_dump(1);
                            DB::rollBack();
                        }
                    }

                }else{
                    var_dump($str);
                    $insert_info =DB::insert($ad_sql.$str);
                    if(!$insert_info){
                        var_dump(2);
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
        	//var_dump($platform_str);die;

            $sel_sql = "select count(1) as count  FROM
                zplay_basic_report_daily
            WHERE
                plat_type = 'ad'
            AND date_time >= '$begin_date'  and   date_time <= '$end_date' and platform_id in ($platform_str)";
            $sel_info = DB::select($sel_sql);
            $sel_info = Service::data($sel_info);
            if($sel_info[0]['count'] !=0){

                $basic_del_sql ="   DELETE
                FROM
                    zplay_basic_report_daily
                WHERE
                    plat_type = 'ad'
                AND date_time >= '$begin_date'  and   date_time <= '$end_date' and platform_id in ($platform_str)" ;
                $update_info =DB::delete($basic_del_sql);

                if(!$update_info){
                    var_dump(3);
                    DB::rollBack();
                }

            }




            // 更新 没有请求数 有成功请求数
//            $basic_update_sql = "UPDATE zplay_ad_report_daily  set all_request =success_requests WHERE  (all_request is NULL or all_request = 0) and `success_requests` > 0 and date >= '$begin_date'  and   date <= '$end_date' ";
//            DB::update($basic_update_sql);

            $basic_insert_sql ="INSERT INTO zplay_basic_report_daily (
                game_creator,
                os_id,
                game_category_id,
                game_group,
                plat_type,
                platform_id,
                date_time,
                app_id,
                country_id,
                earning_type,
                request_all,
                request,
                impression,
                click,
                earning_fix_ad,
                income_fix_ad,
                create_time,
                flow_type,
                statistics,
                channel_id, 
                income_usd_ad,
                earning_usd_ad
            )
            SELECT
                app.company_id AS game_creator,
                app.os_id AS os_id,
                app.app_category_id AS game_category_id,
                app.release_group AS game_group,
                'ad' AS plat_type,
                ad.platform_id,
                ad.date,
                app.id,
                ad.country_id,
                ad.ad_type AS earning_type,
                sum(ad.all_request) AS request_all,
                sum(ad.success_requests) AS request,
                sum(ad.impression) AS impression,
                sum(ad.click) AS click,
                sum(ad.earning_flowing) AS earning_fix_ad,
                sum(ad.earning_fix) AS income_fix_ad,
                now(),
                ad.flow_type,
                ad.statistics,
                channel.id AS channel_id,
                sum(ad.earning_exc_usd) AS income_usd_ad,
                sum(ad.earning_usd) AS earning_usd_ad
            FROM
                zplay_ad_report_daily ad
            LEFT JOIN c_app app ON ad.app_id = app.app_id
            LEFT JOIN c_channel channel ON ad.channel_id = channel.channel_id
            WHERE
                ad.date >= '$begin_date'  and   ad.date <= '$end_date'  and ad.platform_id in ($platform_str)
            GROUP BY
                ad.date,
                ad.app_id,
                ad.country_id,
                ad.platform_id,
                ad.ad_type,
                ad.flow_type,
                ad.statistics";
                $insert_info_1 = DB::insert($basic_insert_sql);
                if(!$insert_info_1){
                    var_dump(4); 
                    DB::rollBack();
                }


//                $update_sql_1 ="UPDATE zplay_basic_report_daily usd,
//                 c_currency_ex cur
//                SET usd.income_usd_ad = usd.income_fix_ad / cur.currency_ex
//                WHERE
//                    cur.`effective_time` = date_format(usd.date_time, '%Y%m')
//                AND cur.`currency_id` = 60
//                AND usd.income_fix_ad = usd.income_usd_ad
//                AND usd.`plat_type` = 'ad' and usd.date_time >= '$begin_date'  and   usd.date_time <= '$end_date'";
//
//                 $update_info_1 = DB::UPDATE($update_sql_1);
//                 if (!$update_info_1){
//                     DB::rollBack();
//                 }

//                 $update_sql_2 ="UPDATE zplay_basic_report_daily usd,
//                 c_currency_ex cur
//                SET usd.earning_usd_ad = usd.earning_fix_ad / cur.currency_ex
//                WHERE
//                    cur.`effective_time` = date_format(usd.date_time, '%Y%m')
//                AND cur.`currency_id` = 60
//                AND usd.earning_fix_ad = usd.earning_usd_ad
//                AND usd.`plat_type` = 'ad' and usd.date_time >= '$begin_date'  and   usd.date_time <= '$end_date'";
//                $update_sql_2= DB::UPDATE($update_sql_2);
//                if (!$update_sql_2){
//                    DB::rollBack();
//                }

                $sql = "select sum(earning) as cost,platform_id,date,data_account from  ".MYSQL_AD_TABLE_NAME." where date between '$begin_date' and '$end_date' group by  platform_id,date,data_account";
                $report_list = DB::select($sql);
                $report_list = Service::data($report_list);
                if ($report_list){
                // 保存广告平台
                    foreach ($report_list as $value){
                        if ($value['data_account']){
                            $info = PlatformImp::add_platform_status($value['platform_id'],$value['data_account'],$value['cost'],$value['date']);
                            if(!$info){
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