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

class CheckAdShowProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'CheckAdShowProcesses {dayid?} ';

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
        $BeginDate=date('Y-m-d', strtotime('-5 day'));
        $EndDate=date('Y-m-d',strtotime('-1 day'));
        //检查处理过后 和 显示的数据
        $plat_date_show = self::check_mysql_show($BeginDate,$EndDate);
        if(count($plat_date_show)>0){
            $error_plat = [];
            $num = 0;
            foreach ($plat_date_show as $key => $value) {
                    $platform_id = $value['platform_id'];
                    $date = $value['date'];
                    self::ad_summary($date,$date,$platform_id);
                    //调完处理过程 之后 现在对一遍数据 如果还有不对;
                    $error_plat[$num]['platform_id']=$platform_id;
                    $error_plat[$num]['date']=$date;
                    $error_plat[$num]['diff']=$value['diff'];
                    $num++;

            }

            if(count($error_plat)>0){
                $str = '';
                foreach ($error_plat as $key => $value) {
                    $platform_sql = "select DISTINCT platform_id,platform_name from c_platform where  platform_id='".$value['platform_id']."'";
                    $platform_name = DB::SELECT($platform_sql);
                    $platform_name =Service::data($platform_name);
                    $platform_name =$platform_name[0]['platform_name'];
                    $platform_id = $value['platform_id'];
                    $str .="$platform_id($platform_name) 日期：".$value['date']."金额为:".$value['diff'].",";
                    
                }

                DataImportImp::saveDataErrorLog(3,'最近五天广告处理数据与显示数据对数','广告处理数据与显示数据对数',6,rtrim($str,','));
                $date =date('Y-m-d');
                $error_msg_mail[0]='最近五天广告处理数据与显示数据对数人民币大于100的错误平台和日期:'.$str;
                // 发送邮件
                CommonFunction::sendMail($error_msg_mail,$date.'数据不准平台和时间信息');
            }

        }


        
 
        //var_dump($plat_date_lt);

        //var_dump($info);die;
        //echo '处理完成';
    }
    public function check_mysql_show($BeginDate,$EndDate,$platform_id=''){
        if($platform_id!=''){
            $mysql_where = " and platform_id  = '$platform_id'";
        }else {
            $mysql_where ='';
        }
        //显示的求和
        $pgsql = "select sum(income_usd_ad) as income_usd_ad ,sum(income_fix_ad) as income ,platform_id as source_id,date_time as dayid   from zplay_basic_report_daily where plat_type ='ad' and date_time between '$BeginDate' and '$EndDate' $mysql_where GROUP BY platform_id,date_time ";
        //echo $pgsql;die;
        $pgsql_info =DB::select($pgsql);
        $pgsql_info = Service::data($pgsql_info);
        //var_dump($pgsql_info);
        //mysql 处理数据求和；
        $mysql_sql ="select sum(earning_fix) as earning ,platform_id,date from zplay_ad_report_daily where date BETWEEN '$BeginDate' AND '$EndDate' GROUP BY platform_id,date";
        $mysql_info =DB::select($mysql_sql);
        $mysql_info = Service::data($mysql_info);
        //var_dump($mysql_info);
        $plat_date = [];
        $plat_date_lt = [];
        $i = 0;
        foreach ($pgsql_info as $key => $value) {
            foreach ($mysql_info as $k => $v) {
                if($value['source_id'] ==$v['platform_id'] && $value['dayid'] ==$v['date'] ){
                    if($v['earning']-$value['income']>100 ||  $value['income_usd_ad'] ==0 ){
                        $plat_date[$i]['platform_id']=$v['platform_id'];
                        $plat_date[$i]['date']=$v['date'];
                        $plat_date[$i]['diff']=$v['earning']-$value['income'];
                         $i++;
                    }
                }

            }
        }
        return $plat_date;//$plat_date_lt;

    }
    public function ad_summary($begin_date,$end_date,$platform_id){
            DB::beginTransaction();
            $sel_sql = "select count(1) as count  FROM
            zplay_basic_report_daily
            WHERE
            plat_type = 'ad'
            AND date_time >= '$begin_date'  and   date_time <= '$end_date' ";
            $sel_info = DB::select($sel_sql);
            $sel_info = Service::data($sel_info);
            if($sel_info[0]['count'] !=0){

                $basic_del_sql ="   DELETE
                FROM
                zplay_basic_report_daily
                WHERE
                plat_type = 'ad'
                AND date_time >= '$begin_date'  and   date_time <= '$end_date' and platform_id ='$platform_id' " ; 
                $update_info =DB::delete($basic_del_sql);

                if(!$update_info){

                    DB::rollBack();
                }
            }


            // 更新 没有请求数 有成功请求数
//            $basic_update_sql = "UPDATE zplay_ad_report_daily  set all_request =success_requests WHERE  all_request is NULL ";
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
                ad.date >= '$begin_date'  and   ad.date <= '$end_date'   and platform_id ='$platform_id'
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
                    //var_dump(4);
                    DB::rollBack();
                }


                $update_sql_1 ="UPDATE zplay_basic_report_daily usd,
                 c_currency_ex cur
                SET usd.income_usd_ad = usd.income_fix_ad / cur.currency_ex
                WHERE
                    cur.`effective_time` = date_format(usd.date_time, '%Y%m')
                AND cur.`currency_id` = 60
                AND usd.income_fix_ad = usd.income_usd_ad
                AND usd.`plat_type` = 'ad' and usd.date_time >= '$begin_date'   and   usd.date_time <= '$end_date' and platform_id ='$platform_id' ";

                $update_info_1 = DB::UPDATE($update_sql_1);
//                if (!$update_info_1){
//                    DB::rollBack();
//                }

                $update_sql_2 ="UPDATE zplay_basic_report_daily usd,
                 c_currency_ex cur
                SET usd.earning_usd_ad = usd.earning_fix_ad / cur.currency_ex
                WHERE
                    cur.`effective_time` = date_format(usd.date_time, '%Y%m')
                AND cur.`currency_id` = 60
                AND usd.earning_fix_ad = usd.earning_usd_ad
                AND usd.`plat_type` = 'ad' and usd.date_time >= '$begin_date'  and   usd.date_time <= '$end_date' and platform_id ='$platform_id' ";
                $update_sql_2 = DB::UPDATE($update_sql_2);
//                if (!$update_sql_2){
//                    DB::rollBack();
//                }

                DB::commit();
    }
}