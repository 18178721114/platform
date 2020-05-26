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

class CheckTgShowProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'CheckTgShowProcesses {dayid?} ';

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

    	echo '<pre>';
        //检查处理过后 和 显示的数据
        $plat_date_show = self::check_mysql_show($BeginDate,$EndDate);
        if(count($plat_date_show)>0){
            $error_plat = [];
            $num = 0;
            foreach ($plat_date_show as $key => $value) {
                    $platform_id = $value['platform_id'];
                    $date = $value['date'];
                    self::tg_summary($date,$date,$platform_id);
                    $error_plat[$num]['platform_id']=$platform_id;
                    $error_plat[$num]['date']=$date;
                    $error_plat[$num]['diff']=$value['diff'];
                    $num++;
            }
            //调完处理过程 之后 现在对一遍数据 如果还有不对;

            //var_dump($error_plat);die;
            if(count($error_plat)>0){
                $str = '';
                foreach ($error_plat as $key => $value) {
                    if($value['platform_id'] =='ptg00') continue; // ptg00 shi 测试 未知 平台
                    $platform_sql = "select DISTINCT platform_id,platform_name from c_platform where  platform_id='".$value['platform_id']."'";
                    $platform_name = DB::SELECT($platform_sql);
                    $platform_name =Service::data($platform_name);
                    $platform_name =$platform_name[0]['platform_name'];
                    $platform_id = $value['platform_id'];
                    $str .="$platform_id($platform_name) 日期：".$value['date']."金额为:".$value['diff'].",";
                    
                }

                DataImportImp::saveDataErrorLog(3,'最近五天推广处理数据与显示数据对数','推广处理数据与显示数据对数',6,rtrim($str,','));
                $date =date('Y-m-d');
                $error_msg_mail[0]='最近五天推广处理数据与显示数据对数人民币大于100的错误平台和日期:'.$str;
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
        $pgsql = "select sum(cost_usd_tg) as cost_usd_tg ,sum(cost_tg) as cost_tg ,platform_id as source_id,date_time as dayid   from zplay_basic_report_daily where plat_type ='ct' and date_time between '$BeginDate' and '$EndDate' $mysql_where GROUP BY platform_id,date_time ";
        $pgsql_info =DB::select($pgsql);
        $pgsql_info = Service::data($pgsql_info);
        //var_dump($pgsql_info);
        //mysql 处理数据求和；
        $mysql_sql ="select  sum(cost_exc) as cost_exc ,platform_id,date from zplay_tg_report_daily where date BETWEEN '$BeginDate' AND '$EndDate' GROUP BY platform_id,date";
        $mysql_info =DB::select($mysql_sql);
        $mysql_info = Service::data($mysql_info);
        //var_dump($mysql_info);
        $plat_date = [];
        $plat_date_lt = [];
        $i = 0;
        foreach ($pgsql_info as $key => $value) {
            foreach ($mysql_info as $k => $v) {
                if($value['source_id'] ==$v['platform_id'] && $value['dayid'] ==$v['date'] ){
                    if($v['cost_exc']-$value['cost_tg']>100 ||  $value['cost_usd_tg'] ==0 ){
                        $plat_date[$i]['platform_id']=$v['platform_id'];
                        $plat_date[$i]['date']=$v['date'];
                        $plat_date[$i]['diff']=$v['cost_exc']-$value['cost_tg'];
                        $i++;
                    }
                }

            }
        }
        return $plat_date;//$plat_date_lt;

    }
    public function tg_summary($begin_date,$end_date,$platform_id){
            DB::beginTransaction();
            $sel_sql = "select count(1) as count  FROM
            zplay_basic_report_daily
            WHERE
            plat_type = 'ct'
            AND date_time >= '$begin_date'  and   date_time <= '$end_date' ";
            $sel_info = DB::select($sel_sql);
            $sel_info = Service::data($sel_info);
            if($sel_info[0]['count'] !=0){
                $basic_del_sql ="   DELETE
                FROM
                zplay_basic_report_daily
                WHERE
                plat_type = 'ct'
                AND date_time >= '$begin_date'  and   date_time <= '$end_date' and platform_id ='$platform_id'" ; 
                $update_info =DB::delete($basic_del_sql);
                if(!$update_info){
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
                cost_usd_tg
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
                sum(tg.cost_usd) AS cost_usd_tg
                FROM
                zplay_tg_report_daily tg
                LEFT JOIN c_app app ON tg.app_id = app.app_id
                WHERE
                tg.date >= '$begin_date'
                AND tg.date <= '$end_date'  and platform_id ='$platform_id'
                GROUP BY
                tg.date,
                tg.app_id,
                tg.country_id,
                tg.agency_platform_id,
                tg.platform_id,
                tg.data_account,
                tg.ad_type";
            $insert_info_1 = DB::insert($basic_insert_sql);
            if(!$insert_info_1){
                DB::rollBack();
            }


//            $update_sql_1 ="UPDATE zplay_basic_report_daily usd,
//                c_currency_ex cur
//                SET usd.cost_usd_tg = usd.cost_tg / cur.currency_ex
//                WHERE
//                cur.`effective_time` = date_format(usd.date_time, '%Y%m')
//                AND cur.`currency_id` = 60
//                AND usd.cost_usd_tg = usd.cost_tg
//                AND usd.`plat_type` = 'ct' and usd.date_time >= '$begin_date'  and   usd.date_time <= '$end_date' and platform_id ='$platform_id'";
//
//                $update_info_1 = DB::UPDATE($update_sql_1);
//                if (!$update_info_1){
//                    DB::rollBack();
//                }

                DB::commit();
    }
}