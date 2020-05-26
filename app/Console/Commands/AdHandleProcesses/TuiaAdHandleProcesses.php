<?php

namespace App\Console\Commands\AdHandleProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessImp\PlatformImp;
use App\BusinessLogic\AdReportLogic;
use App\BusinessLogic\DataImportLogic;
use App\BusinessLogic\PlatformLogic;
use App\Common\CommonFunction;
use App\Common\CurlRequest;
use App\Common\ParseDayid;
use App\Common\Service;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\ApplicationLogic;
use App\BusinessLogic\CommonLogic;
use Illuminate\Support\Facades\Redis;

class TuiaAdHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'TuiaAdHandleProcesses {start_date?} {end_date?} ';

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
        Redis::select(0);
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        set_time_limit(0);
        $start_date = $this->argument('start_date')?$this->argument('start_date'):date('Y-m-d',strtotime('-3 day'));
        $end_date = $this->argument('end_date')?$this->argument('end_date'):date('Y-m-d',strtotime('-1 day'));
        //define('MYSQL_TABLE_NAME','zplay_ad_report_daily');
        $source_id = 'pad44';
        $source_name = '推啊';
        $account = 'weibo@zplay.com';
        var_dump('推啊-pad44-'.$start_date);

        $diff_num = Service::diffBetweenTwoDays($start_date,$end_date);
        for($di = 0;$di <=$diff_num;$di++){
            $dayid_date = date("Y-m-d",strtotime("+$di days",strtotime($start_date)));
            var_dump($dayid_date);
            self::TuiADataProcess($source_id,$source_name,$account,$dayid_date);
        }

    }


    private static function TuiADataProcess($source_id,$source_name,$account,$dayid_date){
        //查询pgsql 的数据
        $sel_sql = "select * from jwy_tuia_ad_data where dayid between '{$dayid_date}' and '{$dayid_date}' and slottype = 9 and income !=0";
        $info = DB::select($sel_sql);
        $info = Service::data($info);
        if(!$info){
//            $error_msg = $start_date.'号到'.$end_date.'号，'.$source_name.'广告平台数据处理程序获取原始数据为空';
//            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,2,$error_msg);
            exit;
        }

        //获取匹配应用的数据
        $sql = "SELECT DISTINCT
            c_app.os_id,
            c_app_ad_platform.platform_app_id,
            c_app_ad_platform.platform_app_name,
            `c_platform`.`divide_ad`,
            `c_app`.`id`,
            `c_app`.`app_id`,
            `c_app_ad_platform`.`platform_id`,
            `c_app_ad_slot`.`zone`,
            `c_app_ad_slot`.`ad_slot_id`,
            `c_app_ad_slot`.`video_placement_id`,
            `c_app_ad_slot`.`interstitial_placement_id`,
            `c_app_ad_slot`.`ad_type`,
            `c_platform`.`currency_type_id`,
            `c_app_ad_platform`.`flow_type` 
            FROM
            `c_app`
            LEFT JOIN `c_app_ad_platform` ON `c_app_ad_platform`.`app_id` = `c_app`.`id`
            LEFT JOIN `c_app_ad_slot` ON `c_app_ad_slot`.`app_ad_platform_id` = `c_app_ad_platform`.`id`

            LEFT JOIN (
            SELECT
            `c_platform`.`bad_account_rate`,c_platform.currency_type_id,c_platform.platform_id, c_divide.*
            FROM
            c_platform
            LEFT JOIN c_divide ON `c_divide`.`app_channel_id` = `c_platform`.`id`
            AND `c_divide`.`type` = 3
            WHERE c_platform.`platform_id` ='$source_id'
            ORDER BY
            c_divide.effective_date DESC LIMIT 1
            ) AS c_platform ON `c_platform`.`platform_id` = `c_app_ad_platform`.`platform_id`

            WHERE
            (
            `c_app_ad_platform`.`platform_id` = '$source_id'
        )";
        $app_list = DB::select($sql);
        $app_list = Service::data($app_list);
        if(!$app_list){
            $error_msg = $source_name.'广告平台数据处理程序应用数据查询为空';
            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,2,$error_msg);
            exit;
        }
//        //获取平台的汇率
//
//        if(empty($app_list[0]['currency'])){
//            $ex_map['currency_id'] = $app_list[0]['currency_type_id'];
//        }else{
//            $ex_map['currency_id'] = $app_list[0]['currency'];
//        }
//        $ex_fields=['currency_ex'];
//        $ex_info = CommonLogic::getCurrencyEXList($ex_map,$ex_fields)->orderby('effective_time','desc')->first();
//        $ex_info = Service::data($ex_info);
//
//        if(!$ex_info){
//        	$error_msg = '汇率数据查询为空';
//            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,2,$error_msg);
//           // exit;
//        }

        // 获取美元汇率
        $effective_time = date("Ym",strtotime($dayid));
        $usd_ex_info = DataImportImp::getPlatformExchangeRate($effective_time);
        $usd_currency_ex = 0;
        if ($usd_ex_info){
            $usd_currency_ex = $usd_ex_info['currency_ex'];
        }

        //获取对照表国家信息
        $country_map =[];
        $country_info = CommonLogic::getCountryList($country_map)->get();
        $country_info = Service::data($country_info);
        if(!$country_info){
            $error_msg = $source_name.'广告平台数据处理程序国家信息数据查询为空';
            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,2,$error_msg);
            exit;
        }

        //获取对照表广告类型
        // $AdType_map['platform_id'] ='pad05';
        // $AdType_info = CommonLogic::getAdTypeCorrespondingList($AdType_map)->get();
        // $AdType_info = Service::data($AdType_info);
        // if(!$AdType_info){
        // 	echo "广告类型数据查询失败";
        // }
        $array = [];
        $error_log_arr=[];
        $num = 0;
        $num_country = 0;
        $num_adtype =0;
        $error_detail_arr = [];//报错的 详细数据信息
        foreach ($info as $k => $v) {

            foreach ($app_list as $app_k => $app_v) {
                if($v['slotid'] ==$app_v['ad_slot_id'] ){
                    $array[$k]['app_id'] = $app_v['app_id'];
                    $array[$k]['flow_type'] = $app_v['flow_type'];
                    $array[$k]['ad_type'] = $app_v['ad_type'];
                    $num = 0;
                    break;
                }else{

                    //广告位配置未配置
                    $num++;

                }

            }
            if($num){
                $error_log_arr['slot_id'][] = $v['slotid'].'('.addslashes(str_replace('\'\'','\'',$v['slotname'])).')';
            }

            $array[$k]['country_id'] = 64;


            if(($num+$num_country+$num_adtype)>0){
                $error_detail_arr[$k]['platform_id'] = $source_id;
                $error_detail_arr[$k]['platform_name'] = $source_name;
                $error_detail_arr[$k]['platform_type'] =2;
                $error_detail_arr[$k]['err_date'] = $v['dayid'];;
                $error_detail_arr[$k]['first_level_id'] = '';
                $error_detail_arr[$k]['first_level_name'] = '';
                $error_detail_arr[$k]['second_level_id'] = addslashes($v['slotid']);
                $error_detail_arr[$k]['second_level_name'] = addslashes(str_replace('\'\'','\'',$v['slotname']));
                $error_detail_arr[$k]['money'] = $v['income'] / 10000;
                $error_detail_arr[$k]['account'] = $account;
                $error_detail_arr[$k]['create_time'] = date('Y-m-d H:i:s');
                unset($array[$k]);
                //插入错误数据
                continue;
            }


            //获取平台的汇率
            $effective_time = date("Ym",strtotime($v['dayid']));
            $sql = "select a.data_account,c.`currency_ex` from c_platform_account_mapping a,c_platform b,`c_currency_ex` c
                    where a.data_account = 'weibo@zplay.com' and a.platform_id = '{$source_id}'  
        and b.platform_id = a.platform_id and a.`customer_id` = b.`customer_id` and a.`company_id` = b.`company_id` and b.`currency_type_id` = c.`currency_id` and c.effective_time = '{$effective_time}' ";

            $ex_info = DB::select($sql);
            $ex_info = Service::data($ex_info);


            $array[$k]['date'] = $v['dayid'];
            $array[$k]['data_account'] = $account;
            $array[$k]['platform_app_id'] = '';
            $array[$k]['platform_app_name'] = '' ;
            $array[$k]['ad_unit_id'] = addslashes($v['slotid']);
            $array[$k]['ad_unit_name'] = addslashes($v['slotname']);
            $array[$k]['impression'] = $v['exposurecount'];
            $array[$k]['all_request'] = 0;
            $array[$k]['click'] = $v['clickcount'];
            $array[$k]['earning'] = $v['income'] / 10000;
            if($app_list[0]['divide_ad']){
                $divide_ad = 1-$app_list[0]['divide_ad']/100;
            }else{
                $divide_ad =1;
            }
            $currency_ex = isset($ex_info[0]) ? floatval($ex_info[0]['currency_ex']) : 1;
            if (!$currency_ex){
                $currency_ex = 1;
            }
            $array[$k]['earning_exc'] = $v['income']*$divide_ad / 10000;
            $array[$k]['earning_flowing'] =isset($v['income']) ? $v['income']/ 10000* $currency_ex: 0;
            $array[$k]['earning_fix'] = $v['income']*$currency_ex*$divide_ad / 10000;

            // 流水美元
            if (($array[$k]['earning'] == $array[$k]['earning_flowing']) && $usd_currency_ex){
                $array[$k]['earning_usd'] = $array[$k]['earning_flowing'] / $usd_currency_ex;
            }else{
                $array[$k]['earning_usd'] = $array[$k]['earning'];
            }

            // 收入美元
            if (($array[$k]['earning_exc'] == $array[$k]['earning_fix']) && $usd_currency_ex){
                $array[$k]['earning_exc_usd'] = $array[$k]['earning_fix'] / $usd_currency_ex;
            }else{
                $array[$k]['earning_exc_usd'] = $array[$k]['earning_exc'];
            }

            $array[$k]['platform_id'] = $source_id;
            $array[$k]['statistics'] = '0';//0是三方2是玉米
//        	$array[$k]['flow_type'] = '2';//(1,自有流量;2,三方流量)
            $array[$k]['create_time'] = date('Y-m-d H:i:s');
            $array[$k]['update_time'] = date('Y-m-d H:i:s');

        }
        // 保存错误信息
        if ($error_log_arr){
            $error_msg_array = [];
            $error_msg_mail = [];
            if (isset($error_log_arr['slot_id'])){
                $slot_id = implode(',',array_unique($error_log_arr['slot_id']));
                $error_msg_array[] = '广告位id匹配失败,ID为:'.$slot_id;
                $error_msg_mail[] = '广告位id匹配失败，ID为：'.$slot_id;
            }

            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,2,implode(';',$error_msg_array));
            DataImportImp::saveDataErrorMoneyLog($source_id,$dayid_date,$error_detail_arr);

            //CommonFunction::sendMail($error_msg_mail,$source_name.'广告平台数据处理error');
        }
        if(!empty($array)){
            $plat_str =$source_id.'lishuyang@lishuyang'.$dayid_date;
            Redis::rpush(env('REDIS_AD_KEYS'), $plat_str);
            //拆分批次
            $step = array();
            $i = 0;
            foreach ($array as $kkkk => $insert_data_info) {
                if ($kkkk % 500 == 0) $i++;
                if ($insert_data_info) {
                    $step[$i][] = $insert_data_info;
                }
            }

            $time = date('Y-m-d H:i:s');

            if ($step) {
                foreach ($step as $k => $v_info) {
                    $sql_str ='';
                    foreach ($v_info as $k_sql => $v) {

                        $sql_str.= "('".$v['date']."'," // date
                            ."'".$v['app_id']."',"  //app_id
                            ."'',"// version
                            ."'',"//channel_id
                            ."'".$v['country_id']."',"//country_id
                            ."'',"//data_platform_id
                            ."'".$v['data_account']."',"//data_account
                            ."'".$v['platform_id']."',"//platform_id
                            ."'".$v['ad_type']."',"//ad_type
                            ."'".$v['statistics']."',"//statistics
                            ."'".$v['platform_app_id']."',"//platform_app_id
                            ."'".$v['platform_app_name']."',"//platform_app_name
                            ."'".$v['ad_unit_id']."',"//ad_unit_id
                            ."'".$v['ad_unit_name']."',"//ad_unit_name
                            ."'',"//round
                            ."'".$v['all_request']."',"//all_request
                            ."'',"//success_requests
                            ."'',"//fail_requests;
                            ."'',"//impression_port;
                            ."'',"//impression_begin;
                            ."'".$v['impression']."',"//impression;
                            ."'".$v['click']."',"//click;
                            ."'',"//download;
                            ."'',"//activate;
                            ."'',"//reward;
                            ."'".$v['earning']."',"//earning;
                            ."'".$v['earning_exc']."',"//earning_exc;
                            ."'".$v['earning_flowing']."',"//earning_flowing;
                            ."'".$v['earning_fix']."',"//earning_fix;
                            ."'".$v['flow_type']."',"//flow_type;
                            ."'',"//remark;
                            ."'".$time."',"//create_time
                            ."'".$time."',"//update_time
                            ."'".$v['earning_usd']."',"//earning_usd
                            ."'".$v['earning_exc_usd']."'),";//earning_exc_usd

                    }
                    $sql_str = rtrim($sql_str,',');
                    Redis::rpush(env('REDIS_AD_KEYS'), $sql_str);
                }
            }
            //echo '处理完成';

        }else{
            //echo '暂无处理数据';
        }
    }
}