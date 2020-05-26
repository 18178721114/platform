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

class ChartboostHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'ChartboostHandleProcesses {dayid?} ';

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
        $dayid = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));

        //define('MYSQL_TABLE_NAME','zplay_ad_report_daily');
        $source_id = 'pad03';
        $source_name = 'Chartboost';
        var_dump('Chartboost-pad03-'.$dayid);

        //查询pgsql 的数据
        $map =[];
        $map['dayid'] = $dayid;
        $map['type'] = 2;
        $map['source_id'] = $source_id;
        $map[] =['income','<>',0] ;
//        $map['account'] = 'contact@zplay.cn';
        $info = DataImportLogic::getChannelData('ad_data','erm_data',$map)->get();
        $info = Service::data($info);
        if(!$info){
//            $error_msg = $dayid.'号，'.$source_name.'广告平台数据处理程序获取原始数据为空';
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
            $error_msg = $dayid.'号，'.$source_name.'广告平台数据处理程序应用数据查询为空';
            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,2,$error_msg);
            exit;
        }

        //获取平台的汇率
        $effective_time = date("Ym",strtotime($dayid));
        $sql = "select a.data_account,c.`currency_ex` from c_platform_account_mapping a,c_platform b,`c_currency_ex` c
                    where a.platform_id = '{$source_id}'  
        and b.platform_id = a.platform_id and a.`customer_id` = b.`customer_id` and a.`company_id` = b.`company_id` and b.`currency_type_id` = c.`currency_id` and c.effective_time = '{$effective_time}' ";

        $ex_info = DB::select($sql);
        $ex_info = Service::data($ex_info);

        // 获取美元汇率
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
            $error_msg = $dayid.'号，'.$source_name.'广告平台数据处理程序国家信息数据查询为空';
            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,2,$error_msg);
            exit;
        }

        $array = [];
        $num = 0;
        $error_log_arr = [];
        $error_detail_arr = [];//报错的 详细数据信息
        foreach ($info as $k => $v) {
        	$json_info = json_decode($v['json_data'],true);
        	foreach ($app_list as $app_k => $app_v) {
        		if(isset($json_info['appId']) && ($json_info['appId'] == $app_v['ad_slot_id'])){
        			$array[$k]['app_id'] = $app_v['app_id'];
        			$array[$k]['ad_type'] = $app_v['ad_type'];
                    $array[$k]['flow_type'] = $app_v['flow_type'];
        			$num = 0;
        			break;
        		}else{
        			//广告位配置未配置
        			$num++;
        			
        		}
        	}

            if ($num){
                $error_log_arr['app_id'][] = $json_info['appId'].'('.addslashes(str_replace('\'\'','\'',$json_info['app'])).')';
            }

            $array[$k]['country_id'] = 16;

        	if( $num > 0){
                $error_detail_arr[$k]['platform_id'] = $source_id;
                $error_detail_arr[$k]['platform_name'] = $source_name;
                $error_detail_arr[$k]['platform_type'] =2;
                $error_detail_arr[$k]['err_date'] = $dayid;
                $error_detail_arr[$k]['first_level_id'] = '';
                $error_detail_arr[$k]['first_level_name'] = '';
                $error_detail_arr[$k]['second_level_id'] = $json_info['appId'];
                $error_detail_arr[$k]['second_level_name'] = addslashes(str_replace('\'\'','\'',$json_info['app']));;
                $error_detail_arr[$k]['money'] = $json_info['moneyEarned'];
                $error_detail_arr[$k]['account'] = $v['account'];
                $error_detail_arr[$k]['create_time'] = date('Y-m-d H:i:s');
        		unset($array[$k]);
        		//插入错误数据
        		continue;
        	}



            $array[$k]['data_account'] = $v['account'];
        	$array[$k]['date'] = $dayid;
        	$array[$k]['ad_unit_id'] = $json_info['appId'];
        	$array[$k]['ad_unit_name'] = addslashes(str_replace('\'\'','\'',$json_info['app']));

        	$array[$k]['impression'] = $json_info['impressionsDelivered'];
        	$array[$k]['click'] = $json_info['clicksDelivered'];

            $array[$k]['earning'] = isset($json_info['moneyEarned']) ? $json_info['moneyEarned'] : 0.00; // 流水原币

            if($app_list[0]['divide_ad']){
                $divide_ad =1-$app_list[0]['divide_ad']/100;
            }else{
                $divide_ad = 1;
            }
            $array[$k]['earning_exc'] = isset($json_info['moneyEarned']) ? $json_info['moneyEarned'] * $divide_ad: 0;


            // 汇率判断
            $currency_ex = 1;
            if ($ex_info){
                foreach ($ex_info as $eik => $eiv){
                    if ($eiv['data_account'] == $v['account']){
                        $currency_ex = $eiv['currency_ex'];
                    }
                }
            }
            $array[$k]['earning_flowing'] =isset($json_info['moneyEarned']) ? $json_info['moneyEarned']* $currency_ex: 0;
            $array[$k]['earning_fix'] = isset($json_info['moneyEarned']) ? $json_info['moneyEarned'] * $currency_ex * $divide_ad : 0;

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
            if (isset($error_log_arr['app_id'])){
                $appid_str = implode(',',array_unique($error_log_arr['app_id']));
                $error_msg_array[] = '应用匹配失败,ID为:'.$appid_str;
                $error_msg_mail[] = '应用匹配失败，ID为：'.$appid_str;
            }
            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,2,implode(';',$error_msg_array));
            DataImportImp::saveDataErrorMoneyLog($source_id,$dayid,$error_detail_arr);
            //CommonFunction::sendMail($error_msg_mail,$source_name.'广告平台数据处理error');
        }

        // 保存正确数据
        if ($array) {
            $plat_str =$source_id.'lishuyang@lishuyang'.$dayid;
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
                        # code...
                    
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
                        ."'',"//platform_app_id
                        ."'',"//platform_app_name
                        ."'".$v['ad_unit_id']."',"//ad_unit_id
                        ."'".$v['ad_unit_name']."',"//ad_unit_name
                        ."'',"//round
                        ."'',"//all_request
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
        }

        


    }
}