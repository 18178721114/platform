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

class ApplovinHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'ApplovinHandleProcesses {dayid?} ';

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
        //查询pgsql 的数据
        //define('MYSQL_TABLE_NAME','zplay_ad_report_daily');
        $source_id = 'pad33';
        $source_name = 'Applovin';
        var_dump('Applovin-pad33-'.$dayid);

        $map =[];
        $map['dayid'] = $dayid;
        $map['type'] = 2;
        $map['source_id'] = $source_id;
        $map[] =['income','<>',0] ;
        //$map['like'][] = ["json_data->bidding_integration",'like','None'];
        $info = DataImportLogic::getChannelData('ad_data','erm_data',$map)->get();
        $info = Service::data($info);
        
        if(!$info){
            //echo 112;
//            $error_msg = $dayid.'号，'.$source_name.'广告平台数据处理程序获取原始数据为空';
//            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,2,$error_msg);
            exit;
        }

        //获取匹配应用的数据
        $sql = "SELECT 
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
            LEFT JOIN `c_app_ad_platform` ON `c_app_ad_platform`.`app_id` = `c_app`.`id`    and `c_app_ad_platform`.`status` = 1
            LEFT JOIN `c_app_ad_slot` ON `c_app_ad_slot`.`app_ad_platform_id` = `c_app_ad_platform`.`id`   and `c_app_ad_slot`.`status` = 1

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
                        //获取对照表广告类型
        $AdType_map['platform_id'] =$source_id;
        $AdType_info = CommonLogic::getAdTypeCorrespondingList($AdType_map)->get();
        $AdType_info = Service::data($AdType_info);
        if(!$AdType_info){
            echo "广告类型数据查询失败";
            $error_msg = $dayid.'号，'.$source_name.'广告平台数据处理程序广告类型数据查询为空';
            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,2,$error_msg);
            exit;
        }

        $array = [];
        $num = 0;
        $num_country = 0;
        $num_adtype =0;
        $error_log_arr = [];
        $error_detail_arr = [];//报错的 详细数据信息
        foreach ($info as $k => $v) {

        	$json_info = json_decode($v['json_data'],true);
        	if(isset($json_info['bidding_integration']) && $json_info['bidding_integration'] !='None' ){
                    continue ;
            }
        	foreach ($app_list as $app_k => $app_v) {
                if($app_v['os_id'] ==1){
                    $os = 'ios';
                }elseif ($app_v['os_id'] ==2) {
                    $os = 'android';
                }elseif ($app_v['os_id'] ==3) {
                    $os = 'h5';
                }else{
                    $os = 'amazon';
                }

                if(($json_info['platform'].'-'.str_replace('\'\'','\'',$json_info['package_name']) == $app_v['platform_app_name']) || ($json_info['platform'].'-'.str_replace('\'\'','\'',$json_info['package_name']) == $os.'-'.$app_v['platform_app_name']) || ($json_info['platform'].'-'.str_replace('\'\'','\'',$json_info['application']) == $app_v['platform_app_name']) || ($json_info['platform'].'-'.str_replace('\'\'','\'',$json_info['application']) == $os.'-'.$app_v['platform_app_name'])){
                    $array[$k]['app_id'] = $app_v['app_id'];
                    $array[$k]['flow_type'] = $app_v['flow_type'];
                    $num = 0;
                    break;
                }else{
                    //广告位配置未配置
                    $num++;

                }

        	}
            $err_name = 'Null#'.(isset($json_info['application']) ?$json_info['application']:'Null').'#Null#'.(isset($json_info['package_name']) ?$json_info['package_name']:'Null');



            if ($num){
                $error_log_arr['application'][] = $json_info['platform'].'-'.str_replace('\'\'','\'',$json_info['package_name']).'('.$err_name.')';
            }

        	foreach ($country_info as $country_k => $country_v) {
        		if(isset($json_info['country']) && strtoupper($json_info['country']) == $country_v['name'] ){
        			$array[$k]['country_id'] = $country_v['c_country_id'];
        			$num_country = 0;
        			break;
        		}else{
        			//国家配置失败
        			$num_country++;
        			
        		}
        	}

            if ($num_country){
                $error_log_arr['country'][] = isset($json_info['country']) ? $json_info['country'].'('.$err_name.')' : 'Unknown Region';
            }

            foreach ($AdType_info as $AdType_k => $AdType_v) {
                if($json_info['size'].'-'.$json_info['ad_type'] == $AdType_v['name'] ){
                    $array[$k]['ad_type'] = $AdType_v['ad_type_id'];
                    $num_adtype = 0;
                    break;
                }else{
                    //广告类型失败
                    $num_adtype++;
                    
                }
            }
            if ($num_adtype){
                $error_log_arr['ad_type'][] = isset($json_info['size']) ? $json_info['size'].'('.$err_name.')' : '' ;
            }

        	
        	if(($num+$num_country+$num_adtype)>0){
                $error_detail_arr[$k]['platform_id'] = $source_id;
                $error_detail_arr[$k]['platform_name'] = $source_name;
                $error_detail_arr[$k]['platform_type'] =2;
                $error_detail_arr[$k]['err_date'] = $dayid;
                $error_detail_arr[$k]['first_level_id'] = isset($json_info['package_name']) ? $json_info['package_name'] : '';
                $error_detail_arr[$k]['first_level_name'] = '';
                $error_detail_arr[$k]['second_level_id'] = isset($json_info['application']) ? $json_info['application'] : '';
                $error_detail_arr[$k]['second_level_name'] = '';
                $error_detail_arr[$k]['money'] = isset($json_info['revenue']) ? $json_info['revenue'] : 0.00; // 流水原币
                $error_detail_arr[$k]['account'] = $v['account'];
                $error_detail_arr[$k]['create_time'] = date('Y-m-d H:i:s');

        		unset($array[$k]);
        		//插入错误数据
        		continue;
        	}


            $array[$k]['data_account'] = $v['account'];
        	$array[$k]['date'] = $dayid;
        	$array[$k]['platform_app_id'] = isset($json_info['package_name']) ? addslashes(str_replace('\'\'','\'',$json_info['package_name'])) : '';
        	$array[$k]['platform_app_name'] = isset($json_info['package_name']) ? addslashes(str_replace('\'\'','\'',$json_info['package_name'])) : '';
        	$array[$k]['ad_unit_id'] = isset($json_info['application']) ? addslashes(str_replace('\'\'','\'',$json_info['application'])) : '';
        	$array[$k]['ad_unit_name'] = isset($json_info['application']) ? addslashes(str_replace('\'\'','\'',$json_info['application'])) : '';
        	$array[$k]['impression'] = $json_info['impressions'];
        	$array[$k]['click'] = $json_info['clicks'];

            $array[$k]['earning'] = isset($json_info['revenue']) ? $json_info['revenue'] : 0.00; // 流水原币

             if($app_list[0]['divide_ad']){
                $divide_ad =1-$app_list[0]['divide_ad']/100;
            }else{
                $divide_ad = 1;
            }
            $array[$k]['earning_exc'] = isset($json_info['revenue']) ? $json_info['revenue'] * $divide_ad: 0;

            // 汇率判断
            $currency_ex = 1;
            if ($ex_info){
                foreach ($ex_info as $eik => $eiv){
                    if ($eiv['data_account'] == $v['account']){
                        $currency_ex = $eiv['currency_ex'];
                    }
                }
            }
            $array[$k]['earning_flowing'] =isset($json_info['revenue']) ? $json_info['revenue']* $currency_ex: 0;
            $array[$k]['earning_fix'] = isset($json_info['revenue']) ? $json_info['revenue'] * $currency_ex * $divide_ad : 0;

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
            $error_log_arr = Service::shield_error($source_id,$error_log_arr);

            if (isset($error_log_arr['application']) && !empty($error_log_arr['application'])){
                $application = implode(',',array_unique($error_log_arr['application']));
                $error_msg_array[] = '应用名称匹配失败,ID为:'.$application;
                $error_msg_mail[] = '应用名称匹配失败，ID为：'.$application;
            }

            if (isset($error_log_arr['country']) && !empty($error_log_arr['country'])){
                $country = implode(',',array_unique($error_log_arr['country']));
                $error_msg_array[] = '国家匹配失败,code为:'.$country;
                $error_msg_mail[] = '国家匹配失败，code为：'.$country;
            }
            if (isset($error_log_arr['ad_type']) && !empty($error_log_arr['ad_type'])){
                $ad_type = implode(',',array_unique($error_log_arr['ad_type']));
                $error_msg_array[] = '广告类型匹配失败,code为:'.$ad_type;
                $error_msg_mail[] = '广告类型匹配失败，code为：'.$ad_type;
            }
            if(!empty($error_msg_array)) {
                DataImportImp::saveDataErrorLog(2, $source_id, $source_name, 2, implode(';', $error_msg_array));
            }
            DataImportImp::saveDataErrorMoneyLog($source_id, $dayid, $error_detail_arr);


            // 发送邮件
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
                        ."'".$v['platform_app_name']."',"//platform_app_name
                        ."'',"//ad_unit_id
                        ."'',"//ad_unit_name
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
            // echo '处理完成';
        }

        


    }
}