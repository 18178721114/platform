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
use function PHPSTORM_META\type;
use Illuminate\Support\Facades\Redis;

class GuangdiantongHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'GuangdiantongHandleProcesses {dayid?} ';

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
        $source_id = 'pad10';
        $source_name = '广点通';
        var_dump('广点通-pad10-'.$dayid);
        try{
            self::GuangdiantongAdDataProcess($source_id, $source_name, $dayid);
        }catch (\Exception $e) {
            $error_msg_info = $dayid.'号,'.$source_name.'广告平台处理程序失败，失败原因：'.$e->getMessage();
            DataImportImp::saveDataErrorLog(5,$source_id,$source_name,2,$error_msg_info);
        }


    }
    public static function GuangdiantongAdDataProcess($source_id,$source_name,$dayid){
        static $stactic_num = 0;
        //查询pgsql 的数据
        $map =[];
        $map['dayid'] = $dayid;
        $map['type'] = 2;
        $map['source_id'] = $source_id;
        $map[] =['income','<>',0] ;
        $map[] =['app_id','<>',0] ;
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
            LEFT JOIN `c_app_ad_platform` ON `c_app_ad_platform`.`app_id` = `c_app`.`id` and `c_app_ad_platform`.`status` = 1
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
            $error_msg =  $dayid.'号，'.$source_name.'广告平台数据处理程序应用数据查询为空';
            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,2,$error_msg);
            exit;
        }

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
            $error_msg =  $dayid.'号，'.$source_name.'广告平台数据处理程序国家信息数据查询为空';
            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,2,$error_msg);
            exit;
        }
        //获取对照表广告类型
        $AdType_map['platform_id'] =$source_id;
        $AdType_info = CommonLogic::getAdTypeCorrespondingList($AdType_map)->get();
        $AdType_info = Service::data($AdType_info);
        if(!$AdType_info){
            $error_msg = $dayid.'号，百度广告平台数据处理程序广告类型数据查询失败';
            DataImportImp::saveDataErrorLog(2,$source_id,'Baidu',2,$error_msg);
            exit;
        }

        $array = [];
        $num = 0;
        $num_country = 0;
        $num_adtype=0;
        $error_log_arr = [];
        $error_detail_arr = [];//报错的 详细数据信息
        $new_campaign_ids = [];

        foreach ($info as $k => $v) {

        	$json_info = json_decode($v['json_data'],true);
        	foreach ($app_list as $app_k => $app_v) {
        		if($json_info['PlacementId'] == $app_v['ad_slot_id'] && $json_info['AppId'] == $app_v['platform_app_id'] ){
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
            $err_name = (isset($json_info['PlacementId']) ?$json_info['PlacementId']:'Null').'#'.(isset($json_info['PlacementName']) ?$json_info['PlacementName']:'Null').'#'.(isset($json_info['AppId']) ?$json_info['AppId']:'Null').'#Null';

            if ($num){
                if($json_info['AppId'] && $json_info['PlacementId']){
                    $app_info_sql = "select ad.`id`,ad.`platform_id`,ad.`platform_app_id`,ca.`os_id`,ca.`app_name`
                                     from c_app_ad_platform ad
                                     left join c_app ca on ad.app_id = ca.id where ad.`platform_id` = '{$source_id}' and ad.status = 1
                                     and ad.`platform_app_id` = '{$json_info['AppId']}' limit 1";
                    $app_info_detail = DB::select($app_info_sql);
                    $app_info_detail = Service::data($app_info_detail);
                    if (isset($app_info_detail[0]) && $app_info_detail[0]){
                        $ad_type = '';
                        // 匹配广告类型
                        foreach ($AdType_info as $AdType_k => $AdType_v) {
                            if($json_info['PlacementType'] == $AdType_v['name'] ){
                                $ad_type = $AdType_v['ad_type_id'];
                                $num_adtype=0;

                                break;
                            }else{
                                $num_adtype++;
                            }
                        }
                        if ($num_adtype){
                            $error_log_arr['ad_type'][] = isset($json_info['PlacementType']) ? $json_info['PlacementType'].'('.$err_name.')' : '' ;
                        }

                        if ($ad_type || $ad_type == 0){
                            $new_campaign_ids[$json_info['AppId']][$app_info_detail[0]['id']][$json_info['PlacementId']] = $ad_type;
                        }
                    }

                }
                $error_log_arr['PlacementId'][] = $json_info['PlacementId'].'('.addslashes(str_replace('\'\'','\'',$err_name)).')';
            }

            $array[$k]['country_id'] = 64;

        	
        	if($num>0){
                $error_detail_arr[$k]['platform_id'] = $source_id;
                $error_detail_arr[$k]['platform_name'] = $source_name;
                $error_detail_arr[$k]['platform_type'] =2;
                $error_detail_arr[$k]['err_date'] = $dayid;
                $error_detail_arr[$k]['first_level_id'] = isset($json_info['AppId']) ? addslashes($json_info['AppId']) : '';
                $error_detail_arr[$k]['first_level_name'] = '';
                $error_detail_arr[$k]['second_level_id'] = isset($json_info['PlacementId']) ? addslashes($json_info['PlacementId']) : '';
                $error_detail_arr[$k]['second_level_name'] = isset($json_info['PlacementName']) ? addslashes(str_replace('\'\'','\'',$json_info['PlacementName'])) : '';
                $error_detail_arr[$k]['money'] = isset($json_info['Revenue']) ? floatval(str_replace(',','',strval($json_info['Revenue']))) : 0.00; // 流水原币
                $error_detail_arr[$k]['account'] = $v['account'];
                $error_detail_arr[$k]['create_time'] = date('Y-m-d H:i:s');
        		unset($array[$k]);
        		//插入错误数据
        		continue;
        	}

        	// 格式化数据
            $array[$k]['data_account'] = $v['account'];
        	$array[$k]['date'] = $dayid;
        	$array[$k]['platform_app_id'] = isset($json_info['AppId']) ? addslashes($json_info['AppId']) : '';
            $array[$k]['platform_app_name'] = '';
            $array[$k]['ad_unit_id'] = isset($json_info['PlacementId']) ? addslashes($json_info['PlacementId']) : '';
            $array[$k]['ad_unit_name'] = isset($json_info['PlacementName']) ? addslashes(str_replace('\'\'','\'',$json_info['PlacementName'])) : '';
            $array[$k]['all_request'] = intval(str_replace(',','',strval($json_info['RequestCount'])));
        	$array[$k]['impression'] = intval(str_replace(',','',strval($json_info['Pv'])));
        	$array[$k]['click'] = intval(str_replace(',','',strval($json_info['Click'])));

            $array[$k]['earning'] = isset($json_info['Revenue']) ? floatval(str_replace(',','',strval($json_info['Revenue']))) : 0.00; // 流水原币

            if($app_list[0]['divide_ad']){
                $divide_ad =1-$app_list[0]['divide_ad']/100;
            }else{
                $divide_ad = 1;
            }

            $array[$k]['earning_exc'] = isset($json_info['Revenue']) ? floatval(str_replace(',','',strval($json_info['Revenue']))) * $divide_ad: 0;
            $array[$k]['earning_flowing'] =isset($json_info['Revenue']) ? floatval(str_replace(',','',strval($json_info['Revenue']))): 0;
            $array[$k]['earning_fix'] = isset($json_info['Revenue']) ? floatval(str_replace(',','',strval($json_info['Revenue']))) * $divide_ad : 0;

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
        if ($new_campaign_ids) {
            $insert_generalize_ad_app = [];
            foreach ($new_campaign_ids as $package_name => $offer_id) {
                if ($offer_id) {
                    foreach ($offer_id as $offer_key => $offer_id_nums) {
                        foreach ($offer_id_nums as $offer_id_nums_key => $offer_id_nums_value) {
                            $insert_generalize_ad_info = [];
                            $insert_generalize_ad_info['app_ad_platform_id'] = $offer_key;
                            $insert_generalize_ad_info['ad_slot_id'] = strval($offer_id_nums_key);
                            $insert_generalize_ad_info['ad_type'] = $offer_id_nums_value;
                            $insert_generalize_ad_info['status'] = 1;
                            $insert_generalize_ad_info['create_time'] = date("Y-m-d H:i:s", time());
                            $insert_generalize_ad_info['update_time'] = date("Y-m-d H:i:s", time());
                            $insert_generalize_ad_app[] = $insert_generalize_ad_info;
                        }
                    }
                }
            }

            if ($insert_generalize_ad_app) {
                if($stactic_num ==1){
                    //反更新没成功 走这里
                }else {
                    // 开启事物 保存数据
                    DB::beginTransaction();
                    $app_info = DB::table('c_app_ad_slot')->insert($insert_generalize_ad_app);
//                var_dump($app_info);
                    if (!$app_info) { // 应用信息已经重复
                        DB::rollBack();
                    } else {
                        DB::commit();
                        $stactic_num++;
                        self::GuangdiantongAdDataProcess($source_id, $source_name, $dayid);
                        exit;
                    }
                }
            }
        }

        // 保存错误信息
        if ($error_log_arr){
            $error_msg_array = [];
            $error_msg_mail = [];
            $error_log_arr = Service::shield_error($source_id,$error_log_arr);

            if (isset($error_log_arr['PlacementId']) && !empty($error_log_arr['PlacementId'])){
                $app_id_str = implode(',',array_unique($error_log_arr['PlacementId']));
                $error_msg_array[] = '广告位匹配失败,ID为:'.$app_id_str;
                $error_msg_mail[] = '广告位匹配失败，ID为：'.$app_id_str;
            }
            if (isset($error_log_arr['ad_type']) && !empty($error_log_arr['ad_type'])){
                $ad_type = implode(',',array_unique($error_log_arr['ad_type']));
                $error_msg_array[] = '广告类型匹配失败,code为:'.$ad_type;
                $error_msg_mail[] = '广告类型匹配失败，code为：'.$ad_type;
            }
            if(!empty($error_msg_array)) {

                DataImportImp::saveDataErrorLog(2, $source_id, $source_name, 2, implode(';', $error_msg_array));
            }
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
        }

    }
}