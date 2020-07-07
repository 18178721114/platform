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
use App\BusinessLogic\ChannelLogic;
use App\Common\ApiResponseFactory;
use Illuminate\Support\Facades\Redis;

class WeixinAdiHandworkHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'WeixinAdiHandworkHandleProcesses {dayid?} ';

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
        $source_id = 'pad59';
        $source_name = '微信广告';
        $dayid = date('Y-m-d');
        $date_arr = $this->argument('dayid');
        //查询pgsql 的数据
        $map =[];
        $map['in'] = ['dayid',$date_arr];
        $map['type']  =1;
        $map['source_id']  =$source_id;
        $info = DataImportLogic::getChannelData('ad_data','erm_data',$map)->get();
        $info = Service::data($info);
        if(!$info){
//        	 $error_msg = $dayid.'号，'.$platform_name.'渠道广告数据处理程序获取原始数据为空';
//            DataImportImp::saveDataErrorLog(2,$source_id,$platform_name,2,$error_msg);
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
            LEFT JOIN `c_ad_type_corresponding` ON `c_ad_type_corresponding`.`ad_type_id` = `c_app_ad_slot`.`ad_type`
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
            $error_msg = $source_name.'数据处理程序应用数据查询为空';
            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,2,$error_msg);
            ApiResponseFactory::apiResponse([],[],'',$error_msg);
            exit;
        }

        // 获取美元汇率
        $usd_currency_date = [];
        $usd_currency_arr = [];
        if ($date_arr){
            foreach ($date_arr as $date_time){
                $usd_currency_date[] = date("Ym",strtotime($date_time));
            }
            $usd_currency_date = array_unique($usd_currency_date);
            $usd_ex_info = DataImportImp::getPlatformExchangeRate($usd_currency_date);
            if ($usd_ex_info){
                foreach ($usd_ex_info as $usd_ex){
                    $usd_currency_arr[$usd_ex['effective_time']] =$usd_ex['currency_ex'];
                }
            }
        }

         $AdType_map['platform_id'] =$source_id;
         $AdType_info = CommonLogic::getAdTypeCorrespondingList($AdType_map)->get();
         $AdType_info = Service::data($AdType_info);
         if(!$AdType_info){
             $error_msg = $source_name.'数据处理程序广告类型查询为空';
             DataImportImp::saveDataErrorLog(2,$source_id,$source_name,2,$error_msg);
             ApiResponseFactory::apiResponse([],[],'',$error_msg);
             exit;
         }

        $array = [];
        $error_log_arr=[];
        $num = 0;
        $num_adtype =0;
        $error_detail_arr = [];//报错的 详细数据信息
        try {
            foreach ($info as $k => $v) {
            	$json_info = json_decode($v['json_data'],true);
            	foreach ($app_list as $app_k => $app_v) {
                    
            		if($json_info['应用名称'] == $app_v['platform_app_name'] ){
            			$array[$k]['app_id'] = $app_v['app_id'];
                        $array[$k]['flow_type'] = $app_v['flow_type'];
            			$num = 0;
            			break;
            		}else{
            			//广告位配置未配置
            			$num++;
            			
            		}
            	}
                $err_name = (isset($json_info['广告位ID']) ?$json_info['广告位ID']:'Null').'#'.(isset($json_info['广告位']) ?$json_info['广告位']:'Null').'#'.(isset($json_info['应用ID']) ?$json_info['应用ID']:'Null').'#'.(isset($json_info['应用名称']) ?$json_info['应用名称']:'Null');

                if($num){
                    $error_log_arr['app_id'][] = $json_info['应用名称'].'('.$err_name.')';
                }
                 foreach ($AdType_info as $AdType_k => $AdType_v) {
                 	if($json_info['形式'] == $AdType_v['name'] ){
                 		$array[$k]['ad_type'] = $AdType_v['ad_type_id'];
                 		$num_adtype = 0;
                 		break;
                 	}else{
                 		//广告类型失败
                 		$num_adtype++;

                 	}
                 }
                if($num_adtype){
                    $error_log_arr['ad_type'][] = $json_info['形式'];
                }
                //默认中国
                $array[$k]['country_id'] = 64;
            	if(($num+$num_adtype)>0){
                    $error_detail_arr[$k]['platform_id'] = $source_id;
                    $error_detail_arr[$k]['platform_name'] = $source_name;
                    $error_detail_arr[$k]['platform_type'] =2;
                    $error_detail_arr[$k]['err_date'] = $v['dayid'];
                    $error_detail_arr[$k]['first_level_id'] = '';
                    $error_detail_arr[$k]['first_level_name'] = addslashes(str_replace('\'\'','\'',$json_info['应用名称']));
                    $error_detail_arr[$k]['second_level_id'] = '';
                    $error_detail_arr[$k]['second_level_name'] = '';
                    $error_detail_arr[$k]['money'] = $json_info['收入(元)'];
                    $error_detail_arr[$k]['account'] = 'zplay';
                    $error_detail_arr[$k]['create_time'] = date('Y-m-d H:i:s');
            		unset($array[$k]);
            		//插入错误数据
            		continue;
            	}
            	$array[$k]['date'] = $v['dayid'];
                $array[$k]['data_account'] = 'zplay';
            	$array[$k]['ad_unit_id'] = '';
            	$array[$k]['ad_unit_name'] = '';
                $array[$k]['platform_app_id'] = '';
                $array[$k]['platform_app_name'] = addslashes(str_replace('\'\'','\'',$json_info['应用名称']));
            	$array[$k]['impression'] = $json_info['曝光量'];
            	$array[$k]['click'] = $json_info['点击量'];
            	$array[$k]['earning'] = $json_info['收入(元)'];
                if($app_list[0]['divide_ad']){
                    $divide_ad = 1-$app_list[0]['divide_ad']/100;
                }else{
                    $divide_ad =1;
                }
                $array[$k]['earning_exc'] = $json_info['收入(元)']*$divide_ad;
                $array[$k]['earning_flowing'] =isset($json_info['收入(元)']) ? $json_info['收入(元)']: 0;
            	$array[$k]['earning_fix'] = $json_info['收入(元)']*$divide_ad;

                $usd_currency_ex = 0;
                if ($usd_currency_arr){
                    foreach ($usd_currency_arr as $usd_currency_date => $usd_currency){
                        if ($usd_currency_date == date("Ym",strtotime($v['dayid']))){
                            $usd_currency_ex = $usd_currency;
                        }
                    }
                }

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

            	$array[$k]['platform_id'] =$source_id ;
            	$array[$k]['statistics'] = '0';//0是三方2是玉米
    //        	$array[$k]['flow_type'] = '2';//(1,自有流量;2,三方流量)
            	$array[$k]['create_time'] = date('Y-m-d H:i:s');
            	$array[$k]['update_time'] = date('Y-m-d H:i:s');
            	
            }
        } catch (\Exception $e) {
            $error_msg_info = $dayid.'号,'.$source_name.'渠道数据匹配失败：'.$e->getMessage();
            ApiResponseFactory::apiResponse([],[],'',$error_msg_info); 
        }
        if ($error_log_arr){
            $error_msg_array = [];
            $error_msg_mail = [];
            if (isset($error_log_arr['app_id'])){
                $app_id = implode(',',array_unique($error_log_arr['app_id']));
                $error_msg_array[] = '应用名称匹配失败,ID为:'.$app_id;
                $error_msg_mail[] = '应用名称d匹配失败，ID为：'.$app_id;
            }
            if (isset($error_log_arr['ad_type'])){
                $app_id = implode(',',array_unique($error_log_arr['ad_type']));
                $error_msg_array[] = '广告类型匹配失败,ID为:'.$app_id;
                $error_msg_mail[] = '广告类型匹配失败，ID为：'.$app_id;
            }



            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,2,implode(';',$error_msg_array));
            foreach ($date_arr as $key => $value) {
                $array_err =[];
                foreach ($error_detail_arr as $k => $v) {
                    if($v['err_date'] ==$value){
                        $array_err[$k] = $v;
                    }
                }
                DataImportImp::saveDataErrorMoneyLog($source_id,$value,$array_err);
            }

            //CommonFunction::sendMail($error_msg_mail,$platform_name.'渠道广告数据处理error');
        }
        $time = date('Y-m-d H:i:s');
        if(!empty($array)){
            foreach ($date_arr as $key => $value) {
                $sql_str ='';
                $plat_str ='';
                foreach ($array as $k => $v) {
                    if($v['date'] ==$value ){
                        $plat_str =$source_id.'lishuyang@lishuyang'.$value;
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
                    
                }
                if($sql_str!='' && $plat_str!=''){
                    Redis::rpush(env('REDIS_AD_KEYS'), $plat_str);
                    $sql_str = rtrim($sql_str,',');
                    Redis::rpush(env('REDIS_AD_KEYS'), $sql_str);
                }
            }
            //echo '处理完成';
        }else{
            // echo '暂无处理数据';
        }


    }
}