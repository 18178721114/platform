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

class MeizuHandworkHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'MeizuHandworkHandleProcesses {dayid?} ';

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
      	$channel_id= 31;
      	//查询渠道关联广告平台
        $map = []; // 查询条件
        $map['c_channel.id'] = $channel_id ;
        $fields = ['c_channel.*','c_channel_ad_platform_mapping.ad_platform_id'];

        $map['leftjoin'] = [
            ['c_channel_ad_platform_mapping','c_channel.id', 'c_channel_ad_platform_mapping.channel_id'],
        ];
        // 获取分页数据
        $channel_list = ChannelLogic::getChannelList($map, $fields)->first();
        $channel_list = Service::data($channel_list);

        $source_id =$channel_list['ad_platform_id'];
      	$platform_name =$channel_list['channel_name'];

        $dayid = date('Y-m-d');
        $date_arr = $this->argument('dayid');

        //查询pgsql 的数据
        $map =[];
        $map['in'] = ['dayid',$date_arr];
        $map['type']  =1;
        $map['source_id']  =$channel_id;
        $info = DataImportLogic::getChannelData('ad_data','erm_data',$map)->get();
        $info = Service::data($info);
        if(!$info){
//        	 $error_msg = $dayid.'号，'.$platform_name.'渠道广告数据处理程序获取原始数据为空';
//            DataImportImp::saveDataErrorLog(2,$source_id,$platform_name,2,$error_msg);
            exit;
        }

        
        //获取匹配应用的数据
                $sql = "SELECT DISTINCT
            c_ad_type_corresponding.name,
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
            $error_msg = $dayid.'号，'.$platform_name.'渠道广告数据处理程序应用数据查询为空';
            DataImportImp::saveDataErrorLog(2,$source_id,$platform_name,2,$error_msg);
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

        $array = [];
        $error_log_arr=[];
        $num = 0;
        $num_adtype =0;
        $error_detail_arr = [];//报错的 详细数据信息
        try { 
            foreach ($info as $k => $v) {
            	$json_info = json_decode($v['json_data'],true);
            	foreach ($app_list as $app_k => $app_v) {
                    
            		if($json_info['应用'] == $app_v['platform_app_name']  &&  $json_info['广告类型'] == $app_v['name'] ){
            			$array[$k]['app_id'] = $app_v['app_id'];
                        $array[$k]['ad_type'] =$app_v['ad_type'];
                        $array[$k]['flow_type'] = $app_v['flow_type'];
            			$num = 0;
            			break;
            		}else{
            			//广告位配置未配置
            			$num++;
            			
            		}
            	}
                if($num){
                    $error_log_arr['app_id'][] = $json_info['应用'].'或'.$json_info['广告类型'];
                }
                //默认中国
                $array[$k]['country_id'] = 64;
            	if(($num)>0){
                    $error_detail_arr[$k]['platform_id'] = $source_id;
                    $error_detail_arr[$k]['platform_name'] = $platform_name;
                    $error_detail_arr[$k]['platform_type'] =2;
                    $error_detail_arr[$k]['err_date'] = $v['dayid'];
                    $error_detail_arr[$k]['first_level_id'] = '';
                    $error_detail_arr[$k]['first_level_name'] = '';
                    $error_detail_arr[$k]['second_level_id'] = $json_info['应用'].'或'.$json_info['广告类型'];
                    $error_detail_arr[$k]['second_level_name'] = '';
                    $error_detail_arr[$k]['money'] = $json_info['收益'];
                    $error_detail_arr[$k]['account'] = $v['account'];
                    unset($array[$k]);
            		//插入错误数据
            		continue;
            	}
            	$array[$k]['date'] = $v['dayid'];
                $array[$k]['data_account'] = 'zplay';
                $array[$k]['platform_app_name'] = addslashes(str_replace('\'\'','\'',$json_info['应用']));
            	$array[$k]['ad_unit_name'] = addslashes(str_replace('\'\'','\'',$json_info['应用']));
            	$array[$k]['impression'] = $json_info['展示数'];
            	$array[$k]['click'] = $json_info['点击数'];
            	$array[$k]['earning'] = $json_info['收益'];
                if($app_list[0]['divide_ad']){
                    $divide_ad = 1-$app_list[0]['divide_ad']/100;
                }else{
                    $divide_ad =1;
                }
                $array[$k]['earning_exc'] = $json_info['收益']*$divide_ad;
                $array[$k]['earning_flowing'] =isset($json_info['收益']) ? $json_info['收益']: 0;
            	$array[$k]['earning_fix'] = $json_info['收益']*$divide_ad;

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

            	$array[$k]['platform_id'] = $channel_list['ad_platform_id'];
                $array[$k]['channel_id'] = $channel_list['channel_id'];
            	$array[$k]['statistics'] = '0';//0是三方2是玉米
    //        	$array[$k]['flow_type'] = '2';//(1,自有流量;2,三方流量)
            	$array[$k]['create_time'] = date('Y-m-d H:i:s');
            	$array[$k]['update_time'] = date('Y-m-d H:i:s');
            	
            }
        } catch (\Exception $e) {
            $error_msg_info = $dayid.'号,'.$platform_name.'渠道数据匹配失败：'.$e->getMessage();
            ApiResponseFactory::apiResponse([],[],'',$error_msg_info); 
        }  
        if ($error_log_arr){
            $error_msg_array = [];
            $error_msg_mail = [];
            if (isset($error_log_arr['app_id'])){
                $app_id = implode(',',array_unique($error_log_arr['app_id']));
                $error_msg_array[] = '广告位id匹配失败,ID为:'.$app_id;
                $error_msg_mail[] = '广告位id匹配失败，ID为：'.$app_id;
            }



            DataImportImp::saveDataErrorLog(2,$source_id,$platform_name,2,implode(';',$error_msg_array));
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
                        ."'".$v['channel_id']."',"//channel_id
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