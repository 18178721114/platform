<?php

namespace App\Console\Commands\FfHandleProcesses;

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
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\ApplicationLogic;
use App\BusinessLogic\CommonLogic;
use App\BusinessLogic\ChannelLogic;
use App\Common\ApiResponseFactory;

class ZhifubaoFfHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'ZhifubaoFfHandleProcesses {dayid?} ';

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
        echo '<pre>';
        set_time_limit(0);
        $source_id = 'pff05';
        $billing_name ='支付宝';
        //最终表
        $mysql_table = 'zplay_ff_report_daily';
        $dayid = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));
        //查询pgsql 的数据
        $map =[];
        $map['dayid']  =$dayid;
        $map['type']  =2;
        $map['source_id']  = $source_id;
        $map['like'][] = ["json_data->payresult",'like','1'];
        $info = DataImportLogic::getChannelData('ff_data','erm_data',$map)->get();
        $info = Service::data($info);
        var_dump(count($info));
        if(!$info){
//            $error_msg = $dayid.'号，支付宝计费平台数据处理程序获取原始数据为空';
//            echo $error_msg;
//            DataImportImp::saveDataErrorLog(2,$source_id,$billing_name,3,$error_msg);
            exit;
        }


        //获取匹配应用的数据

         $sql ="SELECT 
         c_billing.billing_app_id,
         c_billing.billing_app_name,
        `c_billing`.`app_package_name`,
        `c_app`.`app_id`,
        `c_app`.`id`
        FROM
        `c_app`
        LEFT JOIN `c_billing` ON `c_billing`.`app_id` = `c_app`.`id`
        WHERE
        (
        `c_billing`.`pay_platform_id` = '$source_id')";
        $app_list = DB::select($sql);
        $app_list = Service::data($app_list);
        if(!$app_list){
        	$error_msg = $billing_name.'应用数据查询为空';
            DataImportImp::saveDataErrorLog(2,$source_id,$billing_name,3,$error_msg);
            ApiResponseFactory::apiResponse([],[],'',$error_msg);
            exit;
        }
        //查询渠道
        $channel_sql ="SELECT * FROM c_channel ";
        $channel_info = DB::select($channel_sql);
        $channel_info = Service::data($channel_info);
        if(!$channel_info){
            $error_msg = $billing_name.'渠道数据查询为空';
            DataImportImp::saveDataErrorLog(2,$source_id,$billing_name,3,$error_msg);
            ApiResponseFactory::apiResponse([],[],'',$error_msg);
            exit;
        }
        $country_map =[];
        $country_info = CommonLogic::getCountryList($country_map)->get();
        $country_info = Service::data($country_info);
        if(!$country_info){
            $error_msg = $billing_name.'国家信息数据查询为空';
            DataImportImp::saveDataErrorLog(2,$source_id,$billing_name,3,$error_msg);
            exit;
        }
        $array = [];
        $error_log_arr=[];
        $error_detail_arr=[];
        $num = 0;
        $num_country =0;
        $num_channel = 0;
        try {
            foreach ($info as $k => $v) {
            	$json_info = json_decode($v['json_data'],true);
            	foreach ($app_list as $app_k => $app_v) {
                        if($json_info['zplay_key'].'_'.$json_info['platform'] ==$app_v['billing_app_id']  ){
                            $array[$k]['app_id'] = $app_v['app_id'];
                            $num = 0;
                            break;
                        }else{
                            $num++;

                        }

                    


            	}
                if($num){
                    $error_log_arr['app_id'][]=$json_info['zplay_key'].'_'.$json_info['platform'];
                }


                foreach ($channel_info as $channel_k => $channel_v) {
                        if($json_info['channelId'] ==$channel_v['td_channel_id']  ){
                            $array[$k]['channel_id'] = $channel_v['channel_id'];
                            $num_channel = 0;
                            break;
                        }else{
                            $num_channel++;

                        }

                    


                }
                if($num_channel){
                    $error_log_arr['channel_id'][]=$json_info['channelId'].'('.$json_info['name'].')';
                }
                $array[$k]['province_id'] =335;
                $array[$k]['country_id'] =16;
                foreach ($country_info as $country_k => $country_v) {
                    //如果 国家都为空  那么 是未知区域
                     //先匹配省份  在匹配回家
                   if($json_info['position'] ==$country_v['name']){
                       $array[$k]['province_id'] = $country_v['c_country_id'];
                       $num_country = 0;
                   }

                   if($json_info['country'] ==$country_v['name'] ){
                     $array[$k]['country_id'] = $country_v['c_country_id'];
                     $num_country = 0;
                 }
               }
               if($num_country){
                    $error_log_arr['country'][] = isset($json_info['position']) ? $json_info['position'].'/'.$json_info['country'] : 'Unknown Region' ;
                }
                
            	if(($num+$num_country+$num_channel)>0){

                    $error_detail_arr[$k]['platform_id'] = $source_id;
                    $error_detail_arr[$k]['platform_name'] = $billing_name;
                    $error_detail_arr[$k]['platform_type'] =3;
                    $error_detail_arr[$k]['err_date'] = $dayid;
                    $error_detail_arr[$k]['first_level_id'] = $json_info['zplay_key'].'_'.$json_info['platform'];
                    $error_detail_arr[$k]['first_level_name'] = '';
                    $error_detail_arr[$k]['second_level_id'] = ''; // $json_info['channelId'];
                    $error_detail_arr[$k]['second_level_name'] = $json_info['name'];
                    $error_detail_arr[$k]['money'] = $json_info['totalfee'];
                    $error_detail_arr[$k]['account'] = 'zplay';
                    $error_detail_arr[$k]['create_time'] = date('Y-m-d H:i:s');

            		unset($array[$k]);
            		//插入错误数据
            		continue;
            	}
            	$array[$k]['date'] = $dayid;
                $array[$k]['platform_account'] = '掌游';
                $array[$k]['platform_id'] = $source_id;
                $array[$k]['publisher_id'] = 5;
                
                // if($app_list[0]['divide_billing']){
                //     $divide_ad = $app_list[0]['divide_billing']/100;
                // }else{
                //     $divide_ad =1;
                // }
                // if($app_list[0]['bad_account_rate']){
                //     $bad_account_rate = $app_list[0]['bad_account_rate']/100;
                // }else{
                //     $bad_account_rate =1;
                // }
                $divide_ad =1;
                $bad_account_rate =1;
                //汇率 默认为1
                $ex_info['currency_ex'] =1;
                $array[$k]['platform_account'] = '掌游';
                $array[$k]['pay_user'] = $json_info['quantity'];
                $array[$k]['pay_time'] = $json_info['quantity'];
                $array[$k]['pay_user_all'] = $json_info['quantity'];
                $array[$k]['pay_time_all'] = $json_info['quantity'];
                $array[$k]['earning'] = $json_info['totalfee']*$ex_info['currency_ex'];
                $array[$k]['earning_fix'] =$json_info['totalfee']*$ex_info['currency_ex'];//流水人民币
            	$array[$k]['earning_divide_plat'] = $json_info['totalfee']*$ex_info['currency_ex']*$divide_ad;//流水平台分成
                $array[$k]['earning_divide_plat_pay'] = $json_info['totalfee']*$ex_info['currency_ex']*$bad_account_rate;//流水坏账
            	$array[$k]['earning_divide_publisher'] = $json_info['totalfee']*$ex_info['currency_ex'];
            	$array[$k]['income_plat'] = $json_info['totalfee']*$ex_info['currency_ex']*$divide_ad;
            	$array[$k]['income_publisher'] = $json_info['totalfee']*$ex_info['currency_ex'];
            	$array[$k]['income_fix'] =$json_info['totalfee']*$ex_info['currency_ex'];
                $array[$k]['remark'] =$json_info['id'];
            	$array[$k]['create_time'] = date('Y-m-d H:i:s');
            	$array[$k]['update_time'] = date('Y-m-d H:i:s');
            	
            }
        } catch (\Exception $e) {
            $error_msg_info = $dayid.'号,'.$billing_name.'渠道数据匹配失败：'.$e->getMessage();
            ApiResponseFactory::apiResponse([],[],'',$error_msg_info); 
        } 

                // 保存错误信息
        if ($error_log_arr){
            //var_dump($error_log_arr);die;
            $error_msg_array = [];
            if (isset($error_log_arr['app_id'])){
                $app_id = implode(',',array_unique($error_log_arr['app_id']));
                $error_msg_array[] = '应用匹配失败,ID为:'.$app_id;
            }
             if (isset($error_log_arr['channel_id'])){
                $channel_id = implode(',',array_unique($error_log_arr['channel_id']));
                $error_msg_array[] = '渠道匹配失败,ID为:'.$channel_id;
            }
             if (isset($error_log_arr['country'])){
                $country = implode(',',array_unique($error_log_arr['country']));
                $error_msg_array[] = '国家匹配失败,ID为:'.$country;
            }
            DataImportImp::saveDataErrorLog(2,$source_id,$billing_name,3,implode(';',$error_msg_array));
            DataImportImp::saveDataErrorMoneyLog($source_id,$dayid,$error_detail_arr);
//            CommonFunction::sendMail($error_msg_array,$billing_name.'渠道计费数据处理error');
        }

        if(!empty($array)){
            DB::beginTransaction();
            $map_delete['platform_id'] =$source_id;
            $map_delete['date'] = $dayid;
            DataImportLogic::deleteMysqlHistoryData($mysql_table,$map_delete);
            //拆分批次
            $step = array();
            $i = 0;
            foreach ($array as $kkkk => $insert_data_info) {
                if ($kkkk % 500 == 0) $i++;
                if ($insert_data_info) {
                    $step[$i][] = $insert_data_info;
                }
            }
            $is_success = [];
            if ($step) {
                foreach ($step as $k => $v) {
                    $result = DataImportLogic::insertAdReportInfo($mysql_table, $v);
                    if (!$result) {
                        DB::rollBack();
                        $is_success[] = $k;
                    }
                }
               
            }
            DB::commit();


            // 调用存储过程更新总表数据
            Artisan::call('FfSummaryProcesses',['begin_date'=>$dayid,'end_date'=>$dayid,'platform_id'=>$source_id]);
//            DB::update("call ff_summary('$dayid','$dayid','$source_id')");
            
            // 查询广告数据
            $report_map = [];
            $report_map['platform_id'] =$source_id;
            $report_map['date'] = $dayid;
            $group_by = ['platform_id','date','platform_account'];
            $report_list = PlatformLogic::getAdReportSum($mysql_table,$report_map)->select(DB::raw("sum(income_fix) as cost"),'platform_id','date','platform_account')->groupBy($group_by)->get();
            $report_list = Service::data($report_list);
            if ($report_list){
                // 保存广告平台
                foreach ($report_list as $value){
                	$value['platform_account'] ='null';
                    PlatformImp::add_platform_status($source_id,$value['platform_account'],$value['cost'],$value['date']);
                }
            }
            //echo '处理完成';
        }else{
            // echo '暂无处理数据';
        }

    }
}