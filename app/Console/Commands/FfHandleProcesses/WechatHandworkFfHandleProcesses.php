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

class WechatHandworkFfHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'WechatHandworkFfHandleProcesses {dayid?} {channel_id?} ';

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
        $dayid = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));
        $channel_id = $this->argument('channel_id');
        set_time_limit(0);
        $source_id = 'pff04';
        $billing_name ='微信';
        //最终表
        $mysql_table = 'zplay_ff_report_daily';
        $date_arr =[];
        if(gettype($dayid) == 'array'){
            $date = implode("','",$dayid);
            $date_arr = $dayid;
        }else{
            $date = $dayid;
            $date_arr[] = $dayid;
        }
        $channel_id =426;
        //查询pgsql 的数据
        $map =[];
        $map['in'] = ['dayid',$date_arr];
        $map['type']  =1;
        $map['source_id']  = $channel_id;
        $handinfo = DataImportLogic::getChannelData('ff_data','erm_data',$map)->get();
        $handinfo = Service::data($handinfo);
        if(!$handinfo){
//            $error_msg = $date.'号，'.$billing_name.'数据处理程序获取原始数据为空';
//            echo $error_msg;
//            DataImportImp::saveDataErrorLog(2,$source_id,$billing_name,3,$error_msg);
            exit;
        }


        //获取匹配应用的数据
        /*$sql = "  SELECT app_name,app_id as gameid,id as app_id,  app_id as new_app_id from c_app WHERE id not in (select distinct app_id from application)
        UNION all
        select distinct app_name,gameid,app_id,new_app_id from application ";*/

        $sql = " SELECT DISTINCT
         c_billing.billing_app_id,
         c_billing.billing_app_name,
        `c_platform`.`divide_billing`,
        `c_billing`.`app_package_name`,
        `c_app`.`app_id`,
        `c_app`.`id`,
        `c_platform`.`currency_type_id`,
        `c_platform`.`bad_account_rate`
        FROM
        `c_app`
        LEFT JOIN `c_billing` ON `c_billing`.`app_id` = `c_app`.`id`
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
        ) AS c_platform ON `c_platform`.`platform_id` = `c_billing`.`pay_platform_id`
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
        // $channel_sql ="SELECT * FROM c_channel ";
        // $channel_info = DB::select($channel_sql);
        // $channel_info = Service::data($channel_info);
        // if(!$channel_info){
        //     $error_msg = $billing_name.'渠道数据查询为空';
        //     DataImportImp::saveDataErrorLog(2,$source_id,$billing_name,3,$error_msg);
        //     ApiResponseFactory::apiResponse([],[],'',$error_msg);
        //     exit;
        // }
        // $country_map =[];
        // $country_info = CommonLogic::getCountryList($country_map)->get();
        // $country_info = Service::data($country_info);
        // if(!$country_info){
        //     $error_msg = $billing_name.'国家信息数据查询为空';
        //     DataImportImp::saveDataErrorLog(2,$source_id,$billing_name,3,$error_msg);
        //     exit;
        // }
        $array = [];
        $error_log_arr=[];
        $error_detail_arr=[];
        $num = 0;
        $num_country =0;
        $num_channel = 0;
        try {
            foreach ($handinfo as $k => $v) {
                $json_info = json_decode($v['json_data'],true);
                $app_name = trim($json_info['商品名称']);
                $earning = trim($json_info['订单金额']);
                $earning_divide_ad = trim($json_info['手续费']);
                $earning_retreat = trim($json_info['退款金额']);

            	foreach ($app_list as $app_k => $app_v) {

                    if($app_name ==$app_v['billing_app_name']  ){
                        $array[$k]['app_id'] = $app_v['app_id'];
                        $num = 0;
                        break;
                    }else{
                        $num++;
                    }
                }
                if($num){
                    $error_log_arr['app_id'][] = $app_name;
                }
                $array[$k]['channel_id'] =$channel_id;
                $array[$k]['country_id'] =64;             

            	if(($num+$num_country+$num_channel)>0){

                    $error_detail_arr[$k]['platform_id'] = $source_id;
                    $error_detail_arr[$k]['platform_name'] = $billing_name;
                    $error_detail_arr[$k]['platform_type'] =3;
                    $error_detail_arr[$k]['err_date'] = $v['dayid'];
                    $error_detail_arr[$k]['first_level_id'] = $app_name;
                    $error_detail_arr[$k]['first_level_name'] = '';
                    $error_detail_arr[$k]['second_level_id'] = '';
                    $error_detail_arr[$k]['second_level_name'] = '';
                    $error_detail_arr[$k]['money'] = $earning;
                    $error_detail_arr[$k]['account'] = 'zplay';
                    $error_detail_arr[$k]['create_time'] = date('Y-m-d H:i:s');

            		unset($array[$k]);
            		//插入错误数据
            		continue;
            	}

            	$array[$k]['date'] = $v['dayid'];

                $array[$k]['platform_account'] = '掌游';

                $array[$k]['platform_id'] = $source_id;
                //$array[$k]['platform_account'] = null;
                $array[$k]['publisher_id'] = 5;

                $divide_ad =1;
                $bad_account_rate =1;
                //汇率 默认为1
                $ex_info['currency_ex'] =1;
//                $array[$k]['pay_user'] = $json_info['quantity'];
//                $array[$k]['pay_time'] = $json_info['quantity'];
//                $array[$k]['pay_user_all'] = $json_info['quantity'];
//                $array[$k]['pay_time_all'] = $json_info['quantity'];
                $array[$k]['earning'] = $earning*$ex_info['currency_ex'];
                $array[$k]['earning_fix'] =$earning*$ex_info['currency_ex'];//流水人民币
                $array[$k]['earning_divide_plat'] = $earning_divide_ad;//流水平台分成
                $array[$k]['earning_divide_plat_pay'] = $earning*$ex_info['currency_ex']*$bad_account_rate;//流水坏账
                $array[$k]['earning_divide_publisher'] = $earning-$earning_divide_ad-$earning_retreat;
                $array[$k]['income_plat'] = $earning_divide_ad;
                $array[$k]['income_publisher'] = $earning-$earning_divide_ad-$earning_retreat;
                $array[$k]['income_fix'] =$earning-$earning_divide_ad-$earning_retreat;
                $array[$k]['create_time'] = date('Y-m-d H:i:s');
                $array[$k]['update_time'] = date('Y-m-d H:i:s');
                
            }
        } catch (\Exception $e) {
            $error_msg_info = $date.'号,'.$billing_name.'渠道数据匹配失败：'.$e->getMessage();
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
            DataImportImp::saveDataErrorLog(2,$source_id,$billing_name,3,implode(';',$error_msg_array));
            foreach ($date_arr as $key => $value) {
                $array_err =[];
                foreach ($error_detail_arr as $k => $v) {
                    if($v['err_date'] ==$value){
                        $array_err[$k] = $v;
                    }
                }
                DataImportImp::saveDataErrorMoneyLog($source_id,$value,$array_err);
            }
//            CommonFunction::sendMail($error_msg_array,$billing_name.'渠道计费数据处理error');
        }
        if(!empty($array)){
            DB::beginTransaction();
            $update_sql = "update $mysql_table set tongji_type =-1,update_time=now() where date  in  ('$date') and remark ='api接口数据'";
            DB::update($update_sql);
            $map_delete['platform_id'] =$source_id;
            $map_delete['in'] = ['date',$date_arr];
            $map_delete['tongji_type'] = 0;
            DataImportLogic::deleteMysqlHistoryData($mysql_table,$map_delete);
            //拆分批次
            $step = array();
            $i = 0;
            foreach ($array as $kkkk => $insert_data_info) {
                if ($kkkk % 1000 == 0) $i++;
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
            // 
            foreach ($date_arr as $date_time) {
                Artisan::call('FfSummaryProcesses',['begin_date'=>$date_time,'end_date'=>$date_time,'platform_id'=>$source_id]);
                //DB::update("call ff_summary('$date_time','$date_time','$source_id')");
            }
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