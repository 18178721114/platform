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

class YumiPolymerizationHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'YumiPolymerizationHandleProcesses {dayid?} ';

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
   
        set_time_limit(0);
//        ini_set("memory_limit",'1024M');
        $dayid = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));
        //查询pgsql 的数据
        $source_id = 'pad262';
        $source_name = '玉米广告国内安卓渠道';
        var_dump($source_name.'-'.$source_id.'-'.$dayid);
        self::YumiDataProcess($dayid,$source_id,$source_name);
    }


    private static function YumiDataProcess($dayid,$source_id,$source_name){

        //玉米数据库取回来的数据
        $sql ="select * from yumi_polymerization_data where date = '{$dayid}' and earning > 0";

        $info = DB::select($sql);
        $info = Service::data($info);
        var_dump($source_name.'-'.$source_id.'-'.$dayid.'-原始数据数据条数：'.count($info));
//        var_dump(222,count($info));die;
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
            LEFT JOIN `c_app_ad_platform` ON `c_app_ad_platform`.`app_id` = `c_app`.`id`  and `c_app_ad_platform`.`status` = 1
            LEFT JOIN `c_app_ad_slot` ON `c_app_ad_slot`.`app_ad_platform_id` = `c_app_ad_platform`.`id`   and `c_app_ad_slot`.`status` = 1
            LEFT JOIN 
            c_platform AS c_platform ON `c_platform`.`platform_id` = `c_app_ad_platform`.`platform_id`
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
        // $effective_time = date("Ym",strtotime($dayid));
        // $sql = "select a.data_account,c.`currency_ex` from c_platform_account_mapping a,c_platform b,`c_currency_ex` c
        //             where a.platform_id = '{$source_id}'
        // and b.platform_id = a.platform_id and a.`customer_id` = b.`customer_id` and a.`company_id` = b.`company_id` and b.`currency_type_id` = c.`currency_id` and c.effective_time = '{$effective_time}' ";

        // $ex_info = DB::select($sql);
        // $ex_info = Service::data($ex_info);
        $channel_sql ='select * from c_channel';
        $channel_list = DB::select($channel_sql);
        $channel_list = Service::data($channel_list);


        $c_platfrom_yumi_sql ='select * from c_platfrom_yumi';
        $c_platfrom_yumi = DB::select($c_platfrom_yumi_sql);
        $c_platfrom_yumi = Service::data($c_platfrom_yumi);


        //获取对照表国家信息
        $country_map =[];
        $country_info = CommonLogic::getCountryList($country_map)->get();
        $country_info = Service::data($country_info);
        if(!$country_info){
            $error_msg = $dayid.'号，'.$source_name.'广告平台数据处理程序国家信息数据查询为空';
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

        //获取对照表广告类型
        $AdType_map['platform_id'] =$source_id;
        $AdType_info = CommonLogic::getAdTypeCorrespondingList($AdType_map)->get();
        $AdType_info = Service::data($AdType_info);
        if(!$AdType_info){
            echo "广告类型数据查询失败";
            $error_msg = $dayid.'号，'.$source_name.'广告类型数据查询失败';
            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,2,$error_msg);
            exit;
        }
        $array = [];
        $error_log_arr=[];
        $num = 0;
        $num_country = 0;
        $num_adtype =0;
        $num_channel =0;
        $num_yumi_plat =0;
        $error_detail_arr = [];//报错的 详细数据信息
        // 反更新数据
//        $new_campaign_ids = [];
        foreach ($info as $k => $v) {

            $json_info = $v;
            $yumi_app_id = $json_info['app_id'];
            foreach ($app_list as $app_k => $app_v) {
                if($json_info['app_id'] ==$app_v['platform_app_id']){
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
            $err_name = (isset($json_info['slot_uuid']) ?$json_info['slot_uuid']:'Null').'#'.(isset($json_info['slot_name']) ?$json_info['slot_name']:'Null').'#'.(isset($json_info['app_id']) ?$json_info['app_id']:'Null').'#'.(isset($json_info['app_name']) ?$json_info['app_name']:'Null');

            if($num){
                $error_log_arr['siteId'][] = $json_info['app_id'].'('.$err_name.')';
            }
            foreach ($country_info as $country_k => $country_v) {
                if(strtolower(str_replace('\'\'','\'',$json_info['country'])) == strtolower($country_v['name'])){
                    $array[$k]['country_id'] = $country_v['c_country_id'];
                    $num_country = 0;
                    break;
                }else{
                    //
                    //国家配置失败
                    $num_country++;

                }
            }
            if($num_country){
                $error_log_arr['country'][] = isset($json_info['country']) ? str_replace('\'\'','\'',$json_info['country']).'('.$err_name.')' : 'Unknown Region' ;
            }
            foreach ($AdType_info as $AdType_k => $AdType_v) {
             if($json_info['ad_type'] == $AdType_v['name'] ){
                 $array[$k]['ad_type'] = $AdType_v['ad_type_id'];
                 $num_adtype = 0;
                 break;
             }else{
                 //广告类型失败
                 $num_adtype++;

             }
            }
           if($num_adtype){
               $error_log_arr['ad_type'][] = isset($json_info['ad_type']) ? str_replace('\'\'','\'',$json_info['ad_type']).'('.$err_name.')' :'';
           }
            //渠道匹配
            foreach ($channel_list as $channel_k => $channel_v) {
               if($json_info['channel'] ==$channel_v['channel_id'] || $json_info['channel'] ==$channel_v['td_channel_id'] ){
                 $array[$k]['channel_id'] = $channel_v['channel_id'];
                 $num_channel =0;
                 break;

             }else{
                $num_channel ++;
               }
            }

            if($num_channel){
               $error_log_arr['channel'][] = isset($json_info['channel']) ? str_replace('\'\'','\'',$json_info['channel']).'('.$err_name.')' :'';
           }

           //平台匹配
           foreach ($c_platfrom_yumi as $c_platfrom_yumi_k => $c_platfrom_yumi_v) {
            if($json_info['ad_plat_id'] ==$c_platfrom_yumi_v['ad_plat_id']){
                $array[$k]['platform_id'] = $c_platfrom_yumi_v['platform_id'];
                $num_yumi_plat =0;
                break;

            }else{
                $num_yumi_plat ++;
            }
        }

        if($num_yumi_plat){
            $error_log_arr['platfrom_yumi'][] = isset($json_info['ad_plat_id']) ? str_replace('\'\'','\'',$json_info['ad_plat_id']).'('.$err_name.')' :'';
        }
            if(($num+$num_country+$num_adtype+$num_channel+$num_yumi_plat)>0){
                $error_detail_arr[$k]['platform_id'] = $source_id;
                $error_detail_arr[$k]['platform_name'] = $source_name;
                $error_detail_arr[$k]['platform_type'] =2;
                $error_detail_arr[$k]['err_date'] = $dayid;
                $error_detail_arr[$k]['first_level_id'] = addslashes($json_info['app_id']);
                $error_detail_arr[$k]['first_level_name'] = addslashes(str_replace('\'\'','\'',$json_info['app_name']));
                $error_detail_arr[$k]['second_level_id'] = ''; // addslashes($json_info['slot_uuid']);
                $error_detail_arr[$k]['second_level_name'] = addslashes(str_replace('\'\'','\'',$json_info['slot_name']));
                $error_detail_arr[$k]['money'] = $json_info['earning'];
                $error_detail_arr[$k]['account'] = 'zplay';
                $error_detail_arr[$k]['create_time'] = date('Y-m-d H:i:s');
                unset($array[$k]);
                //插入错误数据
                continue;
            }


            $array[$k]['date'] = $dayid;
            $array[$k]['data_account'] = 'tangqiuli@zplay.cn';
            $array[$k]['platform_app_id'] = addslashes($json_info['app_id']);
            $array[$k]['platform_app_name'] = addslashes(str_replace('\'\'','\'',$json_info['app_name'])) ;
            $array[$k]['ad_unit_id'] = addslashes($json_info['slot_uuid']);
            $array[$k]['ad_unit_name'] = addslashes(str_replace('\'\'','\'',$json_info['slot_name'])) ;
            $array[$k]['impression'] = $json_info['imp'];
            $array[$k]['all_request'] = $json_info['request'];
            $array[$k]['success_requests'] = $json_info['request_s'];
            $array[$k]['fail_requests'] = $json_info['request_f'];
            $array[$k]['impression_port'] = $json_info['imp_port'];
            $array[$k]['impression_begin'] = $json_info['imp_begin'];
            $array[$k]['reward'] = $json_info['reward'];
            $array[$k]['click'] = $json_info['click'];
            $array[$k]['earning'] = $json_info['earning'];
            // if($app_list[0]['divide_ad']){
            //     $divide_ad = 1-$app_list[0]['divide_ad']/100;
            // }else{
            //     $divide_ad =1;
            // }
            $divide_ad =1;
            // 汇率判断
            $currency_ex = 1;
            // if ($ex_info){
            //     foreach ($ex_info as $eik => $eiv){
            //         if ($eiv['data_account'] == $v['account']){
            //             $currency_ex = $eiv['currency_ex'];
            //         }
            //     }
            // }

            $array[$k]['earning_exc'] = $json_info['earning']*$divide_ad;
            $array[$k]['earning_flowing'] =isset($json_info['earning']) ? $json_info['earning']* $currency_ex: 0;
            $array[$k]['earning_fix'] = $json_info['earning']*$currency_ex*$divide_ad;

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

            $array[$k]['data_platform_id'] = $source_id;
            $array[$k]['version'] = $json_info['version'];
            $array[$k]['statistics'] = '2';//0是三方2是玉米
//          $array[$k]['flow_type'] = '2';//(1,自有流量;2,三方流量)
            $array[$k]['create_time'] = date('Y-m-d H:i:s');
            $array[$k]['update_time'] = date('Y-m-d H:i:s');

        }

        // 保存错误信息
        if ($error_log_arr){
            $error_msg_array = [];
            $error_msg_mail = [];
            $error_log_arr = Service::shield_error($source_id,$error_log_arr);


            if (isset($error_log_arr['siteId']) && !empty($error_log_arr['siteId'])){
                $app_id = implode(',',array_unique($error_log_arr['siteId']));
                $error_msg_array[] = '应用id匹配失败,ID为:'.$app_id;
                $error_msg_mail[] = '应用id匹配失败，ID为：'.$app_id;
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
            if (isset($error_log_arr['channel']) && !empty($error_log_arr['channel'])){
                $channel = implode(',',array_unique($error_log_arr['channel']));
                $error_msg_array[] = '渠道id匹配失败,code为:'.$channel;
                $error_msg_mail[] = '渠道id匹配失败，code为：'.$channel;
            }

            if (isset($error_log_arr['platfrom_yumi']) && !empty($error_log_arr['platfrom_yumi'])){
                $platfrom_yumi = implode(',',array_unique($error_log_arr['platfrom_yumi']));
                $error_msg_array[] = '玉米平台id匹配失败,code为:'.$platfrom_yumi;
                $error_msg_mail[] = '玉米平台id匹配失败，code为：'.$platfrom_yumi;
            }


            if(!empty($error_msg_array)) {
                DataImportImp::saveDataErrorLog(2, $source_id, $source_name, 2, implode(';', $error_msg_array));
            }
            DataImportImp::saveDataErrorMoneyLog($source_id,$dayid,$error_detail_arr);

            //CommonFunction::sendMail($error_msg_mail,$source_name.'广告平台数据处理error');
        }
        var_dump($source_name.'-'.$source_id.'-'.$dayid.'-匹配成功数据条数：'.count($array));
        if(!empty($array)){
            $map_delete['data_platform_id'] = $source_id;
            $map_delete['date'] = $dayid;
            $map_delete['statistics'] = 2;
            DataImportLogic::deleteMysqlHistoryData('zplay_ad_report_daily',$map_delete);
            //拆分批次
            $step = array();
            $i = 0;
            foreach ($array as $kkkk => $insert_data_info) {
                if ($kkkk % 300 == 0) $i++;
                if ($insert_data_info) {
                    $step[$i][] = $insert_data_info;
                }
            }
            $is_success = [];
            if ($step) {
                foreach ($step as $k => $v) {
                    $result = DataImportLogic::insertAdReportInfo('zplay_ad_report_daily', $v);
                    if (!$result) {
                        $is_success[] = $k;
                    }
                }
            }

            // 调用存储过程更新总表数据
            //DB::update("call ad_summary_channel('$dayid','$dayid','$source_id')");

            // 查询广告数据
//            $report_map = [];
//            $report_map['platform_id'] = $source_id;
//            $report_map['date'] = $dayid;
//            $map_delete['statistics'] = 2;
//            $group_by = ['platform_id','date','data_account'];
//            $report_list = PlatformLogic::getAdReportSum('zplay_ad_report_daily',$report_map)->select(DB::raw("sum(earning) as cost"),'platform_id','date','data_account')->groupBy($group_by)->get();
//            $report_list = Service::data($report_list);
//            if ($report_list){
//                // 保存广告平台
//                foreach ($report_list as $value){
//                    $value['data_account']=' ';
//                    PlatformImp::add_platform_status($source_id,$value['data_account'],$value['cost'],$dayid);
//                }
//            }
            //echo '处理完成';

        }else{
            //echo '暂无处理数据';
        }
    }
}