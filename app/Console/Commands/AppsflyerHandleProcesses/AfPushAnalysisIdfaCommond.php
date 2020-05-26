<?php

namespace App\Console\Commands\AppsflyerHandleProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\BusinessLogic\DataImportLogic;
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
use App\BusinessImp\PlatformImp;
use App\BusinessLogic\PlatformLogic;
use Illuminate\Support\Facades\Redis;

class AfPushAnalysisIdfaCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = ' AfPushAnalysisIdfaCommond {dayid?} {hours?}';
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
//        define('MYSQL_TABLE_NAME','zplay_tg_report_daily');
        $source_id = 'ptg02';
        $source_name = 'Appsflyer';

        $dayid = $this->argument('dayid') ? $this->argument('dayid'):date('Y-m-d');
        $hours = $this->argument('hours') ? $this->argument('hours'):date("H", time());

        //查询pgsql 的数据
        $map =[];
        $map['dayid'] = $dayid;
        $map['hours'] = $hours;
        $info = DataImportLogic::getChannelData('appsflyer_push_data','erm_data',$map)->get();
        $info = Service::data($info);
        if(!$info){
            $error_msg = $dayid.'号，'.$source_name.'推广平台af-push数据处理程序获取原始数据为空';
            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,4,$error_msg);
            exit;
        }
        var_dump(count($info));
        //获取匹配应用的数据
        $sql = "SELECT  distinct
                c_app.id,c_app.app_id,c_app.os_id,c_generalize.platform_id,c_generalize.data_account,c_generalize.application_id,c_generalize.application_name,c_generalize.agency_platform_id,c_generalize_ad_app.campaign_id,c_generalize_ad_app.campaign_name,c_generalize_ad_app.ad_group_id,c_platform.currency_type_id,cpp.currency_type_id as ageccy_currency_type_id
                FROM c_app 
                LEFT JOIN c_generalize ON c_app.id = c_generalize.app_id 
                LEFT JOIN c_generalize_ad_app ON c_generalize.id = c_generalize_ad_app.generalize_id 
                LEFT JOIN c_platform ON c_generalize.platform_id = c_platform.platform_id 
                LEFT JOIN c_platform as cpp ON c_generalize.agency_platform_id = cpp.platform_id 
                WHERE 
                c_generalize.platform_id = '$source_id'   ";
        //and c_generalize_ad_app.status = 1";

        $app_list = DB::select($sql);
        $app_list = Service::data($app_list);
        //var_dump($app_list);die;

        if(!$app_list){
            $error_msg = $source_name.'推广平台af-push数据处理程序应用数据查询为空';
            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,4,$error_msg);
            exit;
        }
        //匹配平台
        $platform_match_sql = 'select * from appsflyer_platform_matching ';
        $platform_match = DB::select($platform_match_sql);
        $platform_match_info = Service::data($platform_match);
        $array = [];
        $num = 0;
        $num_platform=0;
        $error_log_arr = [];
        $error_detail_arr = [];
        foreach ($info as $k => $v) {
            $v =  json_decode($v['json_data'],true);
//            var_dump($v['app_id']);
//            die;
            if(empty($v['advertising_id']) && empty($v['idfa'])&& empty($v['imei']))  continue ;

            foreach ($app_list as $app_k => $app_v) {
                if(isset($v['app_id']) && ($v['app_id']== $app_v['application_id'])){
                    $array[$k]['app_id'] = $app_v['id'];
                    $num = 0;
                    break;
                }else{
                    //广告位配置未配置
                    $num++;

                }
            }
            if ($num){
                $error_log_arr['appid'][] = $v['app_id'];
            }
            $array[$k]['platform_id']=$v['media_source'];
            // todo 匹配平台
            foreach ($platform_match_info as $platform_k => $platform_v) {
                if( (isset($v['media_source']) &&  strtoupper($v['media_source']) == strtoupper($platform_v['source_name']))){
                    $array[$k]['platform_id'] = $platform_v['platform_id'];
                    $num_platform = 0;
                    break;
                }else{
                    //平台配置失败
                    $num_platform++;
                }

            }
            if ($num_platform>0){
                $error_log_arr['platform_id'][] = $v['media_source'];
            }

            if(($num)>0){

                unset($array[$k]);
                //插入错误数据
                continue;
            }
            $array[$k]['date'] = $dayid;
            $array[$k]['hours'] = $hours;
            $array[$k]['ip'] = isset($v['ip']) ? addslashes($v['ip']): '';
            $array[$k]['advertising_id'] = isset($v['advertising_id']) ? addslashes($v['advertising_id']) : '';
            $array[$k]['idfa'] = isset($v['idfa'])  ? addslashes($v['idfa']) : '';
            $array[$k]['imei'] = isset($v['imei']) ? addslashes($v['imei'])  : '';
            $array[$k]['state'] = isset($v['state']) ? addslashes($v['state']) : '';
            $array[$k]['city'] = isset($v['city']) ? addslashes($v['city']) : '';
            $array[$k]['postal_code'] = isset($v['postal_code']) ? addslashes($v['postal_code'])  : '';
            $array[$k]['country_code'] = isset($v['country_code']) ? addslashes($v['country_code']): '';
            $array[$k]['create_time'] = date('Y-m-d H:i:s');
        }

        // 保存错误信息
        if ($error_log_arr){
            $error_msg_array = [];
            $error_msg_mail = [];
            if (isset($error_log_arr['appid'])){
                $appid = implode(',',array_unique($error_log_arr['appid']));
                $error_msg_array[] = 'appid匹配失败,ID为:'.$appid;
                $error_msg_mail[] = 'appid匹配失败，ID为：'.$appid;
            }
            if (isset($error_log_arr['platform_id'])){
                $platform_id = implode(',',array_unique($error_log_arr['platform_id']));
                $error_msg_array[] = 'platform_id匹配失败,ID为:'.$platform_id;
                $error_msg_mail[] = 'platform_id匹配失败，ID为：'.$platform_id;
            }
            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,4,implode(';',$error_msg_array));
            DataImportImp::saveDataErrorMoneyLog($source_id,$dayid,$error_detail_arr);
            // 发送邮件
//            CommonFunction::sendMail($error_msg_mail,$source_name.'推广平台数据处理error');
        }
//        var_dump(count($error_log_arr));
//        var_dump(count($array));die;
        if ($array) {
            DB::beginTransaction();
            $map_delete = [];
            $map_delete['date'] = $dayid;
            $map_delete['hours'] = $hours;
            DataImportLogic::deleteMysqlHistoryData('zplay_appsflyer_device_num', $map_delete);
            //拆分批次
            $step = array();
            $i = 0;
            foreach ($array as $kkkk => $insert_data_info) {
                if ($kkkk % 500== 0) $i++;
                if ($insert_data_info) {
                    $step[$i][] = $insert_data_info;
                }
            }

            $is_success = [];
            if ($step) {
                foreach ($step as $k => $v) {
                    $result = DataImportLogic::insertAdReportInfo('zplay_appsflyer_device_num', $v);
                    if (!$result) {
                        DB::rollBack();
                        $is_success[] = $k;
                    }
                }
            }
            DB::commit();

        }

    }
}