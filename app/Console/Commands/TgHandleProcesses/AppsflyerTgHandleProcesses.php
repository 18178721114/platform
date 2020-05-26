<?php

namespace App\Console\Commands\TgHandleProcesses;

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
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\ApplicationLogic;
use App\BusinessLogic\CommonLogic;
use App\BusinessImp\PlatformImp;
use App\BusinessLogic\PlatformLogic;
use Illuminate\Support\Facades\Redis;
use Illuminate\Support\Facades\Artisan;

class AppsflyerTgHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'AppsflyerTgHandleProcesses {dayid?} {data_account?}';

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
        define('MYSQL_TABLE_NAME','zplay_tg_report_daily');
        $source_id = 'ptg02';
        $source_name = 'AppsFlyer';

        $dayid = $this->argument('dayid') ? $this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));
        var_dump($source_name.'-'.$source_id.'-'.$dayid);
        // $data_account = $this->argument('data_account') ? $this->argument('data_account'): '';

        // if (!$data_account){
        //     $error_msg = $dayid.'号，'.$source_name.'平台处理程序账号有误';
        //     DataImportImp::saveDataErrorLog(2,$source_id,$source_name,4,$error_msg);
        //     exit;
        // }


        //查询pgsql 的数据
        $sql ="select 
        p.platform_id,
        a.date,
        a.appid,
        a.`media_source_(pid)`as source,
        a.`campaign_(c)` as campaign_name,
        a.country as country,
        sum(a.impressions) as impression,
        sum(a.clicks) as click,
        sum(a.installs) as install, 
        sum(a.total_cost) as cost,
        'contact@zplay.cn' as account,
        a.currency,
        round((sum(a.installs)*b.pad/b.install) ,0) as install_pad,
        now()
        from appsflyer a 
        LEFT JOIN (select date_format(t.eventtime,'%Y%m%d') as time,
        t.appid,
        t.mediasource,
        campaign,
        t.countrycode,
        count(*) as install,
        count(*)- count(case when t.devicetype like 'iPad%' then '1' else null end)  as phone,
        count(case when t.devicetype like 'iPad%' then '1' else null end) as pad,
        sum(costvalue)
        from appsflyer_details  t
        where  t.eventtime like '$dayid%'
        group by date_format(t.eventtime,'%Y%m%d'),
        t.mediasource,
        t.appid,
        t.countrycode,
        t.campaign) b on
        a.date = b.time
        AND a.appid = b.appid
        AND a.`media_source_(pid)` = b.mediasource
        AND a.`campaign_(c)` = b.campaign
        AND a.country = b.countrycode
        LEFT JOIN  appsflyer_platform_matching p on p.source_name = a.`media_source_(pid)` AND p.data_platform_id = 'ptg02'
        where a.date  = '$dayid' and  p.platform_id in ('ptg00','ptg31','ptg23','ptg02','ptg98','ptg99','ptg01')
        group by a.date,a.appid ,a.`media_source_(pid)`,a.`campaign_(c)`,a.country,currency";
        //echo $sql;
        $info =db::select($sql);
        $info = Service::data($info);
        if(!$info){
//            $error_msg = $dayid.'号，'.$source_name.'推广平台数据处理程序获取原始数据为空';
//            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,4,$error_msg);
            exit;
        }



        //获取匹配应用的数据
        $sql = "SELECT  distinct
                c_app.id,c_app.app_id,c_generalize.platform_id,c_generalize.data_account,c_generalize.application_id,c_generalize.application_name,c_generalize.agency_platform_id,c_generalize_ad_app.campaign_id,c_generalize_ad_app.campaign_name,c_generalize_ad_app.ad_group_id,c_platform.currency_type_id,cpp.currency_type_id as ageccy_currency_type_id
                FROM c_app 
                LEFT JOIN c_generalize ON c_app.id = c_generalize.app_id 
                LEFT JOIN c_generalize_ad_app ON c_generalize.id = c_generalize_ad_app.generalize_id 
                LEFT JOIN c_platform ON c_generalize.platform_id = c_platform.platform_id 
                LEFT JOIN c_platform as cpp ON c_generalize.agency_platform_id = cpp.platform_id 
                WHERE 
                c_generalize.platform_id = '$source_id' 
                and c_generalize.generalize_status = 1 ";
                //and c_generalize_ad_app.status = 1";

        $app_list = DB::select($sql);
        $app_list = Service::data($app_list);

        if(!$app_list){
            $error_msg = $source_name.'推广平台数据处理程序应用数据查询为空';
            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,4,$error_msg);
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
            $error_msg = $source_name.'推广平台数据处理程序国家信息数据查询为空';
            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,4,$error_msg);
            exit;
        }
        // //获取对照表广告类型
        // $AdType_map['platform_id'] = $source_id;
        // $AdType_info = CommonLogic::getAdTypeCorrespondingList($AdType_map)->get();
        // $AdType_info = Service::data($AdType_info);
        // if(!$AdType_info){
        //     $error_msg = '广告类型数据查询失败';
        //     DataImportImp::saveDataErrorLog(2,$source_id,$source_name,4,$error_msg);
        //     exit;
        // }
        // 
        //获取平台的汇率
        $ex_map['effective_time'] = date("Ym",strtotime($dayid));
        $ex_map['currency_id'] = $app_list[0]['currency_type_id'];
        $ex_fields=['currency_ex'];
        $ex_info = CommonLogic::getCurrencyEXList($ex_map,$ex_fields)->orderby('effective_time','desc')->first();
        $ex_info = Service::data($ex_info);
        if(!$ex_info){
            $error_msg = $source_name.'推广平台数据处理程序汇率数据查询为空';
            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,4,$error_msg);
        }
        $array = [];
        $num = 0;
        $num_country = 0;
        $num_adtype = 0;
        $num_tongji = 0;
        $error_log_arr = [];
        // echo '<pre>';
        // var_dump($info);die;
        foreach ($info as $k => $v) {
            foreach ($app_list as $app_k => $app_v) {
                if(isset($v['appid']) && ($v['appid'] == $app_v['application_id'])){
                    $array[$k]['app_id'] = $app_v['app_id'];
                    $array[$k]['platform_account'] = isset($app_v['account'])?$app_v['account']:NUll;
                    $array[$k]['agency_platform_id'] = isset($app_v['agency_platform_id'])?$app_v['agency_platform_id']:NUll;



                    $num = 0;
                    break;
                }else{
                    //广告位配置未配置
                    $num++;

                }
            }

            if ($num){
                $error_log_arr['campaign_id'][] = $v['appid'];
            }
            // todo 匹配国家用
         foreach ($country_info as $country_k => $country_v) {
               if( (isset($v['country']) &&  strtoupper($v['country']) == strtoupper($country_v['name']))){
                   $array[$k]['country_id'] = $country_v['c_country_id'];
                   $num_country = 0;
                   break;
               }else{
                   //国家配置失败
                   $num_country++;
               }

         }

           if ($num_country){
               $error_log_arr['country'][] = isset($v['country']) ? $v['country'] : '';
           }

           // foreach ($AdType_info as $AdType_k => $AdType_v) {
           //  if(($v['size'].'-'.$v['ad_type']) == $AdType_v['name'] ){
           //         $array[$k]['ad_type'] = $AdType_v['ad_type_id'];
           //         $num_adtype = 0;
           //         break;
           //   }else{
           //       //广告类型失败
           //       $num_adtype++;
                    
           //   }
           //  }
           //  if ($num_adtype){
           //     $error_log_arr['ad_type'][] = $v['size'].'-'.$v['ad_type'];
           // }

            if(($num+$num_country+$num_adtype)>0){
                unset($array[$k]);
                //插入错误数据
                continue;
            }
            if( $v['platform_id'] =='ptg23'  || $v['platform_id'] =='ptg98'){
                $array[$k]['tongji_type'] = 0;
            }else{
                $array[$k]['tongji_type'] = 2;
            }
            $array[$k]['platform_id'] = $v['platform_id'];
            $array[$k]['data_account'] = $v['account'];
            $array[$k]['date'] = $dayid;
            $array[$k]['platform_app_id'] = isset($v['appid']) ? addslashes($v['appid']) : '';
            $array[$k]['impression'] = isset($v['impressions']) ? $v['impressions'] : 0;
            $array[$k]['click'] = isset($v['click']) ? $v['click'] : 0;

            $array[$k]['cost'] = isset($v['cost']) ? $v['cost'] : 0.00; // 流水原币

            $currency_ex = floatval($ex_info['currency_ex']);
            if (!$currency_ex){
                $currency_ex = 1;
            }
            $phone = (int)$v['install'] -(int)$v['install_pad'];
            if($phone > 0 && (int)$v['install_pad'] ==0){
                $array[$k]['device_type'] = 1;

            }elseif($phone== 0 && (int)$v['install_pad'] >0) {
                $array[$k]['device_type'] = 2;
            }else{
                $array[$k]['device_type'] = 0;
            } 
            $array[$k]['cost_exc'] = isset($v['cost']) ? $v['cost'] * $currency_ex : 0;

            // 成本美元
            if (($array[$k]['cost'] == $array[$k]['cost_exc']) && $usd_currency_ex){
                $array[$k]['cost_usd'] = $array[$k]['cost_exc'] / $usd_currency_ex;
            }else{
                $array[$k]['cost_usd'] = $array[$k]['cost'];
            }

            $array[$k]['new'] = (int)$v['install'];
            $array[$k]['new_pad'] = (int)$v['install_pad'];
            $array[$k]['new_phone'] = $phone;
            $array[$k]['data_platform_id'] = $source_id;
            
            $array[$k]['create_time'] = date('Y-m-d H:i:s');
            $array[$k]['update_time'] = date('Y-m-d H:i:s');
        }

        // 保存错误信息
        if ($error_log_arr){
            $error_msg_array = [];
            $error_msg_mail = [];
            if (isset($error_log_arr['campaign_id'])){
                $campaign_id = implode(',',array_unique($error_log_arr['campaign_id']));
                $error_msg_array[] = 'appid匹配失败,ID为:'.$campaign_id;
                $error_msg_mail[] = 'appid匹配失败，ID为：'.$campaign_id;
            }
            if (isset($error_log_arr['country'])){
                $country = implode(',',array_unique($error_log_arr['country']));
                $error_msg_array[] = '国家匹配失败,code为:'.$country;
                $error_msg_mail[] = '国家匹配失败，code为：'.$country;
            }
            // if (isset($error_log_arr['ad_type'])){
            //     $ad_type = implode(',',array_unique($error_log_arr['ad_type']));
            //     $error_msg_array[] = '广告类型匹配失败，ID为：<font color="red">'.$ad_type."</font>";
            //     $error_msg_mail[] = '广告类型匹配失败，ID为：'.$ad_type;
            // }

            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,4,implode(';',$error_msg_array));
            // 发送邮件
//            CommonFunction::sendMail($error_msg_mail,$source_name.'推广平台数据处理error');
        }
        // 保存正确数据
        if ($array) {
            $map_delete = [];
            //$map_delete['data_account'] = $data_account;
            $map_delete['data_platform_id'] = $source_id;
            $map_delete['date'] = $dayid;
            DataImportLogic::deleteMysqlHistoryData(MYSQL_TABLE_NAME, $map_delete);
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
                    $result = DataImportLogic::insertAdReportInfo(MYSQL_TABLE_NAME, $v);
                    if (!$result) {
                        $is_success[] = $k;
                    }
                }
            }
            Artisan::call('TgSummaryProcesses',['begin_date'=>$dayid,'end_date'=>$dayid,'platform_id'=>$source_id]);

            // 调用存储过程更新总表数据
//            DB::update("call tg_summary('$dayid','$dayid','$source_id')");

            // 查询广告数据
            $report_map = [];
            $report_map['data_platform_id'] = $source_id;
            $report_map['date'] = $dayid;
            $group_by = ['data_platform_id','date','data_account'];
            $report_list = PlatformLogic::getAdReportSum(MYSQL_TABLE_NAME,$report_map)->select(DB::raw("sum(cost) as cost"),'data_platform_id','date','data_account')->groupBy($group_by)->get();
            $report_list = Service::data($report_list);

            if ($report_list){
                // 保存广告平台
                foreach ($report_list as $value){
                    PlatformImp::add_platform_status($source_id,$value['data_account'],$value['cost'],$dayid);
                }
            }
            echo '处理完成';
        }
    }

    // 保存日志
    private static function saveLog($platform_name = '未知', $message = ''){

        $fileName = date('Y-m-d',time());
        $dir = './storage/adDataLogs';

        if (!is_dir($dir)) {
            mkdir($dir,0777,true);
        }
        $logFilename = $dir.'/'.$platform_name.'_ad'.'.log';
        //生成日志
        file_put_contents( $logFilename,$message . "\n\n",FILE_APPEND);
    }
}