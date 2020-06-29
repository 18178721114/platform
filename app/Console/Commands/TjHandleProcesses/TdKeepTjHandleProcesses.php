<?php

namespace App\Console\Commands\TjHandleProcesses;

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

// flurr留存统计
class TdKeepTjHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'TdKeepTjHandleProcesses {dayid?} {data_account?}';

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
//        define('MYSQL_TABLE_NAME','zplay_keep_tj_report_daily');
        $source_id = 'ptj02';
        $source_name = 'talkingdata留存';

        $dayid = $this->argument('dayid') ? $this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));
        try {
            //查询pgsql 的数据
            $flurry_sql = "select * from talkingdata_china_keepuser where tjdate = '$dayid'";
            $info = DB::select($flurry_sql);
            $info = Service::data($info);

            if (!$info) {
//            $error_msg = $dayid.'号，'.$source_name.'统计留存数据处理程序获取原始数据为空';
//            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,1,$error_msg);
                exit;
            }

            //获取匹配应用的数据
            $sql = "SELECT DISTINCT
        c_app.id,
        c_app.app_id,
        c_app_statistic.api_key,
        c_app_statistic.td_app_id,
        c_app_statistic_version.statistic_app_name,
        c_app_statistic_version.app_version,
        c_app_statistic_version.statistic_version,
        c_app_statistic_version.ad_status,
        c_app_statistic_version.channel_id,
        c_channel.td_channel_id,
        c_channel.channel_id as c_channel_id
        FROM
        c_app
        LEFT JOIN c_app_statistic ON c_app.id = c_app_statistic.app_id
        LEFT JOIN c_app_statistic_version ON c_app_statistic.id = c_app_statistic_version.app_statistic_id
        LEFT JOIN c_channel ON c_channel.id = c_app_statistic_version.channel_id
        WHERE
        c_app_statistic.statistic_type = 2
        AND c_app_statistic_version.ad_status != 2";

            $app_list = DB::select($sql);
            $app_list = Service::data($app_list);
            if (!$app_list) {
                $error_msg = $source_name . '统计留存数据处理程序应用数据查询为空';
                DataImportImp::saveDataErrorLog(2, $source_id, $source_name, 1, $error_msg);
                exit;
            }

            //获取对照表国家信息
            // $country_map =[];
            // $country_info = CommonLogic::getCountryList($country_map)->get();
            // $country_info = Service::data($country_info);
            // if(!$country_info){
            //     $error_msg = '国家信息数据查询为空';
            //     DataImportImp::saveDataErrorLog(2,$source_id,$source_name,1,$error_msg);
            //     exit;
            // }

            $array = [];
            $num = 0;
            $num_country = 0;
            $error_log_arr = [];
            $error_detail_arr = [];
            foreach ($info as $k => $v) {
                foreach ($app_list as $app_k => $app_v) {
                    if (($v['app_id'] == $app_v['td_app_id']) && (($v['channel_name'] == $app_v['td_channel_id']) || ($v['channel_name'] == $app_v['channel_id']) || ($v['channel_name'] == $app_v['c_channel_id']))) {
                        $array[$k]['app_id'] = $app_v['app_id'];
                        $array[$k]['ad_status'] = $app_v['ad_status'];
                        $array[$k]['channel_id'] = $app_v['channel_id'];
                        $num = 0;
                        break;
                    } else {
                        //广告位配置未配置
                        $num++;

                    }
                }

                if ($num) {
                    //var_dump($json_info['campaign_id']);
                    $error_log_arr['campaign_id'][] = $v['app_name'] . '/' . $v['channel_name'];
                }

                // todo 匹配国家用
                // foreach ($country_info as $country_k => $country_v) {
                //         if( (isset($v['country']) && $v['country'] == $country_v['name']) ){
                //             $array[$k]['country_id'] = $country_v['c_country_id'];
                //             $num_country = 0;
                //             break;
                //         }else{
                //             //国家配置失败
                //             $num_country++;
                //         }

                //   }
                //     if ($num_country){
                //         $error_log_arr['country'][] = isset($v['country']) ? $v['country'] : 'Unknown Region';
                //     }
                if (($num + $num_country) > 0) {

                    $error_detail_arr[$k]['platform_id'] = $source_id;
                    $error_detail_arr[$k]['platform_name'] = $source_name;
                    $error_detail_arr[$k]['platform_type'] = 1;
                    $error_detail_arr[$k]['err_date'] = $dayid;
                    $error_detail_arr[$k]['first_level_id'] = $v['app_id'];
                    $error_detail_arr[$k]['first_level_name'] = '';
                    $error_detail_arr[$k]['second_level_id'] = '';
                    $error_detail_arr[$k]['second_level_name'] = $v['channel_name'];
                    $error_detail_arr[$k]['money'] = 0; // 流水原币
                    $error_detail_arr[$k]['account'] = 'weibo@zplay.cn';
                    $error_detail_arr[$k]['create_time'] = date('Y-m-d H:i:s');
                    $error_detail_arr[$k]['td_err_type'] = 3;
                    unset($array[$k]);
                    //插入错误数据
                    continue;
                }

                $array[$k]['account'] = 'weibo@zplay.cn';
                $array[$k]['date'] = $dayid;
                $array[$k]['keep_id'] = $v['id'];
                $array[$k]['platform_id'] = $source_id;
                $array[$k]['type'] = 1;
                $array[$k]['create_time'] = date('Y-m-d H:i:s');

            }

            // 保存错误信息
            if ($error_log_arr) {
                $error_msg_array = [];
                $error_msg_mail = [];
                if (isset($error_log_arr['campaign_id'])) {
                    $campaign_id = implode(',', array_unique($error_log_arr['campaign_id']));
                    $error_msg_array[] = '应用ID匹配失败,ID为:' . $campaign_id;
                    $error_msg_mail[] = '应用ID匹配失败，ID为：' . $campaign_id;
                }
                // if (isset($error_log_arr['country'])){
                //     $country = implode(',',array_unique($error_log_arr['country']));
                //     $error_msg_array[] = '国家匹配失败，ID为：<font color="red">'.$country."</font>";
                //     $error_msg_mail[] = '国家匹配失败，ID为：'.$country;
                // }

                DataImportImp::saveDataErrorLog(2, $source_id, $source_name, 1, implode(';', $error_msg_array));
                DataImportImp::saveDataErrorMoneyLog($source_id, $dayid, $error_detail_arr, 3);
                // 发送邮件
//            CommonFunction::sendMail($error_msg_mail,$source_name.'统计留存数据处理error');
            }

            // 保存正确数据
            if ($array) {
                DB::beginTransaction();
                $map_delete = [];
                $map_delete['platform_id'] = $source_id;
                $map_delete['date'] = $dayid;
                DataImportLogic::deleteMysqlHistoryData('zplay_keep_tj_report_daily', $map_delete);
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
                        $result = DataImportLogic::insertAdReportInfo('zplay_keep_tj_report_daily', $v);
                        if (!$result) {
                            DB::rollBack();
                            $is_success[] = $k;
                        }
                    }
                    echo '处理成功';
                }
                DB::commit();
                // 调用存储过程更新总表数据
                //DB::update("call tj_summary('$source_id')");
//            Artisan::call('TjSummaryProcesses',['platform_id'=>$source_id]);

                //           // 查询广告数据
                // $report_map = [];
                // $report_map['platform_id'] = $source_id;
                // $report_map['date'] = $dayid;
                // $group_by = ['platform_id','date','account'];
                // $report_list = PlatformLogic::getAdReportSum(MYSQL_TABLE_NAME,$report_map)->select(DB::raw("sum(new_user) as cost"),'platform_id','date','account')->groupBy($group_by)->get();
                // $report_list = Service::data($report_list);
                // if ($report_list){
                //     // 保存广告平台
                //     foreach ($report_list as $value){
                //         PlatformImp::add_platform_status($source_id,$value['account'],$value['cost'],$dayid);
                //     }

                // }
            } else {
                echo '暂无匹配成功数据';
            }
        }catch (\Exception $e) {
            // 异常报错
            $message = "{$dayid}号, " . $source_name . "统计平台程序报错,报错原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, $source_id, $source_name, 1, $message);
            $error_msg_arr[] = $message;
//            CommonFunction::sendMail($error_msg_arr, '统计平台程序error');
            exit;
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