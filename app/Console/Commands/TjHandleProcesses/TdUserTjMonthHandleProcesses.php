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

// talkingdata用户统计
class TdUserTjMonthHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'TdUserTjMonthHandleProcesses {dayid?}';

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
//        define('MYSQL_TABLE_NAME','zplay_user_tj_report_daily');
        $source_id = 'ptj02';
        $source_name = 'TalkingData月活用户';

        $dayid = $this->argument('dayid') ? $this->argument('dayid'):date('Y-m-01',time());
        try {
            $real_dayid = date('Y-m-01', strtotime('-1 month', strtotime($dayid)));
            var_dump($real_dayid);
            $error_msg = $dayid . '号，' . $source_name . '数据处理程序开始时间：' . date('Y-m-d H:i:s');
            DataImportImp::saveDataErrorLog(2, $source_id, $source_name, 1, $error_msg);

            //查询pgsql 的数据
            $talkingdata_sql = "select * from talkingdata_user_month where day = '$dayid'";
            $info = DB::select($talkingdata_sql);
            $info = Service::data($info);

            var_dump(count($info));
            if (!$info) {
//            $error_msg = $dayid.'号，'.$source_name.'统计省份数据处理程序获取原始数据为空';
//            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,1,$error_msg);
                exit;
            }

            //获取匹配应用的数据
            $sql = "SELECT DISTINCT
        c_app.id,
        c_app.app_id,
        c_app.os_id,
        if(c_app.os_id = 1,2,1) as td_os_id,
        c_app_statistic.td_app_id
        FROM
        c_app
        LEFT JOIN c_app_statistic ON c_app.id = c_app_statistic.app_id
        WHERE
        c_app_statistic.statistic_type = 2";

            $app_list = DB::select($sql);
            $app_list = Service::data($app_list);
            if (!$app_list) {
                $error_msg = $source_name . '数据处理程序应用数据查询为空';
                DataImportImp::saveDataErrorLog(2, $source_id, $source_name, 1, $error_msg);
                exit;
            }


            $array = [];
            $num = 0;
            $num_country = 0;
            $error_log_arr = [];
            $error_detail_arr = [];
            foreach ($info as $k => $v) {

                foreach ($app_list as $app_k => $app_v) {
                    if (($v['appid'] == $app_v['td_app_id']) && ($v['platformid'] == $app_v['td_os_id'])) {
                        $array[$k]['app_id'] = $app_v['id'];
                        $num = 0;
                        break;
                    } else {
                        //广告位配置未配置
                        $num++;
                    }
                }

                if ($num) {
                    $error_log_arr['campaign_id'][] = $v['app_name'] . '#' . $v['platformid'];
                }


                $array[$k]['account'] = 'weibo@zplay.cn';
//            $array[$k]['td_app_id'] = isset($app_v['td_app_id']) ? addslashes($app_v['td_app_id']) : '';
                $array[$k]['date'] = $real_dayid;
                $array[$k]['channel_plat_name'] = '';
                $array[$k]['version_id'] = '';
                $array[$k]['new_user'] = $v['new_user'];
                $array[$k]['active_user'] = isset($v['active_user_month']) ? addslashes($v['active_user_month']) : '';
                $array[$k]['session_time'] = isset($v['session']) ? addslashes($v['session']) : 0;
                $array[$k]['session_length'] = isset($v['sessionlength']) ? addslashes($v['sessionlength']) : 0;;
                $array[$k]['flow_type'] = 0;
                $array[$k]['type'] = 1;
                $array[$k]['platform_id'] = $source_id;
                $array[$k]['create_time'] = date('Y-m-d H:i:s');
                $array[$k]['year'] = date('Y', strtotime($real_dayid));
                $array[$k]['month'] = date('m', strtotime($real_dayid));
            }

            // 保存错误信息
            if ($error_log_arr) {
                $error_msg_array = [];
                $error_msg_mail = [];
                $error_log_arr = Service::shield_error($source_id,$error_log_arr);

                if (isset($error_log_arr['campaign_id']) && !empty($error_log_arr['campaign_id'])) {
                    $campaign_id = implode(',', array_unique($error_log_arr['campaign_id']));
                    $error_msg_array[] = '应用ID匹配失败,ID为:' . $campaign_id;
                    $error_msg_mail[] = '应用ID匹配失败，ID为：' . $campaign_id;
                }

                if(!empty($error_msg_array)) {
                    DataImportImp::saveDataErrorLog(2, $source_id, $source_name, 1, implode(';', $error_msg_array));
                    // 发送邮件
//                    CommonFunction::sendMail($error_msg_mail, $source_name . '统计省份数据处理error');
                }
            }

            // 保存正确数据
            if ($array) {
                DB::beginTransaction();
                $map_delete = [];
                $map_delete['platform_id'] = $source_id;
                $map_delete['date'] = $real_dayid;
                $map_delete['type'] = 1;
                DataImportLogic::deleteMysqlHistoryData('zplay_user_tj_report_month', $map_delete);
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
                        $result = DataImportLogic::insertAdReportInfo('zplay_user_tj_report_month', $v);
                        if (!$result) {
                            DB::rollBack();
                            $is_success[] = $k;
                        }
                    }
                }
                DB::commit();

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