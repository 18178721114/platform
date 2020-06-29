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

class BreakHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'BreakHandleProcesses {dayid?} {data_account?}';

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
        define('MYSQL_TABLE_NAME','zplay_tg_report_daily');
        $dayid = $this->argument('dayid') ? $this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));
        $time = strtotime($dayid);

        try {
            //查询pgsql 的数据
            $sql = "select * from channel_amount WHERE intime = '$time' and active_count    != 0";
            $info = DB::connection('mysql_channel')->select($sql);
            $info = Service::data($info);
            if (!$info) {
//            echo $error_msg = $dayid.'号,越狱渠道数据处理程序获取原始数据为空';
                //DataImportImp::saveDataErrorLog(2,$source_id,$source_name,4,$error_msg);
                exit;
            }


            //获取匹配应用的数据
            $sql = "SELECT DISTINCT
         c_app.id,
         c_app.app_id,
         c_generalize.platform_id,
         c_generalize.data_account,
         c_generalize.application_id,
         c_generalize.application_name,
         c_generalize.agency_platform_id,
         c_generalize_ad_app.campaign_id,
         c_generalize_ad_app.campaign_name,
         c_generalize_ad_app.ad_group_id,
         c_platform.currency_type_id,
         cpp.currency_type_id AS ageccy_currency_type_id,
         c_generalize_ad_app.generalize_price,
         c_generalize_ad_app.generalize_time,
         channel.channel_id as break_platform_id,
         channel.new_channel_id ,
         application.application_id as break_c_app_id
         FROM
         c_app
         LEFT JOIN c_generalize ON c_app.id = c_generalize.app_id
         LEFT JOIN c_generalize_ad_app ON c_generalize.id = c_generalize_ad_app.generalize_id
         LEFT JOIN c_platform ON c_generalize.platform_id = c_platform.platform_id
         LEFT JOIN c_platform AS cpp ON c_generalize.agency_platform_id = cpp.platform_id
         LEFT JOIN channel AS channel ON channel.c_channel_id = c_generalize.platform_id
         LEFT JOIN application AS application ON application.app_id = c_app.id
         WHERE
         c_app.id in (SELECT DISTINCT app_id from application) and channel.channel_id is not null  ORDER BY c_generalize.platform_id";

            $app_list = DB::select($sql);
            $app_list = Service::data($app_list);

            // 获取美元汇率
            $effective_time = date("Ym", strtotime($dayid));
            $usd_ex_info = DataImportImp::getPlatformExchangeRate($effective_time);
            $usd_currency_ex = 0;
            if ($usd_ex_info) {
                $usd_currency_ex = $usd_ex_info['currency_ex'];
            }

            if (!$app_list) {
                $error_msg = '越狱渠道数据处理程序应用数据查询为空';
                DataImportImp::saveDataErrorLog(2, 'ptg-000', '越狱渠道', 4, $error_msg);
                exit;
            }

            // //获取对照表国家信息
            // $country_map =[];
            // $country_info = CommonLogic::getCountryList($country_map)->get();
            // $country_info = Service::data($country_info);
            // if(!$country_info){
            //     $error_msg = '国家信息数据查询为空';
            //     DataImportImp::saveDataErrorLog(2,$source_id,$source_name,4,$error_msg);
            //     exit;
            // }

            $array = [];
            $num = 0;
            $num_country = 0;
            $error_log_arr = [];
            foreach ($info as $k => $v) {
                foreach ($app_list as $app_k => $app_v) {
                    if (($v['channel_id'] == $app_v['break_platform_id']) && ($v['application_id'] == $app_v['break_c_app_id'])) {

                        $generalize_price = $app_v['generalize_price'];
                        $time_now = date('Ymd', $v['intime']);

                        $generalize_time = $app_v['generalize_time'];
                        if ($generalize_time) {
                            $generalize_time = explode('-', $generalize_time);
                            $generalize_start_time = $generalize_time[0];
                            $generalize_end_time = $generalize_time[1];
                            if ($time_now < $generalize_start_time || $time_now > $generalize_end_time) {
                                $generalize_price = 0;
                            }
                        }
                        $array[$k]['app_id'] = $app_v['app_id'];
                        $array[$k]['channel_id'] = $app_v['new_channel_id'];
                        $array[$k]['platform_id'] = $app_v['platform_id'];
                        $array[$k]['data_account'] = $app_v['data_account'];
                        $array[$k]['platform_account'] = $app_v['data_account'];
                        $array[$k]['new'] = $v['active_count'];
                        $array[$k]['click'] = $v['day_count'];
                        $array[$k]['type'] = 2;
                        $array[$k]['cost'] = $v['active_count'] * $generalize_price; // 流水原币
                        $array[$k]['cost_exc'] = $v['active_count'] * $generalize_price; // 流水原币

                        // 成本美元
                        if (($array[$k]['cost'] == $array[$k]['cost_exc']) && $usd_currency_ex) {
                            $array[$k]['cost_usd'] = $array[$k]['cost_exc'] / $usd_currency_ex;
                        } else {
                            $array[$k]['cost_usd'] = $array[$k]['cost'];
                        }

                        $num = 0;
                        break;
                    } else {
                        //广告位配置未配置
                        $num++;

                    }
                }

                if ($num) {
                    $error_log_arr['channel_id'][] = $v['channel_id'] . '或应用id匹配失败' . $v['application_id'];
                }

                //var_dump($app_list);die;
                $array[$k]['country_id'] = 64;

                if (($num + $num_country) > 0) {
                    unset($array[$k]);
                    //插入错误数据
                    continue;
                }

                $array[$k]['date'] = $dayid;
                $array[$k]['create_time'] = date('Y-m-d H:i:s');
                $array[$k]['update_time'] = date('Y-m-d H:i:s');
            }

            // 保存错误信息
            if ($error_log_arr) {
                $error_msg_array = [];
                $error_msg_mail = [];
                if (isset($error_log_arr['channel_id'])) {
                    $channel_id = implode(',', array_unique($error_log_arr['channel_id']));
                    $error_msg_array[] = 'ID匹配失败,ID为:' . $channel_id;
                    $error_msg_mail[] = '平台ID匹配失败，ID为：' . $channel_id;
                }

                DataImportImp::saveDataErrorLog(2, 'ptg-000', '越狱渠道', 4, implode(';', $error_msg_array));
                // // 发送邮件
                CommonFunction::sendMail($error_msg_mail, '越狱渠道数据处理error');
            }

            // 保存正确数据
            if ($array) {
                //var_dump($array);die;
                $del_sql = "delete from " . MYSQL_TABLE_NAME . " where platform_id in (SELECT c_channel_id from channel WHERE c_channel_id !='') and date ='$dayid' ";
                DB::delete($del_sql);
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
                $platform_id_array = array_unique(array_column($array, 'platform_id'));
                // 调用存储过程更新总表数据
                if ($platform_id_array) {
                    foreach ($platform_id_array as $platform_id) {
                        Artisan::call('TgSummaryProcesses', ['begin_date' => $dayid, 'end_date' => $dayid, 'platform_id' => $platform_id]);
                        //DB::update("call tg_summary('$dayid','$dayid','$platform_id')");
                    }
                }

                // 查询广告数
                $sql = "select sum(cost) as cost,platform_id,date,data_account from  " . MYSQL_TABLE_NAME . " where date ='$dayid' and platform_id in (SELECT c_channel_id from channel WHERE c_channel_id !='') group by platform_id,date ";
                $report_list = DB::select($sql);
                $report_list = Service::data($report_list);
                if ($report_list) {
                    // 保存广告平台
                    foreach ($report_list as $value) {
                        if ($value['data_account']) {
                            PlatformImp::add_platform_status($value['platform_id'], $value['data_account'], $value['cost'], $dayid);
                        }
                    }
                }
            }
        }catch (\Exception $e) {
            // 异常报错
            $message = "{$dayid}号,越狱渠道程序报错,报错原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, 'ptg-000', '越狱渠道', 4, $message);
            $error_msg_arr[] = $message;
            CommonFunction::sendMail($error_msg_arr, '推广平台程序error');
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