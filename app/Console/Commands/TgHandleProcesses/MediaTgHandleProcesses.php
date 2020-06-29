<?php

namespace App\Console\Commands\TgHandleProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\BusinessLogic\DataImportLogic;
use App\Common\ApiResponseFactory;
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

class MediaTgHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'MediaTgHandleProcesses {dayid?} {data_account?}';

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
        $source_id = 'ptg279';
        $source_name = 'MediaTg';

        $dayid =  $this->argument('dayid');
//        var_dump($source_name.'-'.$source_id.'-'.$dayid);


        self::MediaTgDataProcess($dayid,$source_id,$source_name);
    }

    private static function MediaTgDataProcess($dayid,$source_id,$source_name){
        try {
            //查询pgsql 的数据
            $map = [];
            $map['in'] = ['dayid', $dayid];
            $map['type'] = 1;
            $map['source_id'] = $source_id;
            //$map[] =['cost','<>',0] ;
            $info = DataImportLogic::getChannelData('tg_data', 'erm_data', $map)->get();
            $info = Service::data($info);
            if (!$info) {
                $error_msg = $dayid . '号，' . $source_name . '推广平台数据处理程序获取原始数据为空';
                DataImportImp::saveDataErrorLog(2, $source_id, $source_name, 4, $error_msg);
                exit;
            }

            //获取匹配应用的数据
            $sql = "SELECT  distinct
                c_app.id,c_app.app_id,c_generalize.platform_id,c_generalize.data_account,c_generalize.application_id,c_generalize.application_name,c_generalize.agency_platform_id,c_generalize_ad_app.campaign_id,c_generalize_ad_app.campaign_name,c_generalize_ad_app.ad_group_id,c_platform.currency_type_id,cpp.currency_type_id as ageccy_currency_type_id
                FROM c_app 
                LEFT JOIN c_generalize ON c_app.id = c_generalize.app_id  and c_generalize.generalize_status = 1
                LEFT JOIN c_generalize_ad_app ON c_generalize.id = c_generalize_ad_app.generalize_id and  c_generalize_ad_app.status = 1
                LEFT JOIN c_platform ON c_generalize.platform_id = c_platform.platform_id 
                LEFT JOIN c_platform as cpp ON c_generalize.agency_platform_id = cpp.platform_id 
                WHERE 
                c_generalize.platform_id = '$source_id'";

            $app_list = DB::select($sql);
            $app_list = Service::data($app_list);


            if (!$app_list) {
                $error_msg = $source_name . '推广平台数据处理程序应用数据查询为空';
                DataImportImp::saveDataErrorLog(2, $source_id, $source_name, 4, $error_msg);
                exit;
            }


            // 获取美元汇率
            $usd_currency_date = [];
            $usd_currency_arr = [];
            if ($dayid) {
                foreach ($dayid as $date_time) {
                    $usd_currency_date[] = date("Ym", strtotime($date_time));
                }
                $usd_currency_date = array_unique($usd_currency_date);
                $usd_ex_info = DataImportImp::getPlatformExchangeRate($usd_currency_date);
                if ($usd_ex_info) {
                    foreach ($usd_ex_info as $usd_ex) {
                        $usd_currency_arr[$usd_ex['effective_time']] = $usd_ex['currency_ex'];
                    }
                }
            }


            $array = [];
            $num = 0;
            $num_country = 0;
            $error_log_arr = [];
            $error_detail_arr = [];
            try {
                foreach ($info as $k => $v) {
                    $json_info = json_decode($v['json_data'], true);
                    $err_name = (isset($json_info['campaign_id']) ? $json_info['campaign_id'] : 'Null') . '#' . (isset($json_info['campaign_name']) ? addslashes($json_info['campaign_name']) : 'Null') . '#' . (isset($json_info['app_id']) ? $json_info['app_id'] : 'Null') . '#' . (isset($json_info['app_name']) ? $json_info['app_name'] : 'Null');
                    foreach ($app_list as $app_k => $app_v) {
                        if (isset($json_info['app_id']) && ($json_info['app_id'] == $app_v['application_id']) && ($app_v['data_account'] == $json_info['account'])) {
                            $array[$k]['app_id'] = $app_v['app_id'];
                            $array[$k]['platform_account'] = $app_v['data_account'];
                            $array[$k]['agency_platform_id'] = $app_v['agency_platform_id'];

                            //获取平台的汇率
                            $ex_map['currency_id'] = $app_list[0]['currency_type_id'];
                            if ($app_v['ageccy_currency_type_id']) {
                                $ex_map['currency_id'] = $app_list[0]['ageccy_currency_type_id'];

                            }
                            $ex_map['effective_time'] = date("Ym", strtotime($json_info['date_time']));
                            $ex_fields = ['currency_ex'];
                            $ex_info = CommonLogic::getCurrencyEXList($ex_map, $ex_fields)->orderby('effective_time', 'desc')->first();
                            $ex_info = Service::data($ex_info);
                            if (!$ex_info) {
                                $error_msg = $source_name . '推广平台数据处理程序汇率数据查询为空';
                                DataImportImp::saveDataErrorLog(2, $source_id, $source_name, 4, $error_msg);
                            }

                            $num = 0;
                            break;
                        } else {
                            //广告位配置未配置
                            $num++;

                        }
                    }

                    if ($num) {
                        $error_log_arr['campaign_id'][] = $json_info['application_id'] . '(' . $err_name . ')';
                    }


                    if (($num + $num_country) > 0) {

                        $error_detail_arr[$k]['platform_id'] = $source_id;
                        $error_detail_arr[$k]['platform_name'] = $source_name;
                        $error_detail_arr[$k]['platform_type'] = 4;
                        $error_detail_arr[$k]['err_date'] = $dayid;
                        $error_detail_arr[$k]['first_level_id'] = $json_info['app_id'];
                        $error_detail_arr[$k]['first_level_name'] = $json_info['app_name'];
                        $error_detail_arr[$k]['second_level_id'] = '';//$err_campaign_id;
                        $error_detail_arr[$k]['second_level_name'] = '';
                        $error_detail_arr[$k]['money'] = isset($json_info['money']) ? $json_info['money'] : 0.00; // 流水原币
                        $error_detail_arr[$k]['account'] = isset($json_info['account']) ? $json_info['account'] : '';
                        $error_detail_arr[$k]['create_time'] = date('Y-m-d H:i:s');

                        unset($array[$k]);
                        //插入错误数据
                        continue;
                    }

                    $array[$k]['data_account'] = $json_info['account'];
                    $array[$k]['date'] = $json_info['date_time'];

                    $array[$k]['platform_app_id'] = isset($json_info['app_id']) ? addslashes($json_info['app_id']) : '';
                    $array[$k]['platform_app_name'] = isset($json_info['app_name']) ? addslashes(str_replace('\'\'', '\'', $json_info['app_name'])) : '';

                    $array[$k]['ad_id'] = '';
                    $array[$k]['ad_name'] = '';
                    $array[$k]['country_id'] = 1;
                    $array[$k]['impression'] = 0;
                    $array[$k]['click'] = 0;
                    $array[$k]['new'] = 0;
                    $array[$k]['cost'] = isset($json_info['money']) ? $json_info['money'] : 0.00; // 流水原币
                    $currency_ex = floatval($ex_info['currency_ex']);
                    if (!$currency_ex) {
                        $currency_ex = 1;
                    }
                    $array[$k]['cost_exc'] = isset($json_info['money']) ? $json_info['money'] * $currency_ex : 0;
                    $usd_currency_ex = 0;
                    if ($usd_currency_arr) {
                        foreach ($usd_currency_arr as $usd_currency_date => $usd_currency) {
                            if ($usd_currency_date == date("Ym", strtotime($json_info['date_time']))) {
                                $usd_currency_ex = $usd_currency;
                            }
                        }
                    }


                    // 成本美元
                    if (($array[$k]['cost'] == $array[$k]['cost_exc']) && $usd_currency_ex) {
                        $array[$k]['cost_usd'] = $array[$k]['cost_exc'] / $usd_currency_ex;
                    } else {
                        $array[$k]['cost_usd'] = $array[$k]['cost'];
                    }

                    $array[$k]['platform_id'] = $source_id;
                    $array[$k]['create_time'] = date('Y-m-d H:i:s');
                    $array[$k]['update_time'] = date('Y-m-d H:i:s');
                }
            } catch (\Exception $e) {
                $error_msg_info = $source_name . '渠道数据匹配失败：' . $e->getMessage();
                ApiResponseFactory::apiResponse([], [], '', $error_msg_info);
            }


            // 保存错误信息
            if ($error_log_arr) {
                $error_msg_array = [];
                $error_msg_mail = [];
                $error_log_arr = Service::shield_error($source_id, $error_log_arr);

                if (isset($error_log_arr['campaign_id'])) {
                    $campaign_id = implode(',', array_unique($error_log_arr['campaign_id']));
                    $error_msg_array[] = '应用id匹配失败,ID为:' . $campaign_id;
                    $error_msg_mail[] = '应用id匹配失败，ID为：' . $campaign_id;
                }


                if (!empty($error_msg_array)) {
                    DataImportImp::saveDataErrorLog(2, $source_id, $source_name, 4, implode(';', $error_msg_array));
                    // 发送邮件
//                CommonFunction::sendMail($error_msg_mail,$source_name.'推广平台数据处理error');
                }
                DataImportImp::saveDataErrorMoneyLog($source_id, $dayid, $error_detail_arr);

            }

            // 保存正确数据
            if ($array) {
                $time = date('Y-m-d H:i:s');
                foreach ($dayid as $key => $value) {
                    $sql_str = '';
                    $plat_str = '';
                    foreach ($array as $k => $v) {
                        if ($v['date'] == $value) {
                            $plat_str = $source_id . 'lishuyang@lishuyang' . $value;
                            $sql_str .= "('" . $v['date'] . "'," // date
                                . "'" . $v['app_id'] . "',"  //app_id
                                . "'',"//channel_id
                                . "'" . $v['country_id'] . "',"//country_id
                                . "'" . $v['platform_id'] . "',"//platform_id
                                . "'" . $v['agency_platform_id'] . "',"//agency_platform_id
                                . "'',"//data_platform_id
                                . "'1',"//type
                                . "'" . $v['platform_account'] . "',"//platform_account
                                . "'" . $v['data_account'] . "',"//data_account
                                . "'1',"//cost_type
                                . "'" . $v['platform_app_id'] . "',"//platform_app_id
                                . "'" . $v['platform_app_name'] . "',"//platform_app_name
                                . "'" . $v['ad_id'] . "',"//ad_id
                                . "'" . $v['ad_name'] . "',"//ad_name
                                . "'',"//ad_type
                                . "'',"//tongji_type;
                                . "'" . $v['impression'] . "',"//impression;
                                . "'" . $v['click'] . "',"//click;
                                . "'" . $v['new'] . "',"//new;
                                . "'',"//new_phone;
                                . "'',"//new_pad;
                                . "'" . $v['cost'] . "',"//cost;
                                . "'" . $v['cost_exc'] . "',"//cost_exc;
                                . "'',"//device_type;
                                . "'',"//remark;
                                . "'" . $time . "',"//create_time
                                . "'" . $time . "',"//update_time
                                . "'" . $v['cost_usd'] . "'),";//cost_usd
                        }

                    }
                    if ($sql_str != '' && $plat_str != '') {
                        Redis::rpush(env('REDIS_TG_KEYS'), $plat_str);
                        $sql_str = rtrim($sql_str, ',');
                        Redis::rpush(env('REDIS_TG_KEYS'), $sql_str);
                    }
                }

            }
        }catch (\Exception $e) {
            // 异常报错
            $message = "{$dayid}号, " . AD_PLATFORM . " 推广平台程序报错,报错原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, SOURCE_ID, AD_PLATFORM, 4, $message);
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