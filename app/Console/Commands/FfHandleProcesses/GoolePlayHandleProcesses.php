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
use function GuzzleHttp\Psr7\str;
use Illuminate\Console\Command;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\ApplicationLogic;
use App\BusinessLogic\CommonLogic;

class GoolePlayHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'GoolePlayHandleProcesses {dayid?} ';

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
        try {
            $source_id = 'pff02';
            $billing_name = 'GoolePlay';
            //临时表
            $mysql_table_name = 'zplay_ff_report_daily_temporary';
            //最终表
            $mysql_table = 'zplay_ff_report_daily';
            $dayid = $this->argument('dayid') ? $this->argument('dayid') : date('Ym');

            //月初月末时间戳
            $month = substr($dayid, 0, 4);//当前年月
            $day = substr($dayid, 4, 6);//当前年月
            $month_start = date('Y-m-01', strtotime($month . '-' . $day));//指定月份月初时间戳
            $month_end = date('Y-m-d', mktime(23, 59, 59, date('m', strtotime($month . '-' . $day)) + 1, 00));//指定月份月末时间戳

            //查询pgsql 的数据
            $map = [];
            $map['between'] = ['dayid', [$month_start, $month_end]];
            $map['type'] = 2;
            $map['source_id'] = $source_id;

            $info = DataImportLogic::getChannelData('ff_data', 'erm_data', $map)->get();
            $info = Service::data($info);

            if (!$info) {
//            $error_msg = $dayid.'号，goolePlay计费平台数据处理程序获取原始数据为空';
//            DataImportImp::saveDataErrorLog(2,$source_id,$billing_name,3,$error_msg);
                exit;
            }


            //获取匹配应用的数据
            $sql = "SELECT DISTINCT
            `c_billing_point`.`billing_point_name`,
            `c_billing_point`.`billing_point_id`,
            `c_billing_point`.`billing_point_price_usd`,
            `c_billing_point`.`billing_point_price_cny`,
            `c_billing_point`.`rebate_billing_point_price_usd`,
            `c_billing_point`.`rebate_begin_time`,
            `c_billing_point`.`currency_type`,
            `c_billing_point`.`rebate_end_time`,
            `c_billing`.`app_package_name`,
            `c_app`.`app_id`,
            `c_app`.`id`
            FROM
            `c_app`
            LEFT JOIN `c_billing` ON `c_billing`.`app_id` = `c_app`.`id`
            LEFT JOIN `c_billing_point` ON `c_billing_point`.`app_id` = `c_app`.`id`
            WHERE
            (
            `c_app`.os_id = 2
            AND  `c_billing`.`pay_platform_id` ='$source_id'
        )";
            $app_list = DB::select($sql);
            $app_list = Service::data($app_list);
            if (!$app_list) {
                $error_msg = '应用数据查询为空';
                DataImportImp::saveDataErrorLog(2, $source_id, $billing_name, 3, $error_msg);
                exit;
            }


            $sql = "SELECT
        `c_platform`.`bad_account_rate`,c_platform.currency_type_id,c_platform.platform_id, c_divide.*
        FROM
        c_platform
        LEFT JOIN c_divide ON `c_divide`.`app_channel_id` = `c_platform`.`id`
        AND `c_divide`.`type` = 3
        WHERE c_platform.`platform_id` ='$source_id'
        ORDER BY
        c_divide.effective_date DESC LIMIT 1";
            $divide = DB::select($sql);
            $divide = Service::data($divide);
            if (!$divide) {
                $error_msg = 'goolePlay计费平台数据处理程序平台数据查询为空';
                DataImportImp::saveDataErrorLog(2, $source_id, $billing_name, 3, $error_msg);
                exit;
            }


            $sql = 'select * from c_currency_type';
            $currency_info = db::select($sql);
            $currency_info = Service::data($currency_info);
            //获取对照表国家信息
            $country_map = [];
            $country_info = CommonLogic::getCountryList($country_map)->get();
            $country_info = Service::data($country_info);
            if (!$country_info) {
                $error_msg = 'goolePlay计费平台数据处理程序国家信息数据查询为空';
                DataImportImp::saveDataErrorLog(2, $source_id, $billing_name, 3, $error_msg);
                exit;
            }

            $array = [];
            $error_log_arr = [];
            $error_detail_arr = [];
            $num = 0;
            $num_country = 0;
            $num_currency = 0;
            //坏账 和分成
            if ($divide[0]['divide_billing']) {
                $divide_ad = floatval($divide[0]['divide_billing'] / 100);
            } else {
                $divide_ad = 1;
            }

            if ($divide[0]['bad_account_rate']) {
                $bad_account_rate = $divide[0]['bad_account_rate'] / 100;
            } else {
                $bad_account_rate = 0;
            }
            foreach ($info as $k => $v) {
                $json_info = json_decode($v['json_data'], true);
                //不等于Charged 就是推广
                $refund = '';
                if ($json_info['financial_status'] != 'Charged') {
                    $refund = '-';
                }
                foreach ($app_list as $app_k => $app_v) {

                    if ($json_info['product_id'] == $app_v['app_package_name'] && (str_replace('\\', '', $json_info['sku_id']) == $app_v['billing_point_id'] || $json_info['product_title'] == $app_v['billing_point_id']) && !is_null($app_v['billing_point_id'])) {
                        //获取汇率信息
                        $c_currency_type_map = [];
                        $c_currency_type_map['currency_id'] = $app_v['currency_type'];
                        $c_currency_type_map['effective_time'] = $dayid;
                        $c_currency_type_info = CommonLogic::getCurrencyEXList($c_currency_type_map)->first();

                        $c_currency_info = Service::data($c_currency_type_info);

                        if (!$c_currency_info) {
                            $error_msg = 'goolePlay计费平台数据处理程序汇率类型查询为空1' . $app_v['currency_type'];
                            DataImportImp::saveDataErrorLog(2, $source_id, $billing_name, 3, $error_msg);
                            $num++;
                            break;
                        }


                        $array[$k]['app_id'] = $app_v['app_id'];
                        $billing_point = $app_v['billing_point_price_usd'];
                        if ($v['dayid'] >= $app_v['rebate_begin_time'] && $v['dayid'] <= $app_v['rebate_end_time']) {
                            $billing_point = $app_v['rebate_billing_point_price_usd'];
                        }
                        $array[$k]['earning'] = $refund . $billing_point;
                        $array[$k]['earning_fix'] = $refund . $billing_point * $c_currency_info['currency_ex'];//流水人民币
                        $array[$k]['earning_divide_plat'] = $refund . $billing_point * $c_currency_info['currency_ex'] * $divide_ad;//流水平台分成
                        $array[$k]['earning_divide_plat_pay'] = $refund . $billing_point * $c_currency_info['currency_ex'] * $bad_account_rate;//流水坏账
                        $array[$k]['earning_divide_publisher'] = $refund . $billing_point * $c_currency_info['currency_ex'] * (1 - $divide_ad);

                        $array[$k]['income_plat'] = $refund . $billing_point * $c_currency_info['currency_ex'] * $divide_ad;
                        $array[$k]['income_publisher'] = $refund . $billing_point * $c_currency_info['currency_ex'] * (1 - $divide_ad);
                        $array[$k]['income_fix'] = $refund . $billing_point * $c_currency_info['currency_ex'] * (1 - $divide_ad);
                        $num = 0;
                        break;
                    } else {

                        //广告位配置未配置
                        $num++;

                    }

                }
                if ($num) {
                    foreach ($app_list as $app_k => $app_v) {
                        if ($json_info['product_id'] == $app_v['app_package_name']) {
                            foreach ($currency_info as $currency_k => $currency_v) {
                                $array[$k]['app_id'] = $app_v['app_id'];
                                if ($json_info['currency_of_sale'] == $currency_v['currency_en']) {
                                    $currency_id = $currency_v['id'];
                                    //获取平台汇率
                                    $ex_map['currency_id'] = $currency_v['id'];
                                    $ex_map['effective_time'] = $dayid;
                                    $ex_fields = ['currency_ex'];
                                    $ex_info = CommonLogic::getCurrencyEXList($ex_map, $ex_fields)->orderby('effective_time', 'desc')->first();
                                    $ex_info = Service::data($ex_info);


                                    if (!$ex_info) {
                                        $error_msg = 'googlePlay计费平台数据处理程序汇率数据查询为空2' . $json_info['currency_of_sale'];
                                        DataImportImp::saveDataErrorLog(2, $source_id, $billing_name, 3, $error_msg);
                                    }
                                    $num_currency = 0;
                                    break;
                                } else {
                                    //
                                    //汇率匹配失败
                                    $num_currency++;

                                }

                            }
                            $num = 0;
                            $array[$k]['earning'] = $json_info['charged_amount'] * $ex_info['currency_ex'];
                            $array[$k]['earning_fix'] = $json_info['charged_amount'] * $ex_info['currency_ex'];//流水人民币
                            $array[$k]['earning_divide_plat'] = $json_info['charged_amount'] * $ex_info['currency_ex'] * $divide_ad;//流水平台分成
                            $array[$k]['earning_divide_plat_pay'] = $json_info['charged_amount'] * $ex_info['currency_ex'] * $bad_account_rate;//流水坏账
                            $array[$k]['earning_divide_publisher'] = $json_info['charged_amount'] * $ex_info['currency_ex'] * (1 - $divide_ad);

                            $array[$k]['income_plat'] = $json_info['charged_amount'] * $ex_info['currency_ex'] * $divide_ad;
                            $array[$k]['income_publisher'] = $json_info['charged_amount'] * $ex_info['currency_ex'] * (1 - $divide_ad);
                            $array[$k]['income_fix'] = $json_info['charged_amount'] * $ex_info['currency_ex'] * (1 - $divide_ad);
                            break;
                        }
                    }
                    $err_name = (isset($json_info['sku_id']) ?$json_info['sku_id']:'Null').'#'.(isset($json_info['product_title']) ?$json_info['product_title']:'Null').'#'.(isset($json_info['parent_identifier']) ?$json_info['parent_identifier']:'Null').'#'.(isset($json_info['product_id']) ?$json_info['product_id']:'Null');

                    if ($num) {
                        $error_log_arr['app_id'][] = $json_info['product_id'] . '或' . $json_info['sku_id'].'('.$err_name.')';
                    }
                }
                $array[$k]['country_id'] = 16;
                $array[$k]['province_id'] = 16;
                foreach ($country_info as $country_k => $country_v) {

                    if ($json_info['city_of_buyer'] == $country_v['name']) {
                        $array[$k]['province_id'] = $country_v['c_country_id'];
                    }
                    if ($json_info['country_of_buyer'] == $country_v['name']) {
                        $array[$k]['country_id'] = $country_v['c_country_id'];
                        $num_country = 0;
                        break;
                    } else {
                        //
                        //国家配置失败
                        $num_country++;

                    }
                }
                if ($num_country) {
                    $error_log_arr['country'][] = isset($json_info['country_of_buyer']) ? $json_info['country_of_buyer'].'('.$err_name.')' : 'Unknown Region';
                }

                // foreach ($c_currency_type_info as $currency_k => $currency_v) {
                //     if($json_info['currency_of_sale'] ==$currency_v['currency_en']){
                //         $currency_id= $currency_v['id'];
                //         //获取平台汇率
                //      $ex_map['currency_id'] = $currency_v['id'];
                //      $ex_map['effective_time'] = date("Ym",strtotime($dayid));
                //      $ex_fields=['currency_ex'];
                //      $ex_info = CommonLogic::getCurrencyEXList($ex_map,$ex_fields)->orderby('effective_time','desc')->first();
                //      $ex_info = Service::data($ex_info);


                //         if(!$ex_info){
                //             $error_msg = '汇率数据查询为空'.$json_info['currency_of_sale'];
                //             DataImportImp::saveDataErrorLog(2,$source_id,$billing_name,3,$error_msg);
                //         }
                //         $num_currency = 0;
                //         break;
                //     }else{
                //        //
                //         //汇率匹配失败
                //         $num_currency++;

                //     }

                // }
                if ($num_currency) {
                    $error_log_arr['currency'][] = $json_info['currency_of_sale'].'('.$err_name.')';
                }
                //
                if (($num + $num_country + $num_currency) > 0) {

                    $error_detail_arr[$k]['platform_id'] = $source_id;
                    $error_detail_arr[$k]['platform_name'] = $billing_name;
                    $error_detail_arr[$k]['platform_type'] = 3;
                    $error_detail_arr[$k]['err_date'] = $v['dayid'];
                    $error_detail_arr[$k]['first_level_id'] = isset($json_info['product_id']) ? $json_info['product_id'] : '';
                    $error_detail_arr[$k]['first_level_name'] = '';
                    $error_detail_arr[$k]['second_level_id'] = isset($json_info['sku_id']) ? $json_info['sku_id'] : '';
                    $error_detail_arr[$k]['second_level_name'] = '';
                    $error_detail_arr[$k]['money'] = $refund ? floatval($refund) : 0; // 流水原币
                    $error_detail_arr[$k]['account'] = isset($v['account']) ? $v['account'] : '';
                    $error_detail_arr[$k]['create_time'] = date('Y-m-d H:i:s');

                    unset($array[$k]);
                    //插入错误数据
                    continue;
                }
                $array[$k]['date'] = $v['dayid'];
                $array[$k]['channel_id'] = 'cg001';
                $array[$k]['platform_id'] = $source_id;
                $array[$k]['platform_account'] = $v['account'];
                $array[$k]['publisher_id'] = 5;
                $array[$k]['pay_user'] = $refund . '1';
                $array[$k]['pay_time'] = $refund . '1';
                $array[$k]['pay_user_all'] = $refund . '1';
                $array[$k]['pay_time_all'] = $refund . '1';

                // if($divide[0]['divide_billing']){
                //     $divide_ad = floatval($divide[0]['divide_billing']/100);
                // }else{
                //     $divide_ad =1;
                // }
                // if($divide[0]['bad_account_rate']){
                //     $bad_account_rate = $divide[0]['bad_account_rate']/100;
                // }else{
                //     $bad_account_rate =0;
                // }

                // if(!$ex_info['currency_ex']){
                //     $ex_info['currency_ex'] =1;
                // }
                //    $array[$k]['earning'] = $json_info['charged_amount']*$ex_info['currency_ex'];
                //    $array[$k]['earning_fix'] =$json_info['charged_amount']*$ex_info['currency_ex'];//流水人民币
                // $array[$k]['earning_divide_plat'] = $json_info['charged_amount']*$ex_info['currency_ex']*$divide_ad;//流水平台分成
                //    $array[$k]['earning_divide_plat_pay'] = $json_info['charged_amount']*$ex_info['currency_ex']*$bad_account_rate;//流水坏账
                // $array[$k]['earning_divide_publisher'] = $json_info['charged_amount']*$ex_info['currency_ex']*(1-$divide_ad);

                // $array[$k]['income_plat'] = $json_info['charged_amount']*$ex_info['currency_ex']*$divide_ad;
                // $array[$k]['income_publisher'] = $json_info['charged_amount']*$ex_info['currency_ex']*(1-$divide_ad);
                // $array[$k]['income_fix'] =$json_info['charged_amount']*$ex_info['currency_ex']*(1-$divide_ad);
                $array[$k]['create_time'] = date('Y-m-d H:i:s');
                $array[$k]['update_time'] = date('Y-m-d H:i:s');

            }

            // 保存错误信息
            if ($error_log_arr) {
                $error_msg_array = [];
                $error_log_arr = Service::shield_error($source_id, $error_log_arr);

                if (isset($error_log_arr['app_id']) && !empty($error_log_arr['app_id'])) {
                    sort($error_log_arr['app_id']);
                    $app_id = implode(',', array_unique($error_log_arr['app_id']));
                    $error_msg_array[] = '应用id匹配失败,ID为:' . $app_id;
                }
                if (isset($error_log_arr['country']) && !empty($error_log_arr['country'])) {
                    sort($error_log_arr['country']);
                    $country = implode(',', array_unique($error_log_arr['country']));
                    $error_msg_array[] = '国家匹配失败,ID为:' . $country;
                }
                if (isset($error_log_arr['currency']) && !empty($error_log_arr['currency'])) {
                    sort($error_log_arr['currency']);
                    $currency = implode(',', array_unique($error_log_arr['currency']));
                    $error_msg_array[] = '汇率匹配失败,ID为:' . $currency;
                }
                if (!empty($error_msg_array)) {

                    DataImportImp::saveDataErrorLog(2, $source_id, $billing_name, 3, implode(';', $error_msg_array));
                }
                $diff_num = Service::diffBetweenTwoDays($month_start, $month_end);
                $date_arr = [];
                for ($d_i = 0; $d_i <= $diff_num; $d_i++) {
                    $date_arr[] = date("Y-m-d", strtotime("+$d_i days", strtotime($month_start)));
                }
                if ($date_arr && $error_detail_arr) {
                    foreach ($date_arr as $key => $value) {
                        $array_err = [];
                        foreach ($error_detail_arr as $k => $v) {
                            if ($v['err_date'] == $value) {
                                $array_err[$k] = $v;
                            }
                        }
                        DataImportImp::saveDataErrorMoneyLog($source_id, $value, $array_err);
                    }
                }


//            CommonFunction::sendMail($error_msg_array,$billing_name.'计费平台数据处理error');
            }

            if (!empty($array)) {
                DB::beginTransaction();
                $map_delete['platform_id'] = $source_id;
                $map_delete['between'] = ['date', [$month_start, $month_end]];
                DataImportLogic::deleteMysqlHistoryData($mysql_table, $map_delete);
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
                    // DataImportLogic::deleteMysqlHistoryData($mysql_table,$map_delete);
                    // $sql = "INSERT INTO zplay_ff_report_daily SELECT
                    // id,
                    // date,
                    // app_id,
                    // channel_id,
                    // country_id,
                    // province_id,
                    // platform_id,
                    // data_platform_id,
                    // platform_account,
                    // publisher_id,
                    // business_id,
                    // business_name,
                    // channel_id_plat,
                    // channel_name_plat,
                    // publisher_type,
                    // channel_type,
                    // business_divide,
                    // count(1) AS pay_user,
                    // count(1) AS pay_time,
                    // 0 AS pay_user_fail,
                    // 0 AS pay_time_fail,
                    // count(1) AS pay_user_all,
                    // count(1) AS pay_time_all,
                    // SUM(earning),
                    // SUM(earning_fix),
                    // SUM(earning_divide_plat),
                    // SUM(earning_divide_plat_z),
                    // SUM(earning_divide_plat_pay),
                    // SUM(earning_divide_publisher),
                    // SUM(earning_divide_channel),
                    // SUM(income_plat),
                    // SUM(income_publisher),
                    // SUM(income_channel),
                    // SUM(income_fix),
                    // tongji_type,
                    // pay_type,
                    // remark,
                    // now(),
                    // now()
                    // FROM
                    // zplay_ff_report_daily_temporary
                    // WHERE
                    // date = '$dayid'
                    // AND platform_id = '$source_id'
                    // GROUP BY
                    // date,
                    // app_id,
                    // country_id,
                    // platform_id,
                    // platform_account";
                    // DB::insert($sql);
                }
                DB::commit();
                // 调用存储过程更新总表数据
//            DB::update("call ff_summary('$month_start','$month_end','$source_id')");
                Artisan::call('FfSummaryProcesses', ['begin_date' => $month_start, 'end_date' => $source_id, 'platform_id' => $source_id]);
                // 查询广告数据
                $report_map = [];
                $report_map['platform_id'] = $source_id;
                $report_map['between'] = ['date', [$month_start, $month_end]];
                $group_by = ['platform_id', 'date', 'platform_account'];
                $report_list = PlatformLogic::getAdReportSum($mysql_table, $report_map)->select(DB::raw("sum(income_fix) as cost"), 'platform_id', 'date', 'platform_account')->groupBy($group_by)->get();
                $report_list = Service::data($report_list);

                if ($report_list) {
                    // 保存广告平台
                    foreach ($report_list as $value) {
                        PlatformImp::add_platform_status($source_id, $value['platform_account'], $value['cost'], $value['date']);
                    }
                }
                echo '处理完成';
            } else {
                echo '暂无处理数据';
            }
        }catch (\Exception $e) {
            $error_msg_info = $dayid.'号,'.$billing_name.'付费平台程序失败，失败原因：'.$e->getMessage();
            DataImportImp::saveDataErrorLog(5,$source_id,$billing_name,2,$error_msg_info);

        }

    }
}