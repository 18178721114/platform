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

class AppStoreHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'AppStoreHandleProcesses {dayid?} ';

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

        try {
            echo '<pre>';
            set_time_limit(0);
            $source_id = 'pff03';
            $billing_name = 'AppStore';
            //临时表
            $mysql_table_name = 'zplay_ff_report_daily_temporary';
            //最终表
            $mysql_table = 'zplay_ff_report_daily';
            $dayid = $this->argument('dayid') ? $this->argument('dayid') : date('Y-m-d', strtotime('-1 day'));
            //查询pgsql 的数据
            $map = [];
            $map['dayid'] = $dayid;
            $map['type'] = 2;
            $map['source_id'] = $source_id;
            $info = DataImportLogic::getChannelData('ff_data', 'erm_data', $map)->get();
            $info = Service::data($info);
            var_dump(count($info));
            if (!$info) {
//            $error_msg = $dayid.'号，AppStore计费平台数据处理程序获取原始数据为空';
//            echo $error_msg;
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
            `c_billing_point`.`currency_type`,
            `c_billing_point`.`rebate_begin_time`,
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
            `c_app`.os_id = 1
            AND  `c_billing`.`pay_platform_id` ='$source_id'
        )";

            $app_list = DB::select($sql);
            $app_list = Service::data($app_list);
            if (!$app_list) {
                $error_msg = 'AppStore计费平台数据处理程序应用数据查询为空';
                DataImportImp::saveDataErrorLog(2, $source_id, $billing_name, 3, $error_msg);
                exit;
            }


            $sql = "        SELECT
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
                $error_msg = 'AppStore计费平台数据处理程序平台数据查询为空';
                DataImportImp::saveDataErrorLog(2, $source_id, $billing_name, 3, $error_msg);
                exit;
            }



            //获取对照表国家信息
            // $c_currency_type_map =[];
            // $c_currency_type_info = CommonLogic::getCurrencyType($c_currency_type_map)->get();
            // $c_currency_type_info = Service::data($c_currency_type_info);
            // if(!$c_currency_type_info){
            //     $error_msg = '汇率类型查询为空';
            //     DataImportImp::saveDataErrorLog(2,$source_id,$billing_name,3,$error_msg);
            //     exit;
            // }
            //
            //获取对照表国家信息
            // $c_currency_type_map =[];
            // $c_currency_type_map['currency_id'] =60;
            // $c_currency_type_map['effective_time']  = date("Ym",strtotime($dayid));
            // $c_currency_type_info = CommonLogic::getCurrencyEXList($c_currency_type_map)->first();
            // $c_currency_info = Service::data($c_currency_type_info);
            // if(!$c_currency_info){
            //     $error_msg = 'AppStore计费平台数据处理程序汇率类型查询为空';
            //     DataImportImp::saveDataErrorLog(2,$source_id,$billing_name,3,$error_msg);
            //     exit;
            // }

            $sql = 'select * from c_currency_type';
            $currency_info = db::select($sql);
            $currency_info = Service::data($currency_info);


            //获取对照表国家信息
            $country_map = [];
            $country_info = CommonLogic::getCountryList($country_map)->get();
            $country_info = Service::data($country_info);
            if (!$country_info) {
                $error_msg = 'AppStore计费平台数据处理程序国家信息数据查询为空';
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
                if ($json_info['developer_proceeds'] == '0.00') {
                    continue;
                }

                foreach ($app_list as $app_k => $app_v) {
                    if (empty($json_info['parent_identifier']) || $json_info['parent_identifier'] == ' ') {
                        if ($json_info['sku'] == $app_v['app_package_name'] && $json_info['sku'] == $app_v['billing_point_id']) {
                            //获取对照表国家信息
                            $c_currency_type_map = [];
                            $c_currency_type_map['currency_id'] = $app_v['currency_type'];
                            $c_currency_type_map['effective_time'] = date("Ym", strtotime($dayid));
                            $c_currency_type_info = CommonLogic::getCurrencyEXList($c_currency_type_map)->first();
                            $c_currency_info = Service::data($c_currency_type_info);
                            if (!$c_currency_info) {
                                $error_msg = 'goolePlay计费平台数据处理程序汇率类型查询为空';
                                DataImportImp::saveDataErrorLog(2, $source_id, $billing_name, 3, $error_msg);
                                $num++;
                                break;
                            }
                            $array[$k]['app_id'] = $app_v['app_id'];
                            $billing_point = $app_v['billing_point_price_usd'];
                            if ($v['dayid'] >= $app_v['rebate_begin_time'] && $v['dayid'] <= $app_v['rebate_end_time']) {
                                $billing_point = $app_v['rebate_billing_point_price_usd'];
                            }
                            $array[$k]['earning'] = $billing_point * $json_info['units'];
                            $array[$k]['earning_fix'] = $billing_point * $c_currency_info['currency_ex'] * $json_info['units'];//流水人民币
                            $array[$k]['earning_divide_plat'] = $divide_ad * ($billing_point + 0.01) * $c_currency_info['currency_ex'] * $json_info['units'];//流水平台分成
                            $array[$k]['earning_divide_plat_pay'] = $billing_point * $c_currency_info['currency_ex'] * $bad_account_rate * $json_info['units'];//流水坏账
                            $array[$k]['earning_divide_publisher'] = $billing_point * $c_currency_info['currency_ex'] * $json_info['units'];

                            $array[$k]['income_plat'] = $divide_ad * ($billing_point + 0.01) * $c_currency_info['currency_ex'] * $json_info['units'];
                            $array[$k]['income_publisher'] = (1 - $divide_ad) * ($billing_point + 0.01) * $c_currency_info['currency_ex'] * $json_info['units'];
                            $array[$k]['income_fix'] = (1 - $divide_ad) * ($billing_point + 0.01) * $c_currency_info['currency_ex'] * $json_info['units'];
                            $num = 0;
                            break;
                        } else {
                            $num++;

                        }

                    } else {
                        if ($json_info['parent_identifier'] == $app_v['app_package_name'] && $json_info['sku'] == $app_v['billing_point_id']) {
                            //获取对照表国家信息
                            $c_currency_type_map = [];
                            $c_currency_type_map['currency_id'] = $app_v['currency_type'];
                            $c_currency_type_map['effective_time'] = date("Ym", strtotime($dayid));
                            $c_currency_type_info = CommonLogic::getCurrencyEXList($c_currency_type_map)->first();
                            $c_currency_info = Service::data($c_currency_type_info);
                            if (!$c_currency_info) {
                                $error_msg = 'goolePlay计费平台数据处理程序汇率类型查询为空';
                                DataImportImp::saveDataErrorLog(2, $source_id, $billing_name, 3, $error_msg);
                                $num++;
                                break;
                            }
                            $array[$k]['app_id'] = $app_v['app_id'];
                            $billing_point = $app_v['billing_point_price_usd'];


                            if ($v['dayid'] >= $app_v['rebate_begin_time'] && $v['dayid'] <= $app_v['rebate_end_time']) {
                                $billing_point = $app_v['rebate_billing_point_price_usd'];
                            }
                            $array[$k]['earning'] = $billing_point * $json_info['units'];
                            $array[$k]['earning_fix'] = $billing_point * $c_currency_info['currency_ex'] * $json_info['units'];//流水人民币
                            $array[$k]['earning_divide_plat'] = $divide_ad * ($billing_point + 0.01) * $c_currency_info['currency_ex'] * $json_info['units'];//流水平台分成
                            $array[$k]['earning_divide_plat_pay'] = $billing_point * $c_currency_info['currency_ex'] * $bad_account_rate * $json_info['units'];//流水坏账
                            $array[$k]['earning_divide_publisher'] = $billing_point * $c_currency_info['currency_ex'] * $json_info['units'];

                            $array[$k]['income_plat'] = $divide_ad * ($billing_point + 0.01) * $c_currency_info['currency_ex'] * $json_info['units'];
                            $array[$k]['income_publisher'] = (1 - $divide_ad) * ($billing_point + 0.01) * $c_currency_info['currency_ex'] * $json_info['units'];
                            $array[$k]['income_fix'] = (1 - $divide_ad) * ($billing_point + 0.01) * $c_currency_info['currency_ex'] * $json_info['units'];
                            $num = 0;
                            break;
                        } else {

                            $num++;

                        }

                    }


                }
                if ($num) {
                    foreach ($app_list as $app_k => $app_v) {
                        if (empty($json_info['parent_identifier']) || $json_info['parent_identifier'] == ' ') {
                            if ($json_info['sku'] == $app_v['app_package_name']) {
                                $array[$k]['app_id'] = $app_v['app_id'];
                                $num = 0;
                                break;
                            } else {
                                $num++;

                            }

                        } else {
                            if ($json_info['parent_identifier'] == $app_v['app_package_name']) {
                                $array[$k]['app_id'] = $app_v['app_id'];
                                $num = 0;
                                break;
                            } else {

                                $num++;

                            }

                        }


                    }
                    foreach ($currency_info as $currency_k => $currency_v) {
                        if ($json_info['currency_of_proceeds'] == $currency_v['currency_en']) {
                            $currency_id = $currency_v['id'];
                            //获取平台汇率
                            $ex_map['currency_id'] = $currency_v['id'];
                            $ex_map['effective_time'] = date("Ym", strtotime($dayid));
                            $ex_fields = ['currency_ex'];
                            $ex_info = CommonLogic::getCurrencyEXList($ex_map, $ex_fields)->orderby('effective_time', 'desc')->first();
                            $ex_info = Service::data($ex_info);

                            if (!$ex_info) {
                                var_dump($ex_map);
                                $error_msg = '汇率数据查询为空' . $json_info['currency_of_proceeds'] . '--';
                                DataImportImp::saveDataErrorLog(2, $source_id, $billing_name, 3, $error_msg);
                                // exit;
                            }
                            $num_currency = 0;
                            break;
                        } else {
                            //
                            //汇率匹配失败
                            $num_currency++;

                        }

                    }
                    $array[$k]['earning'] = $json_info['customer_price'] * $ex_info['currency_ex'] * $json_info['units'];
                    $array[$k]['earning_fix'] = $json_info['customer_price'] * $ex_info['currency_ex'] * $json_info['units'];//流水人民币
                    $array[$k]['earning_divide_plat'] = $json_info['customer_price'] * $ex_info['currency_ex'] * $json_info['units'];//流水平台分成
                    $array[$k]['earning_divide_plat_pay'] = $json_info['customer_price'] * $ex_info['currency_ex'] * $bad_account_rate * $json_info['units'];//流水坏账
                    $array[$k]['earning_divide_publisher'] = $json_info['customer_price'] * $ex_info['currency_ex'] * $json_info['units'];

                    $array[$k]['income_plat'] = $json_info['developer_proceeds'] * $ex_info['currency_ex'] * $json_info['units'];
                    $array[$k]['income_publisher'] = $json_info['developer_proceeds'] * $ex_info['currency_ex'] * $json_info['units'];
                    $array[$k]['income_fix'] = $json_info['developer_proceeds'] * $ex_info['currency_ex'] * $json_info['units'];
                    $array[$k]['times'] = $json_info['product_type_identifier'];
                    $err_name = (isset($json_info['sku']) ?$json_info['sku']:'Null').'#Null#'.(isset($json_info['parent_identifier']) ?$json_info['parent_identifier']:'Null').'#'.(isset($json_info['code_name']) ?$json_info['code_name']:'Null');
                    if ($num) {
                        if ($json_info['parent_identifier'] == ' ') {
                            $error_log_arr['app_id'][] = $json_info['sku'];
                        } else {
                            $error_log_arr['app_id'][] = $json_info['parent_identifier'] . '或' . $json_info['sku'].'('.$err_name.')';
                        }
                    }
                    //$error_log_arr['app_id'][] = empty($json_info['parent_identifier'])?$json_info['sku']:$json_info['parent_identifier'];
                }
                $array[$k]['country_id'] = 16;
                foreach ($country_info as $country_k => $country_v) {


                    if ($json_info['country_code'] == $country_v['name']) {
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
                    $error_log_arr['country'][] = isset($json_info['country_code']) ? $json_info['country_code'].'('.$err_name.')' : 'Unknown Region';
                }

                // foreach ($c_currency_type_info as $currency_k => $currency_v) {
                //     if($json_info['currency_of_proceeds'] ==$currency_v['currency_en']){
                //      $currency_id= $currency_v['id'];
                //         //获取平台汇率
                //      $ex_map['currency_id'] = $currency_v['id'];
                //      $ex_map['effective_time'] = date("Ym",strtotime($dayid));
                //      $ex_fields=['currency_ex'];
                //      $ex_info = CommonLogic::getCurrencyEXList($ex_map,$ex_fields)->orderby('effective_time','desc')->first();
                //      $ex_info = Service::data($ex_info);

                //      if(!$ex_info){
                //         $error_msg = '汇率数据查询为空'.$json_info['currency_of_proceeds'].'--';
                //         DataImportImp::saveDataErrorLog(2,$source_id,$billing_name,3,$error_msg);
                //          // exit;
                //     }
                //         $num_currency = 0;
                //         break;
                //     }else{
                //        //
                //         //汇率匹配失败
                //         $num_currency++;

                //     }

                // }
                if ($num_currency) {
                    $error_log_arr['currency'][] = $json_info['currency_of_proceeds'].'('.$err_name.')';
                }
                if (($num + $num_country + $num_currency) > 0) {

                    $error_detail_arr[$k]['platform_id'] = $source_id;
                    $error_detail_arr[$k]['platform_name'] = $billing_name;
                    $error_detail_arr[$k]['platform_type'] = 3;
                    $error_detail_arr[$k]['err_date'] = $dayid;
                    $error_detail_arr[$k]['first_level_id'] = isset($json_info['parent_identifier']) ? $json_info['parent_identifier'] : '';
                    $error_detail_arr[$k]['first_level_name'] = '';
                    $error_detail_arr[$k]['second_level_id'] = isset($json_info['sku']) ? $json_info['sku'] : '';
                    $error_detail_arr[$k]['second_level_name'] = '';
                    $error_detail_arr[$k]['money'] = isset($json_info['units']) ? $json_info['units'] : 0.00; // 流水原币
                    $error_detail_arr[$k]['account'] = isset($v['account']) ? $v['account'] : '';
                    $error_detail_arr[$k]['create_time'] = date('Y-m-d H:i:s');


                    unset($array[$k]);
                    //插入错误数据
                    continue;
                }
                $array[$k]['date'] = $dayid;
                $array[$k]['channel_id'] = 'ci001';
                $array[$k]['platform_id'] = $source_id;
                $array[$k]['platform_account'] = $v['account'];
                $array[$k]['publisher_id'] = 5;

                // if($divide[0]['divide_billing']){
                //     $divide_ad = floatval($divide[0]['divide_billing']/100);
                // }else{
                //     $divide_ad =1;
                // }

                // if($divide[0]['bad_account_rate']){
                //     $bad_account_rate = floatval($divide[0]['bad_account_rate']/100);
                // }else{
                //     $bad_account_rate =0;
                // }

                // if(!$ex_info['currency_ex']){
                //     $ex_info['currency_ex'] =1;
                // }

                $array[$k]['device_type'] = $json_info['device'];
                $array[$k]['pay_user'] = $json_info['units'];
                $array[$k]['pay_time'] = $json_info['units'];
                $array[$k]['pay_user_all'] = $json_info['units'];
                $array[$k]['pay_time_all'] = $json_info['units'];

                $array[$k]['times'] = $json_info['product_type_identifier'];
                $array[$k]['create_time'] = date('Y-m-d H:i:s');
                $array[$k]['update_time'] = date('Y-m-d H:i:s');

            }
            //var_dump(count($array));die;

            // 保存错误信息
            if ($error_log_arr) {
                //var_dump($error_log_arr);die;
                $error_msg_array = [];
                $error_log_arr = Service::shield_error($source_id, $error_log_arr);

                if (isset($error_log_arr['app_id']) && !empty($error_log_arr['app_id'])) {
                    $app_id = implode(',', array_unique($error_log_arr['app_id']));
                    $error_msg_array[] = '应用id匹配失败,ID为:' . $app_id;
                }
                if (isset($error_log_arr['country']) && !empty($error_log_arr['country'])) {
                    $country = implode(',', array_unique($error_log_arr['country']));
                    $error_msg_array[] = '国家匹配失败,ID为:' . $country;
                }
                if (isset($error_log_arr['currency']) && !empty($error_log_arr['currency'])) {
                    $currency = implode(',', array_unique($error_log_arr['currency']));
                    $error_msg_array[] = '汇率匹配失败,ID为:' . $currency;
                }
                if (!empty($error_msg_array)) {
                    DataImportImp::saveDataErrorLog(2, $source_id, $billing_name, 3, implode(';', $error_msg_array));
                }
                DataImportImp::saveDataErrorMoneyLog($source_id, $dayid, $error_detail_arr);
//            CommonFunction::sendMail($error_msg_array,$billing_name.'计费平台数据处理error');
            }

            if (!empty($array)) {
                DB::beginTransaction();
                $map_delete['platform_id'] = $source_id;
                $map_delete['date'] = $dayid;
                DataImportLogic::deleteMysqlHistoryData($mysql_table, $map_delete);
                //拆分批次
                $step = array();
                $i = 0;
                foreach ($array as $kkkk => $insert_data_info) {
                    if ($kkkk % 200 == 0) $i++;
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
                    // sum(pay_user) AS pay_user,
                    // sum(pay_time) AS pay_time,
                    // 0 AS pay_user_fail,
                    // 0 AS pay_time_fail,
                    // sum(pay_user_all) AS pay_user_all,
                    // sum(pay_time_all) AS pay_time_all,
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
//            DB::update("call ff_summary('$dayid','$dayid','$source_id')");
                Artisan::call('FfSummaryProcesses', ['begin_date' => $dayid, 'end_date' => $dayid, 'platform_id' => $source_id]);
                // 查询广告数据
                $report_map = [];
                $report_map['platform_id'] = $source_id;
                $report_map['date'] = $dayid;
                $group_by = ['platform_id', 'date', 'platform_account'];
                $report_list = PlatformLogic::getAdReportSum($mysql_table, $report_map)->select(DB::raw("sum(income_fix) as cost"), 'platform_id', 'date', 'platform_account')->groupBy($group_by)->get();
                $report_list = Service::data($report_list);
                if ($report_list) {
                    // 保存广告平台
                    foreach ($report_list as $value) {
                        PlatformImp::add_platform_status($source_id, $value['platform_account'], $value['cost'], $dayid);
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