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

class FacebookTgHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'FacebookTgHandleProcesses {dayid?} {data_account?}';

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
        $source_id = 'ptg33';
        $source_name = 'Facebook';

        $dayid = $this->argument('dayid') ? $this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));
        var_dump($source_name.'-'.$source_id.'-'.$dayid);

        self::facebookTgDataProcess($dayid, $source_id, $source_name);
    }


    private static function facebookTgDataProcess($dayid, $source_id, $source_name){
        try {
            static $facebook_num = 0;
            //查询pgsql 的数据
            $map = [];
            $map['dayid'] = $dayid;
            $map['type'] = 2;
            $map['source_id'] = $source_id;
            $map[] = ['cost', '<>', 0];
            $info = DataImportLogic::getChannelData('tg_data', 'erm_data', $map)->get();
            $info = Service::data($info);
            if (!$info) {
                //var_dump(123);
                $error_msg = $dayid.'号，'.$source_name.'推广平台数据处理程序获取原始数据为空';
                DataImportImp::saveDataErrorLog(2,$source_id,$source_name,4,$error_msg);
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
                c_generalize.platform_id = 'ptg33' ";

            $app_list = DB::select($sql);
            $app_list = Service::data($app_list);

            if (!$app_list) {
                $error_msg = $source_name . '推广平台数据处理程序应用数据查询为空';
                DataImportImp::saveDataErrorLog(2, $source_id, $source_name, 4, $error_msg);
                exit;
            }

            // 获取美元汇率
            $effective_time = date("Ym", strtotime($dayid));
            $usd_ex_info = DataImportImp::getPlatformExchangeRate($effective_time);
            $usd_currency_ex = 0;
            if ($usd_ex_info) {
                $usd_currency_ex = $usd_ex_info['currency_ex'];
            }

            //获取对照表国家信息
            $country_map = [];
            $country_info = CommonLogic::getCountryList($country_map)->get();
            $country_info = Service::data($country_info);
            if (!$country_info) {
                $error_msg = $source_name . '推广平台数据处理程序国家信息数据查询为空';
                DataImportImp::saveDataErrorLog(2, $source_id, $source_name, 4, $error_msg);
                exit;
            }

            $array = [];
            $num = 0;
            $num_country = 0;
            $error_log_arr = [];
            // 反更新数据
            $error_detail_arr = [];
            $new_campaign_ids = [];
            foreach ($info as $k => $v) {
                $third_app_id = $v['account'];
                $json_info = json_decode($v['json_data'], true);
                $err_name = (isset($json_info['campaign_id']) ? $json_info['campaign_id'] : 'Null') . '#' . (isset($json_info['campaign_name']) ? addslashes($json_info['campaign_name']) : 'Null') . '#' . (isset($v['account']) ? $v['account'] : 'Null') . '#' . (isset($json_info['app_name']) ? $json_info['app_name'] : 'Null');

                foreach ($app_list as $app_k => $app_v) {
                    if (isset($json_info['campaign_id']) && ($json_info['campaign_id'] == $app_v['campaign_id'])) {
                        $array[$k]['app_id'] = $app_v['app_id'];
                        $array[$k]['platform_account'] = $app_v['data_account'];
                        $array[$k]['agency_platform_id'] = $app_v['agency_platform_id'];

                        //获取平台的汇率
                        $ex_map['currency_id'] = $app_list[0]['currency_type_id'];
                        if ($app_v['ageccy_currency_type_id']) {
                            $ex_map['currency_id'] = $app_list[0]['ageccy_currency_type_id'];

                        }
                        $ex_map['effective_time'] = date("Ym", strtotime($dayid));
                        $ex_fields = ['currency_ex'];
                        $ex_info = CommonLogic::getCurrencyEXList($ex_map, $ex_fields)->orderby('effective_time', 'desc')->first();
                        $ex_info = Service::data($ex_info);
                        if (!$ex_info) {
                            $error_msg = '汇率数据查询为空';
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

//                var_dump($json_info['campaign_id']);
                    $campaign_os_name = explode('_', $json_info['campaign_name']);
                    $campaign_os_list = array_map('strtolower', $campaign_os_name);

                    $campaign_os_mad_name = explode('-', $json_info['campaign_name']);
                    $campaign_os_mad_list = array_map('strtolower', $campaign_os_mad_name);
//                var_dump($campaign_os_list);
                    $app_os_id = '';
                    if (in_array('ios', $campaign_os_list) || in_array('ios', $campaign_os_mad_list)) {
                        $app_os_id = 1;
                    } elseif (in_array('and', $campaign_os_list) || in_array('and', $campaign_os_mad_list)) {
                        $app_os_id = 2;
                    } elseif (in_array('android', $campaign_os_list) || in_array('android', $campaign_os_mad_list)) {
                        $app_os_id = 2;
                    } elseif (in_array('h5', $campaign_os_list) || in_array('h5', $campaign_os_mad_list)) {
                        $app_os_id = 3;
                    }
                    if (in_array('amazon', $campaign_os_list) || in_array('amazon', $campaign_os_mad_list)) {
                        $app_os_id = 4;
                    }

                    if ($app_os_id && $third_app_id) {
                        $app_info_sql = "select cg.`id`,cg.`platform_id`,cg.`application_id`,ca.`os_id`,ca.`app_name` from c_generalize cg left join c_app ca on cg.app_id = ca.id where cg.`platform_id` = '{$source_id}' and cg.`application_id` = '{$third_app_id}' and cg.generalize_status = 1 and ca.`os_id` = {$app_os_id} limit 1";
//                    var_dump($app_info_sql);
                        $app_info_detail = DB::select($app_info_sql);
                        $app_info_detail = Service::data($app_info_detail);
//                    var_dump($app_info_detail);
                        if (isset($app_info_detail[0]) && $app_info_detail[0]) {
//                        var_dump($app_info_detail[0]['id']);
                            $new_campaign_ids[$third_app_id][$app_info_detail[0]['id']][] = $json_info['campaign_id'];
                        }
//                    var_dump($new_campaign_ids);
                    }

                    $error_log_arr['campaign_id'][] = $json_info['campaign_id'] . '(' . $err_name . ')';
                }


                // todo 匹配国家用
                foreach ($country_info as $country_k => $country_v) {
                    if (isset($json_info['country']) && ($json_info['country'] == $country_v['name'])) {
                        $array[$k]['country_id'] = $country_v['c_country_id'];
                        $num_country = 0;
                        break;
                    } else {
                        //国家配置失败
                        $num_country++;
                    }

                }

                if ($num_country) {
                    $error_log_arr['country'][] = (isset($json_info['country']) ? $json_info['country'] : 'Unknown Region') . '(' . $err_name . ')';
                }

                if (($num + $num_country) > 0) {

                    $error_detail_arr[$k]['platform_id'] = $source_id;
                    $error_detail_arr[$k]['platform_name'] = $source_name;
                    $error_detail_arr[$k]['platform_type'] = 4;
                    $error_detail_arr[$k]['err_date'] = $dayid;
                    $error_detail_arr[$k]['first_level_id'] = $third_app_id;
                    $error_detail_arr[$k]['first_level_name'] = '';

                    $err_campaign_id = ''; // isset($json_info['campaign_id']) ? addslashes($json_info['campaign_id']) : '';
                    $err_campaign_name = isset($json_info['campaign_name']) ? addslashes(str_replace('\'\'', '\'', $json_info['campaign_name'])) : '';

                    $error_detail_arr[$k]['second_level_id'] = $err_campaign_id;
                    $error_detail_arr[$k]['second_level_name'] = $err_campaign_name;
                    $error_detail_arr[$k]['money'] = isset($json_info['spend']) ? $json_info['spend'] : 0.00; // 流水原币
                    $error_detail_arr[$k]['account'] = isset($v['account']) ? $v['account'] : '';
                    $error_detail_arr[$k]['create_time'] = date('Y-m-d H:i:s');

                    unset($array[$k]);
                    //插入错误数据
                    continue;
                }

                $array[$k]['data_account'] = $v['account'];
                $array[$k]['date'] = $dayid;

                $array[$k]['platform_app_id'] = isset($json_info['campaign_id']) ? addslashes($json_info['campaign_id']) : '';
                $array[$k]['platform_app_name'] = isset($json_info['campaign_name']) ? addslashes(str_replace('\'\'', '\'', $json_info['campaign_name'])) : '';

                $array[$k]['ad_id'] = isset($json_info['ad_id']) ? addslashes($json_info['ad_id']) : '';
                $array[$k]['ad_name'] = isset($json_info['ad_name']) ? addslashes(str_replace('\'\'', '\'', $json_info['ad_name'])) : '';

                $array[$k]['impression'] = isset($json_info['impressions']) ? $json_info['impressions'] : 0;
                $array[$k]['click'] = isset($json_info['clicks']) ? $json_info['clicks'] : 0;
                $array[$k]['new'] = isset($json_info['mobile_app_install']) ? $json_info['mobile_app_install'] : 0;


                $array[$k]['cost'] = isset($json_info['spend']) ? $json_info['spend'] : 0.00; // 流水原币
                $currency_ex = floatval($ex_info['currency_ex']);
                if (!$currency_ex) {
                    $currency_ex = 1;
                }
                $array[$k]['cost_exc'] = isset($json_info['spend']) ? $json_info['spend'] * $currency_ex : 0;

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


            // 反更新数据
//        var_dump($new_campaign_ids);
            if ($new_campaign_ids) {
                $insert_generalize_ad_app = [];
                foreach ($new_campaign_ids as $package_name => $offer_id) {
                    if ($offer_id) {
                        foreach ($offer_id as $offer_key => $offer_id_nums) {
                            $offer_id_nums = array_unique($offer_id_nums);
//                            var_dump(222,$offer_id_nums);
                            foreach ($offer_id_nums as $offer_id_nums_key => $offer_id_nums_value) {
                                $insert_generalize_ad_info = [];
                                $insert_generalize_ad_info['generalize_id'] = $offer_key;
                                $insert_generalize_ad_info['campaign_id'] = strval($offer_id_nums_value);
                                $insert_generalize_ad_info['status'] = 1;
                                $insert_generalize_ad_info['create_time'] = date("Y-m-d H:i:s", time());
                                $insert_generalize_ad_info['update_time'] = date("Y-m-d H:i:s", time());
                                $insert_generalize_ad_app[] = $insert_generalize_ad_info;
                            }
                        }
                    }
                }


                if ($insert_generalize_ad_app) {
                    var_dump($facebook_num);
                    if ($facebook_num == 1) {
                        var_dump('反更新有问题：'.json_encode($insert_generalize_ad_app));
                    }else {
                        // 开启事物 保存数据
                        DB::beginTransaction();
                        $app_info = DB::table('c_generalize_ad_app')->insert($insert_generalize_ad_app);
                        if (!$app_info) { // 应用信息已经重复
                            DB::rollBack();
                        } else {
                            DB::commit();
                            $facebook_num ++;
                            self::facebookTgDataProcess($dayid, $source_id, $source_name);
                            exit;
                        }
                    }
                }
            }

            // 保存错误信息
            if ($error_log_arr) {
                $error_msg_array = [];
                $error_msg_mail = [];
                $error_log_arr = Service::shield_error($source_id, $error_log_arr);

                if (isset($error_log_arr['campaign_id']) && !empty($error_log_arr['campaign_id'])) {
                    $campaign_id = implode(',', array_unique($error_log_arr['campaign_id']));
                    $error_msg_array[] = 'campaign_id匹配失败,ID为:' . $campaign_id;
                    $error_msg_mail[] = 'campaign_id匹配失败，ID为：' . $campaign_id;
                }

                if (isset($error_log_arr['country']) && !empty($error_log_arr['country'])) {
                    $country = implode(',', array_unique($error_log_arr['country']));
                    $error_msg_array[] = '国家匹配失败,code为:' . $country;
                    $error_msg_mail[] = '国家匹配失败，code为：' . $country;
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
                $plat_str = $source_id . 'lishuyang@lishuyang' . $dayid;
                Redis::rpush(env('REDIS_TG_KEYS'), $plat_str);
                //拆分批次
                $step = array();
                $i = 0;
                foreach ($array as $kkkk => $insert_data_info) {
                    if ($kkkk % 500 == 0) $i++;
                    if ($insert_data_info) {
                        $step[$i][] = $insert_data_info;
                    }
                }

                //$ad_sql = "insert into ".MYSQL_AD_TABLE_NAME." (`date`,`app_id`,`channel_id`,`country_id`,`platform_id`,`agency_platform_id`,`data_platform_id`,`type`,`platform_account`,`data_account`,`cost_type`,`platform_app_id`,`platform_app_name`,`ad_id`,`ad_name`,`ad_type`,`tongji_type`,`impression`,`click`,`new`,`new_phone`,`new_pad`,`cost`,`cost_exc`,`device_type`,`remark`,`create_time`,`update_time`)values";

                $time = date('Y-m-d H:i:s');

                if ($step) {
                    foreach ($step as $k => $v_info) {
                        $sql_str = '';
                        foreach ($v_info as $k_sql => $v) {
                            # code...
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
                        $sql_str = rtrim($sql_str, ',');
                        Redis::rpush(env('REDIS_TG_KEYS'), $sql_str);
                    }
                }

            }
        }catch (\Exception $e) {
            // 异常报错
            $message = "{$dayid}号, " . $source_name . "推广平台程序报错,报错原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, $source_id, $source_name, 4, $message);
            $error_msg_arr[] = $message;
//            CommonFunction::sendMail($error_msg_arr, '推广平台程序error');
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