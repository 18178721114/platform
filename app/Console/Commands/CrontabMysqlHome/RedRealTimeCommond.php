<?php

namespace App\Console\Commands\CrontabMysqlHome;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\Common\ApiResponseFactory;
use App\Common\CommonFunction;
use App\Common\CurlRequest;
use App\Common\Service;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\DataImportLogic;
#红包实时数据
class RedRealTimeCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'RedRealTimeCommond {dayid?} {appid?}';

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
       
        $dayid = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d');
        $mysql_table = 'zplay_red_data_statistics';
        try{
            $appid[0]['app_id'] = 'gi008034';
            $appid[1]['app_id'] = 'gi121005';
            foreach ($appid as $k => $v) {
                $url = 'https://red-bags-sdk.yeaplay.com/sdk-api/show_data_info_json.php?user=zplay&token=f382c6c1528ad5fd0cc7666b14603c50&game_id='.$v['app_id'];
                $info = self::get_response($url);
                $ret = json_decode($info, true);
                if ($ret) {
                    DB::beginTransaction();
                    //查询应用主键id
                    $sql = "select id  from c_app where app_id ='{$v['app_id']}' limit 1";
                    $sel_id = DB::select($sql);
                    $sel_id = Service::data($sel_id);
                    $sel_sql = "select count(1) as count  FROM $mysql_table WHERE date_time = '$dayid' and app_id = '{$sel_id[0]['id']}' ";
                    $sel_info = DB::select($sel_sql);
                    $sel_info = Service::data($sel_info);
                    if ($sel_info[0]['count'] != 0) {
                        $del_sql = "DELETE FROM $mysql_table WHERE date_time = '$dayid' and app_id = '{$sel_id[0]['id']}' ";
                        $delete_info = DB::delete($del_sql);

                        if (!$delete_info) {
                            DB::rollBack();
                        }
                    }

                    $app_sql = "select id ,app_id  FROM c_app";
                    $app_info = DB::select($app_sql);
                    $app_info = Service::data($app_info);


                    $create_time = date("Y-m-d H:i:s", time());
                    $k = 0;
                    $num = 0;
                    //foreach ($ret as $k => $v) {
                    //var_dump($v);die;
                    foreach ($app_info as $a => $b) {
                        if ($ret['all_money_arr']['game_id'] == $b['app_id']) {
                            $insert_data[$k]['app_id'] = $b['id'];
                            $num = 0;
                            break;
                        } else {
                            $num++;
                        }
                    }
                    if ($num > 0) {
                        $message = date("Y-m-d") . "红包数据应用id匹配失败，失败原因:" . $ret['all_money_arr']['game_id'];
                        DataImportImp::saveDataErrorLog(5, 'pad-001', '红包数据', 2, $message);
                        ApiResponseFactory::apiResponse([], [], 1056);
                    }

                    $insert_data[$k]['date_time'] = $dayid;
                    $insert_data[$k]['all_card_count'] = $ret['all_card_arr']['all_card'];
                    $insert_data[$k]['all_user_count'] = $ret['all_card_user_count_arr']['all_user'];
                    $insert_data[$k]['all_cat_count'] = $ret['all_cat_count_arr']['all_cat'];
                    $insert_data[$k]['all_cat_user_count'] = $ret['all_cat_user_count_arr']['all_cat_user'];
                    $insert_data[$k]['game_total_amount'] = $ret['all_money_arr']['total_amount_money'];
                    $insert_data[$k]['all_9cat_user_count'] = $ret['all_get_9_user_count_arr']['all_9cat_user_count'];
                    $insert_data[$k]['all_today_total'] = $ret['all_get_9_user_all_moeny_result_arr']['all_today_total'];
                    $insert_data[$k]['today_red_bags_user_count'] = $ret['all_get_9_user_all_count_arr']['user_count'];
                    $insert_data[$k]['tixian_total'] = $ret['tixian_result_arr']['tixian'];
                    $insert_data[$k]['all_send_money'] = $ret['all_send_money']['all_send_money'];
                    $insert_data[$k]['red_bags_count'] = $ret['red_bags_count']['red_bags_count'];
                    $insert_data[$k]['red_bags_user_count'] = $ret['red_bags_user_count']['red_bags_user_count'];
                    $insert_data[$k]['tixian_user_count'] = $ret['tixian_user_count']['tixian_count'];
                    $insert_data[$k]['create_time'] = $create_time;


                    // }


                    if ($insert_data) {
                        $step = array();
                        $i = 0;
                        foreach ($insert_data as $kkkk => $insert_data_info) {
                            if ($kkkk % 1000 == 0) $i++;
                            if ($insert_data_info) {
                                $step[$i][] = $insert_data_info;
                            }
                        }

                        $is_success = [];
                        if ($step) {
                            foreach ($step as $k => $v) {
                                $result = DataImportLogic::insertAdReportInfo($mysql_table, $v);
                                if (!$result) {
                                    $is_success[] = $k;
                                }
                            }
                        }

                        DB::commit();
                    }
                }
            }

        } catch (\Exception $e) {
            $message = $dayid."红包实时数据程序报错，失败原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, 'pad-001', '红包实时数据', 2, $message);
            ApiResponseFactory::apiResponse([],[],1056,$message);

        }


    }
    public static function get_response($url, $headers='')
    {
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);

        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_HEADER, 0);
        curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
        curl_setopt($ch, CURLOPT_TIMEOUT,120);
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, FALSE);
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, FALSE);

        if (!empty($headers)) {
            curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
        }

        $output = curl_exec($ch);
        curl_close($ch);
        return $output;
    }
}
