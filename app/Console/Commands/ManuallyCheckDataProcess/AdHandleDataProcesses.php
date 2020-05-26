<?php

namespace App\Console\Commands\ManuallyCheckDataProcess;

use App\BusinessImp\DataImportImp;
use App\Common\CurlRequest;
use App\Common\Service;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\DataImportLogic;
use App\Common\CommonFunction;
use Illuminate\Support\Facades\Redis;
use App\BusinessLogic\DataUploadLogic;
use App\BusinessLogic\CommonLogic;
use App\BusinessLogic\OperationLogLogic;
use App\Common\ApiResponseFactory;
use App\BusinessLogic\RoleLogic;
use PHPExcel_IOFactory;

class AdHandleDataProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'AdHandleDataProcesses';

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

    }

    public static function unityDataProcess($plat_config, $platform_id, $arr_data)
    {
        $result_data = [];
        $num_data = 0;
        $arr_info = [];
        $arr_info_1 = [];
        $arr_del_ad = [];
        $app_id = '';
        $os_id = '';
        $error_log_arr = [];
        foreach ($arr_data as $p_k => $p_v) {
//            if(count($p_v)<9){
//                continue;
//            }
            $timestamp = isset($p_v['timestamp']) ? date('Y-m-d', strtotime($p_v['timestamp'])) : '';
            if (empty($timestamp)) {
                continue;
            }

            if ($p_v['revenue_sum'] == 0 && $p_v['revenue_sum'] == 0) {
                continue;
            }
            $num = 0;
            foreach ($plat_config as $k => $v) {

                if (isset($p_v['source_game_id']) && !empty($p_v['source_game_id'])) {
                    if ($p_v['source_game_id'] == $v['platform_app_id']) {
                        $app_id = $v['app_id'];
                        $os_id = $v['os_id'];
                        $num = 1;
                    }

                }
            }
            if ($num == 1) {
                $arr_del_ad[] = $timestamp;
                $arr_info[$num_data]['date'] = $timestamp;
                $arr_info[$num_data]['ad_id'] = '';
                $arr_info[$num_data]['ad_name'] = '';
                $arr_info[$num_data]['platform_app_id'] = isset($p_v['source_game_id']) ? $p_v['source_game_id'] : '';
                $arr_info[$num_data]['platform_app_name'] = isset($p_v['source_name']) ? addslashes($p_v['source_name']) : '';
                $arr_info[$num_data]['app_id'] = $app_id;
                $arr_info[$num_data]['os_id'] = $os_id;
                $arr_info[$num_data]['income'] = isset($p_v['revenue_sum']) ? floatval(trim($p_v['revenue_sum'])) : 0;
                $arr_info[$num_data]['biding_income'] = isset($p_v['Bidding Revenue']) ? floatval(trim($p_v['Bidding Revenue'])) : 0;
                $arr_info[$num_data]['platform_id'] = $platform_id;
                $arr_info[$num_data]['create_time'] = date('Y-m-d H:i:s');

            } else {

                $arr_del_ad[] = $timestamp;
                $arr_info_1[$num_data]['date'] = $timestamp;
                $arr_info_1[$num_data]['ad_id'] = '';
                $arr_info_1[$num_data]['ad_name'] = '';
                $arr_info_1[$num_data]['platform_app_id'] = isset($p_v['source_game_id']) ? $p_v['source_game_id'] : '';
                $arr_info_1[$num_data]['platform_app_name'] = isset($p_v['source_name']) ? addslashes($p_v['source_name']) : '';
                $arr_info_1[$num_data]['app_id'] = 0;
                $arr_info_1[$num_data]['os_id'] = 0;
                $arr_info_1[$num_data]['income'] = isset($p_v['revenue_sum']) ? floatval($p_v['revenue_sum']) : 0;
                $arr_info_1[$num_data]['biding_income'] = isset($p_v['Bidding Revenue']) ? floatval(trim($p_v['Bidding Revenue'])) : 0;
                $arr_info_1[$num_data]['platform_id'] = $platform_id;
                $arr_info_1[$num_data]['create_time'] = date('Y-m-d H:i:s');

                $app_id = isset($p_v['source_game_id']) ? $p_v['source_game_id'] : '';
                $app_name = isset($p_v['source_name']) ? addslashes($p_v['source_name']) : '';
                $error_log_arr['app_id'][] = $app_id.'('.$app_name.')';
            }
            $num_data++;

        }

        $result_data['arr_info'] = $arr_info;
        $result_data['arr_info_1'] = $arr_info_1;
        $result_data['arr_del_ad'] = array_unique($arr_del_ad);

        self::saveHandWorkError($error_log_arr,'app_id',$platform_id,'unity');

        return $result_data;
    }

    public static function admobDataProcess($plat_config, $platform_id, $arr_data)
    {
        $result_data = [];
        $num_data = 0;
        $arr_info = [];
        $arr_info_1 = [];
        $arr_del_ad = [];
        $app_id = '';
        $os_id = '';
        $error_log_arr = [];
        foreach ($arr_data as $p_k => $p_v) {
//            if(count($p_v)<9){
//                continue;
//            }
            $time = (int)$p_v['Date'];
            $d = 25569;
            $t= 24*60*60;
            $timen = date('Y-m-d',($time-$d)*$t);
            $timestamp = isset($p_v['Date']) ? $timen : '';
            if (empty($timestamp)) {
                continue;
            }

            if ($p_v['Estimated earnings (USD)'] == 0 && $p_v['Estimated earnings (USD)'] == 0) {
                continue;
            }
            $num = 0;
            foreach ($plat_config as $k => $v) {

                if (isset($p_v['Ad unit']) && !empty($p_v['Ad unit'])) {

                    if ($p_v['Ad unit'] == $v['ad_slot_name']) {
                        $app_id = $v['app_id'];
                        $os_id = $v['os_id'];
                        $num = 1;
                    }

                }
            }
            if ($num == 1) {
                $arr_del_ad[] = $timestamp;
                $arr_info[$num_data]['date'] = $timestamp;
                $arr_info[$num_data]['ad_id'] = '';
                $arr_info[$num_data]['ad_name'] = isset($p_v['Ad unit']) ? addslashes($p_v['Ad unit']) : '';
                $arr_info[$num_data]['platform_app_id'] = '';
                $arr_info[$num_data]['platform_app_name'] = isset($p_v['App']) ? addslashes($p_v['App']) : '';
                $arr_info[$num_data]['app_id'] = $app_id;
                $arr_info[$num_data]['os_id'] = $os_id;
                $arr_info[$num_data]['income'] = isset($p_v['Estimated earnings (USD)']) ? floatval(trim($p_v['Estimated earnings (USD)'])) : 0;
                $arr_info[$num_data]['biding_income'] =  0;
                $arr_info[$num_data]['platform_id'] = $platform_id;
                $arr_info[$num_data]['create_time'] = date('Y-m-d H:i:s');

            } else {

                $arr_del_ad[] = $timestamp;
                $arr_info_1[$num_data]['date'] = $timestamp;
                $arr_info_1[$num_data]['ad_id'] = '';
                $arr_info_1[$num_data]['ad_name'] = isset($p_v['Ad unit']) ? addslashes($p_v['Ad unit']) : '';
                $arr_info_1[$num_data]['platform_app_id'] =  '';
                $arr_info_1[$num_data]['platform_app_name'] = isset($p_v['App']) ? addslashes($p_v['App']) : '';
                $arr_info_1[$num_data]['app_id'] = 0;
                $arr_info_1[$num_data]['os_id'] = 0;
                $arr_info_1[$num_data]['income'] = isset($p_v['Estimated earnings (USD)']) ? floatval($p_v['Estimated earnings (USD)']) : 0;
                $arr_info_1[$num_data]['biding_income'] =  0;
                $arr_info_1[$num_data]['platform_id'] = $platform_id;
                $arr_info_1[$num_data]['create_time'] = date('Y-m-d H:i:s');

                $unit_id = isset($p_v['Ad unit']) ? addslashes($p_v['Ad unit']) : '';
                $error_log_arr['unit_id'][] = $unit_id;
            }
            $num_data++;

        }

        $result_data['arr_info'] = $arr_info;
        $result_data['arr_info_1'] = $arr_info_1;
        $result_data['arr_del_ad'] = array_unique($arr_del_ad);

        self::saveHandWorkError($error_log_arr,'unit_id',$platform_id,'admob');

        return $result_data;
    }

    public static function chartboostDataProcess($plat_config, $platform_id, $arr_data)
    {
        $result_data = [];
        $num_data = 0;
        $arr_info = [];
        $arr_info_1 = [];
        $arr_del_ad = [];
        $app_id = '';
        $os_id = '';
        $error_log_arr = [];
        foreach ($arr_data as $p_k => $p_v) {
//            if(count($p_v)<9){
//                continue;
//            }
            $time = (int)$p_v['Date'];
            $d = 25569;
            $t= 24*60*60;
            $timen = date('Y-m-d',($time-$d)*$t);
            $timestamp = isset($p_v['Date']) ? $timen : '';
            if (empty($timestamp)) {
                continue;
            }

            if (trim($p_v['Money'],'$') == 0 && trim($p_v['Money'],'$') == 0) {
                continue;
            }
            $num = 0;
            foreach ($plat_config as $k => $v) {

                if (isset($p_v['Name']) && !empty($p_v['Name'])) {
                    if ($p_v['Name'] == $v['platform_app_name']) {
                        $app_id = $v['app_id'];
                        $os_id = $v['os_id'];
                        $num = 1;
                    }

                }
            }
            if ($num == 1) {
                $arr_del_ad[] = $timestamp;
                $arr_info[$num_data]['date'] = $timestamp;
                $arr_info[$num_data]['ad_id'] = '';
                $arr_info[$num_data]['ad_name'] = '';
                $arr_info[$num_data]['platform_app_id'] =  '';
                $arr_info[$num_data]['platform_app_name'] = isset($p_v['Name']) ? addslashes($p_v['Name']) : '';
                $arr_info[$num_data]['app_id'] = $app_id;
                $arr_info[$num_data]['os_id'] = $os_id;
                $arr_info[$num_data]['income'] = isset($p_v['Money']) ? floatval(trim($p_v['Money'],'$')) : 0;
                $arr_info[$num_data]['biding_income'] = isset($p_v['Bidding Revenue']) ? floatval(trim($p_v['Bidding Revenue'])) : 0;
                $arr_info[$num_data]['platform_id'] = $platform_id;
                $arr_info[$num_data]['create_time'] = date('Y-m-d H:i:s');

            } else {

                $arr_del_ad[] = $timestamp;
                $arr_info_1[$num_data]['date'] = $timestamp;
                $arr_info_1[$num_data]['ad_id'] = '';
                $arr_info_1[$num_data]['ad_name'] = '';
                $arr_info_1[$num_data]['platform_app_id'] = '';
                $arr_info_1[$num_data]['platform_app_name'] = isset($p_v['Name']) ? addslashes($p_v['Name']) : '';
                $arr_info_1[$num_data]['app_id'] = 0;
                $arr_info_1[$num_data]['os_id'] = 0;
                $arr_info_1[$num_data]['income'] = isset($p_v['Money']) ?floatval(trim($p_v['Money'],'$')) : 0;
                $arr_info_1[$num_data]['biding_income'] = isset($p_v['Bidding Revenue']) ? floatval(trim($p_v['Bidding Revenue'])) : 0;
                $arr_info_1[$num_data]['platform_id'] = $platform_id;
                $arr_info_1[$num_data]['create_time'] = date('Y-m-d H:i:s');

                $app_name = isset($p_v['Name']) ? addslashes($p_v['Name']) : '';
                $error_log_arr['app_name'][] = $app_name;
            }
            $num_data++;

        }

        $result_data['arr_info'] = $arr_info;
        $result_data['arr_info_1'] = $arr_info_1;
        $result_data['arr_del_ad'] = array_unique($arr_del_ad);

        self::saveHandWorkError($error_log_arr,'app_name',$platform_id,'chartboost');

        return $result_data;
    }

    public static function ironsoursDataProcess($plat_config, $platform_id, $arr_data)
    {
        $result_data = [];
        $num_data = 0;
        $arr_info = [];
        $arr_info_1 = [];
        $arr_del_ad = [];
        $app_id = '';
        $os_id = '';
        $error_log_arr = [];
        foreach ($arr_data as $p_k => $p_v) {
//            if(count($p_v)<9){
//                continue;
//            }
            $timestamp = isset($p_v['Date']) ? date('Y-m-d', strtotime($p_v['Date'])) : '';
            if (empty($timestamp)) {
                continue;
            }

            if ($p_v['Revenue'] == 0 && $p_v['Revenue'] == 0) {
                continue;
            }
            $num = 0;
            foreach ($plat_config as $k => $v) {

                if (isset($p_v['App Name']) && !empty($p_v['App Name'])) {
                    if ($p_v['App Name'] == $v['platform_app_name']) {
                        $app_id = $v['app_id'];
                        $os_id = $v['os_id'];
                        $num = 1;
                    }

                }
            }
            if ($num == 1) {
                $arr_del_ad[] = $timestamp;
                $arr_info[$num_data]['date'] = $timestamp;
                $arr_info[$num_data]['ad_id'] = '';
                $arr_info[$num_data]['ad_name'] = '';
                $arr_info[$num_data]['platform_app_id'] = '';
                $arr_info[$num_data]['platform_app_name'] = isset($p_v['App Name']) ? addslashes($p_v['App Name']) : '';
                $arr_info[$num_data]['app_id'] = $app_id;
                $arr_info[$num_data]['os_id'] = $os_id;
                $arr_info[$num_data]['income'] = isset($p_v['Revenue']) ? floatval(trim($p_v['Revenue'])) : 0;
                $arr_info[$num_data]['biding_income'] = isset($p_v['Bidding Revenue']) ? floatval(trim($p_v['Bidding Revenue'])) : 0;
                $arr_info[$num_data]['platform_id'] = $platform_id;
                $arr_info[$num_data]['create_time'] = date('Y-m-d H:i:s');

            } else {

                $arr_del_ad[] = $timestamp;
                $arr_info_1[$num_data]['date'] = $timestamp;
                $arr_info_1[$num_data]['ad_id'] = '';
                $arr_info_1[$num_data]['ad_name'] = '';
                $arr_info_1[$num_data]['platform_app_id'] = '';
                $arr_info_1[$num_data]['platform_app_name'] = isset($p_v['App Name']) ? addslashes($p_v['App Name']) : '';
                $arr_info_1[$num_data]['app_id'] = 0;
                $arr_info_1[$num_data]['os_id'] = 0;
                $arr_info_1[$num_data]['income'] = isset($p_v['Revenue']) ? floatval($p_v['Revenue']) : 0;
                $arr_info_1[$num_data]['biding_income'] = isset($p_v['Bidding Revenue']) ? floatval(trim($p_v['Bidding Revenue'])) : 0;
                $arr_info_1[$num_data]['platform_id'] = $platform_id;
                $arr_info_1[$num_data]['create_time'] = date('Y-m-d H:i:s');

                $app_name = isset($p_v['App Name']) ? addslashes($p_v['App Name']) : '';
                $error_log_arr['app_name'][] = $app_name;
            }
            $num_data++;

        }

        $result_data['arr_info'] = $arr_info;
        $result_data['arr_info_1'] = $arr_info_1;
        $result_data['arr_del_ad'] = array_unique($arr_del_ad);

        self::saveHandWorkError($error_log_arr,'app_name',$platform_id,'ironsours');

        return $result_data;
    }

    public static function guangdiantongDataProcess($plat_config, $platform_id, $arr_data)
    {
        $result_data = [];
        $num_data = 0;
        $arr_info = [];
        $arr_info_1 = [];
        $arr_del_ad = [];
        $app_id = '';
        $os_id = '';
        $error_log_arr = [];
        foreach ($arr_data as $p_k => $p_v) {
//            if(count($p_v)<9){
//                continue;
//            }
            $timestamp = isset($p_v['时间']) ? date('Y-m-d', strtotime($p_v['时间'])) : '';
            if (empty($timestamp)) {
                continue;
            }

            if (floatval(str_replace(',','',trim($p_v['预计收入']))) == 0 ) {
                continue;
            }
            $num = 0;
            foreach ($plat_config as $k => $v) {

                if (isset($p_v['媒体ID']) && !empty($p_v['媒体ID'])) {
                    if (trim(trim($p_v['媒体ID'],'"')) == $v['platform_app_id']) {
                        $app_id = $v['app_id'];
                        $os_id = $v['os_id'];
                        $num = 1;
                    }

                }
            }
            if ($num == 1) {
                $arr_del_ad[] = $timestamp;
                $arr_info[$num_data]['date'] = $timestamp;
                $arr_info[$num_data]['ad_id'] = '';
                $arr_info[$num_data]['ad_name'] = '';
                $arr_info[$num_data]['platform_app_id'] = isset($p_v['媒体ID']) ? trim(trim($p_v['媒体ID'],'"')) : '';
                $arr_info[$num_data]['platform_app_name'] = isset($p_v['媒体']) ? addslashes($p_v['媒体']) : '';
                $arr_info[$num_data]['app_id'] = $app_id;
                $arr_info[$num_data]['os_id'] = $os_id;
                $arr_info[$num_data]['income'] = isset($p_v['预计收入']) ? floatval(str_replace(',','',trim($p_v['预计收入']))) : 0;
                $arr_info[$num_data]['biding_income'] = isset($p_v['Bidding Revenue']) ? floatval(trim($p_v['Bidding Revenue'])) : 0;
                $arr_info[$num_data]['platform_id'] = $platform_id;
                $arr_info[$num_data]['create_time'] = date('Y-m-d H:i:s');

            } else {

                $arr_del_ad[] = $timestamp;
                $arr_info_1[$num_data]['date'] = $timestamp;
                $arr_info_1[$num_data]['ad_id'] = '';
                $arr_info_1[$num_data]['ad_name'] = '';
                $arr_info_1[$num_data]['platform_app_id'] = isset($p_v['媒体ID']) ? trim(trim($p_v['媒体ID'],'""')) : '';
                $arr_info_1[$num_data]['platform_app_name'] = isset($p_v['媒体']) ? addslashes($p_v['媒体']) : '';
                $arr_info_1[$num_data]['app_id'] = 0;
                $arr_info_1[$num_data]['os_id'] = 0;
                $arr_info_1[$num_data]['income'] = isset($p_v['预计收入']) ? floatval(str_replace(',','',trim($p_v['预计收入']))): 0;
                $arr_info_1[$num_data]['biding_income'] = isset($p_v['Bidding Revenue']) ? floatval(trim($p_v['Bidding Revenue'])) : 0;
                $arr_info_1[$num_data]['platform_id'] = $platform_id;
                $arr_info_1[$num_data]['create_time'] = date('Y-m-d H:i:s');

                $app_id = isset($p_v['媒体ID']) ? $p_v['媒体ID'] : '';
                $app_name = isset($p_v['媒体']) ? addslashes($p_v['媒体']) : '';
                $error_log_arr['app_id'][] = $app_id.'('.$app_name.')';
            }
            $num_data++;

        }

        $result_data['arr_info'] = $arr_info;
        $result_data['arr_info_1'] = $arr_info_1;
        $result_data['arr_del_ad'] = array_unique($arr_del_ad);

        self::saveHandWorkError($error_log_arr,'app_id',$platform_id,'广点通');

        return $result_data;
    }

    public static function baiduDataProcess($plat_config, $platform_id, $arr_data)
    {
        $result_data = [];
        $num_data = 0;
        $arr_info = [];
        $arr_info_1 = [];
        $arr_del_ad = [];
        $app_id = '';
        $os_id = '';
        $error_log_arr = [];
        foreach ($arr_data as $p_k => $p_v) {
//            if(count($p_v)<9){
//                continue;
//            }
            $timestamp = isset($p_v['时间']) ? date('Y-m-d', strtotime($p_v['时间'])) : '';
            if (empty($timestamp)) {
                continue;
            }

            if ($p_v['收入'] == 0 && $p_v['收入'] == 0) {
                continue;
            }
            $num = 0;
            foreach ($plat_config as $k => $v) {

                if (isset($p_v['APPID']) && !empty($p_v['APPID'])) {
                    if ($p_v['APPID'] == $v['platform_app_id']) {
                        $app_id = $v['app_id'];
                        $os_id = $v['os_id'];
                        $num = 1;
                    }

                }
            }
            if ($num == 1) {
                $arr_del_ad[] = $timestamp;
                $arr_info[$num_data]['date'] = $timestamp;
                $arr_info[$num_data]['ad_id'] = '';
                $arr_info[$num_data]['ad_name'] = '';
                $arr_info[$num_data]['platform_app_id'] = isset($p_v['APPID']) ? $p_v['APPID'] : '';
                $arr_info[$num_data]['platform_app_name'] = isset($p_v['APP']) ? addslashes($p_v['APP']) : '';
                $arr_info[$num_data]['app_id'] = $app_id;
                $arr_info[$num_data]['os_id'] = $os_id;
                $arr_info[$num_data]['income'] = isset($p_v['收入']) ? floatval(trim($p_v['收入'])) : 0;
                $arr_info[$num_data]['biding_income'] = isset($p_v['Bidding Revenue']) ? floatval(trim($p_v['Bidding Revenue'])) : 0;
                $arr_info[$num_data]['platform_id'] = $platform_id;
                $arr_info[$num_data]['create_time'] = date('Y-m-d H:i:s');

            } else {

                $arr_del_ad[] = $timestamp;
                $arr_info_1[$num_data]['date'] = $timestamp;
                $arr_info_1[$num_data]['ad_id'] = '';
                $arr_info_1[$num_data]['ad_name'] = '';
                $arr_info_1[$num_data]['platform_app_id'] = isset($p_v['APPID']) ? $p_v['APPID'] : '';
                $arr_info_1[$num_data]['platform_app_name'] = isset($p_v['APP']) ? addslashes($p_v['APP']) : '';
                $arr_info_1[$num_data]['app_id'] = 0;
                $arr_info_1[$num_data]['os_id'] = 0;
                $arr_info_1[$num_data]['income'] = isset($p_v['收入']) ? floatval($p_v['收入']) : 0;
                $arr_info_1[$num_data]['biding_income'] = isset($p_v['Bidding Revenue']) ? floatval(trim($p_v['Bidding Revenue'])) : 0;
                $arr_info_1[$num_data]['platform_id'] = $platform_id;
                $arr_info_1[$num_data]['create_time'] = date('Y-m-d H:i:s');

                $app_id = isset($p_v['APPID']) ? $p_v['APPID'] : '';
                $app_name = isset($p_v['APP']) ? addslashes($p_v['APP']) : '';
                $error_log_arr['app_id'][] = $app_id.'('.$app_name.')';
            }
            $num_data++;

        }

        $result_data['arr_info'] = $arr_info;
        $result_data['arr_info_1'] = $arr_info_1;
        $result_data['arr_del_ad'] = array_unique($arr_del_ad);

        self::saveHandWorkError($error_log_arr,'app_id',$platform_id,'百度');

        return $result_data;
    }

    public static function mintegalDataProcess($plat_config, $platform_id, $arr_data)
    {
        $result_data = [];
        $num_data = 0;
        $arr_info = [];
        $arr_info_1 = [];
        $arr_del_ad = [];
        $app_id = '';
        $os_id = '';
        $error_log_arr = [];
        foreach ($arr_data as $p_k => $p_v) {
//            if(count($p_v)<9){
//                continue;
//            }
            $timestamp = isset($p_v['Date']) ? date('Y-m-d', strtotime($p_v['Date'])) : '';
            if (empty($timestamp)) {
                continue;
            }

            if (trim($p_v['Revenue'],'$') == 0 && trim($p_v['Revenue'],'$') == 0) {
                continue;
            }
            $num = 0;
            foreach ($plat_config as $k => $v) {

                if (isset($p_v['Unit']) && !empty($p_v['Unit'])) {
                    $ad_slot_id= explode("-",$p_v['Unit']);
                    if ( trim($ad_slot_id[0])== $v['ad_slot_id']) {
                        $app_id = $v['app_id'];
                        $os_id = $v['os_id'];
                        $num = 1;
                    }

                }
            }
            if ($num == 1) {
                $arr_del_ad[] = $timestamp;
                $arr_info[$num_data]['date'] = $timestamp;
                $arr_info[$num_data]['ad_id'] = isset($p_v['Unit']) ? trim($ad_slot_id[0]) : '';
                $arr_info[$num_data]['ad_name'] = isset($p_v['Unit']) ? addslashes($p_v['Unit']) : '';
                $arr_info[$num_data]['platform_app_id'] = '';
                $arr_info[$num_data]['platform_app_name'] =  '';
                $arr_info[$num_data]['app_id'] = $app_id;
                $arr_info[$num_data]['os_id'] = $os_id;
                $arr_info[$num_data]['income'] = isset($p_v['Revenue']) ? floatval(trim($p_v['Revenue'],'$')) : 0;
                $arr_info[$num_data]['biding_income'] = isset($p_v['Bidding Revenue']) ? floatval(trim($p_v['Bidding Revenue'])) : 0;
                $arr_info[$num_data]['platform_id'] = $platform_id;
                $arr_info[$num_data]['create_time'] = date('Y-m-d H:i:s');

            } else {

                $arr_del_ad[] = $timestamp;
                $arr_info_1[$num_data]['date'] = $timestamp;
                $arr_info_1[$num_data]['ad_id'] = isset($p_v['Unit']) ? trim($ad_slot_id[0]) : '';;
                $arr_info_1[$num_data]['ad_name'] = isset($p_v['Unit']) ? addslashes($p_v['Unit']) : '';
                $arr_info_1[$num_data]['platform_app_id'] =  '';
                $arr_info_1[$num_data]['platform_app_name'] = '';
                $arr_info_1[$num_data]['app_id'] = 0;
                $arr_info_1[$num_data]['os_id'] = 0;
                $arr_info_1[$num_data]['income'] = isset($p_v['Revenue']) ? floatval(trim($p_v['Revenue'],'$')) : 0;
                $arr_info_1[$num_data]['biding_income'] = isset($p_v['Bidding Revenue']) ? floatval(trim($p_v['Bidding Revenue'])) : 0;
                $arr_info_1[$num_data]['platform_id'] = $platform_id;
                $arr_info_1[$num_data]['create_time'] = date('Y-m-d H:i:s');

                $unit_id = isset($p_v['Unit']) ? addslashes($p_v['Unit']) : '';
                $error_log_arr['unit_id'][] = $unit_id;
            }
            $num_data++;

        }

        $result_data['arr_info'] = $arr_info;
        $result_data['arr_info_1'] = $arr_info_1;
        $result_data['arr_del_ad'] = array_unique($arr_del_ad);

        self::saveHandWorkError($error_log_arr,'unit_id',$platform_id,'mintegral');

        return $result_data;
    }

    public static function tiktokDataProcess($plat_config, $platform_id, $arr_data)
    {
        $result_data = [];
        $num_data = 0;
        $arr_info = [];
        $arr_info_1 = [];
        $arr_del_ad = [];
        $app_id = '';
        $os_id = '';
        $error_log_arr = [];
        foreach ($arr_data as $p_k => $p_v) {
//            if(count($p_v)<9){
//                continue;
//            }
            if($p_v['时间']=='总计'){
                continue;
            }
            $timestamp = isset($p_v['时间']) ? date('Y-m-d', strtotime($p_v['时间'])) : '';
            if (empty($timestamp)) {
                continue;
            }

            if ($p_v['预估收益(美元)'] == 0 && $p_v['预估收益(美元)'] == 0) {
                continue;
            }
            $num = 0;
            foreach ($plat_config as $k => $v) {

                if (isset($p_v['代码位ID']) && !empty($p_v['代码位ID']) &&isset($p_v['应用ID']) && !empty($p_v['应用ID'])) {
                    if ($p_v['应用ID'] == $v['platform_app_id'] && $p_v['代码位ID'] == $v['ad_slot_id'] ) {
                        $app_id = $v['app_id'];
                        $os_id = $v['os_id'];
                        $num = 1;
                    }

                }
            }
            if ($num == 1) {
                $arr_del_ad[] = $timestamp;
                $arr_info[$num_data]['date'] = $timestamp;
                $arr_info[$num_data]['ad_id'] = isset($p_v['代码位ID']) ? $p_v['代码位ID'] : '';
                $arr_info[$num_data]['ad_name'] = isset($p_v['代码位名称']) ? $p_v['代码位名称'] : '';
                $arr_info[$num_data]['platform_app_id'] = isset($p_v['应用ID']) ? $p_v['应用ID'] : '';
                $arr_info[$num_data]['platform_app_name'] = isset($p_v['应用名称']) ? addslashes($p_v['应用名称']) : '';
                $arr_info[$num_data]['app_id'] = $app_id;
                $arr_info[$num_data]['os_id'] = $os_id;
                $arr_info[$num_data]['income'] = isset($p_v['预估收益(美元)']) ? floatval(trim($p_v['预估收益(美元)'])) : 0;
                $arr_info[$num_data]['biding_income'] = isset($p_v['Bidding Revenue']) ? floatval(trim($p_v['Bidding Revenue'])) : 0;
                $arr_info[$num_data]['platform_id'] = $platform_id;
                $arr_info[$num_data]['create_time'] = date('Y-m-d H:i:s');

            } else {

                $arr_del_ad[] = $timestamp;
                $arr_info_1[$num_data]['date'] = $timestamp;
                $arr_info_1[$num_data]['ad_id'] = isset($p_v['代码位ID']) ? $p_v['代码位ID'] : '';
                $arr_info_1[$num_data]['ad_name'] = isset($p_v['代码位名称']) ? $p_v['代码位名称'] : '';
                $arr_info_1[$num_data]['platform_app_id'] = isset($p_v['应用ID']) ? $p_v['应用ID'] : '';
                $arr_info_1[$num_data]['platform_app_name'] = isset($p_v['应用名称']) ? addslashes($p_v['应用名称']) : '';
                $arr_info_1[$num_data]['app_id'] = 0;
                $arr_info_1[$num_data]['os_id'] = 0;
                $arr_info_1[$num_data]['income'] = isset($p_v['预估收益(美元)']) ? floatval($p_v['预估收益(美元)']) : 0;
                $arr_info_1[$num_data]['biding_income'] = isset($p_v['Bidding Revenue']) ? floatval(trim($p_v['Bidding Revenue'])) : 0;
                $arr_info_1[$num_data]['platform_id'] = $platform_id;
                $arr_info_1[$num_data]['create_time'] = date('Y-m-d H:i:s');

                $app_id = isset($p_v['应用ID']) ? $p_v['应用ID'] : '';
                $app_name = isset($p_v['应用名称']) ? addslashes($p_v['应用名称']) : '';
                $error_log_arr['app_id'][] = $app_id.'('.$app_name.')';
            }
            $num_data++;

        }

        $result_data['arr_info'] = $arr_info;
        $result_data['arr_info_1'] = $arr_info_1;
        $result_data['arr_del_ad'] = array_unique($arr_del_ad);

        self::saveHandWorkError($error_log_arr,'app_id',$platform_id,'Tiktok');

        return $result_data;
    }

//穿山甲
    public static function pangolinDataProcess($plat_config, $platform_id, $arr_data)
    {
        $result_data = [];
        $num_data = 0;
        $arr_info = [];
        $arr_info_1 = [];
        $arr_del_ad = [];
        $app_id = '';
        $os_id = '';
        $error_log_arr = [];
        foreach ($arr_data as $p_k => $p_v) {
//            if(count($p_v)<9){
//                continue;
//            }
            if($p_v['时间']=='总计'){
                continue;
            }
            $timestamp = isset($p_v['时间']) ? date('Y-m-d', strtotime($p_v['时间'])) : '';
            if (empty($timestamp)) {
                continue;
            }

            if ($p_v['预估收益(人民币)'] == 0 && $p_v['预估收益(人民币)'] == 0) {
                continue;
            }
            $num = 0;
            foreach ($plat_config as $k => $v) {

                if (isset($p_v['代码位ID']) && !empty($p_v['代码位ID']) &&isset($p_v['应用ID']) && !empty($p_v['应用ID'])) {
                    if ($p_v['应用ID'] == $v['platform_app_id'] && $p_v['代码位ID'] == $v['ad_slot_id'] ) {
                        $app_id = $v['app_id'];
                        $os_id = $v['os_id'];
                        $num = 1;
                    }

                }
            }
            if ($num == 1) {
                $arr_del_ad[] = $timestamp;
                $arr_info[$num_data]['date'] = $timestamp;
                $arr_info[$num_data]['ad_id'] = isset($p_v['代码位ID']) ? $p_v['代码位ID'] : '';
                $arr_info[$num_data]['ad_name'] = isset($p_v['代码位名称']) ? $p_v['代码位名称'] : '';
                $arr_info[$num_data]['platform_app_id'] = isset($p_v['应用ID']) ? $p_v['应用ID'] : '';
                $arr_info[$num_data]['platform_app_name'] = isset($p_v['应用名称']) ? addslashes($p_v['应用名称']) : '';
                $arr_info[$num_data]['app_id'] = $app_id;
                $arr_info[$num_data]['os_id'] = $os_id;
                $arr_info[$num_data]['income'] = isset($p_v['预估收益(人民币)']) ? floatval(trim($p_v['预估收益(人民币)'])) : 0;
                $arr_info[$num_data]['biding_income'] = isset($p_v['Bidding Revenue']) ? floatval(trim($p_v['Bidding Revenue'])) : 0;
                $arr_info[$num_data]['platform_id'] = $platform_id;
                $arr_info[$num_data]['create_time'] = date('Y-m-d H:i:s');

            } else {

                $arr_del_ad[] = $timestamp;
                $arr_info_1[$num_data]['date'] = $timestamp;
                $arr_info_1[$num_data]['ad_id'] = isset($p_v['代码位ID']) ? $p_v['代码位ID'] : '';
                $arr_info_1[$num_data]['ad_name'] = isset($p_v['代码位名称']) ? $p_v['代码位名称'] : '';
                $arr_info_1[$num_data]['platform_app_id'] = isset($p_v['应用ID']) ? $p_v['应用ID'] : '';
                $arr_info_1[$num_data]['platform_app_name'] = isset($p_v['应用名称']) ? addslashes($p_v['应用名称']) : '';
                $arr_info_1[$num_data]['app_id'] = 0;
                $arr_info_1[$num_data]['os_id'] = 0;
                $arr_info_1[$num_data]['income'] = isset($p_v['预估收益(人民币)']) ? floatval($p_v['预估收益(人民币)']) : 0;
                $arr_info_1[$num_data]['biding_income'] = isset($p_v['Bidding Revenue']) ? floatval(trim($p_v['Bidding Revenue'])) : 0;
                $arr_info_1[$num_data]['platform_id'] = $platform_id;
                $arr_info_1[$num_data]['create_time'] = date('Y-m-d H:i:s');

                $app_id = isset($p_v['应用ID']) ? $p_v['应用ID'] : '';
                $app_name = isset($p_v['应用名称']) ? addslashes($p_v['应用名称']) : '';
                $error_log_arr['app_id'][] = $app_id.'('.$app_name.')';
            }
            $num_data++;

        }

        $result_data['arr_info'] = $arr_info;
        $result_data['arr_info_1'] = $arr_info_1;
        $result_data['arr_del_ad'] = array_unique($arr_del_ad);

        self::saveHandWorkError($error_log_arr,'app_id',$platform_id,'穿山甲');

        return $result_data;
    }

    public static function facebookDataProcess($plat_config, $platform_id, $arr_data)
    {
        $result_data = [];
        $num_data = 0;
        $arr_info = [];
        $arr_info_1 = [];
        $arr_del_ad = [];
        $app_id = '';
        $os_id = '';
        $error_log_arr = [];
        foreach ($arr_data as $p_k => $p_v) {
//            if(count($p_v)<9){
//                continue;
//            }
            $timestamp = isset($p_v['Date']) ? date('Y-m-d', strtotime($p_v['Date'])) : '';
            if (empty($timestamp)) {
                continue;
            }

            if ($p_v['Revenue'] == 0 && $p_v['Revenue'] == 0) {
                continue;
            }
            $num = 0;
            foreach ($plat_config as $k => $v) {

                if (isset($p_v['App ID']) && !empty($p_v['App ID'])) {
                    if (trim(trim(trim($p_v['App ID'],'='), '"'))== $v['platform_app_id']) {
                        $app_id = $v['app_id'];
                        $os_id = $v['os_id'];
                        $num = 1;
                    }

                }
            }
            if ($num == 1) {
                $arr_del_ad[] = $timestamp;
                $arr_info[$num_data]['date'] = $timestamp;
                $arr_info[$num_data]['ad_id'] = '';
                $arr_info[$num_data]['ad_name'] = '';
                $arr_info[$num_data]['platform_app_id'] = isset($p_v['App ID']) ? trim(trim($p_v['App ID'],'='), '"') : '';
                $arr_info[$num_data]['platform_app_name'] = isset($p_v['App Name']) ? addslashes(trim(trim($p_v['App Name'],'='), '"')) : '';
                $arr_info[$num_data]['app_id'] = $app_id;
                $arr_info[$num_data]['os_id'] = $os_id;
                $arr_info[$num_data]['income'] = isset($p_v['Revenue']) ? floatval(trim($p_v['Revenue'])) : 0;
                $arr_info[$num_data]['biding_income'] = isset($p_v['Bidding Revenue']) ? floatval(trim($p_v['Bidding Revenue'])) : 0;
                $arr_info[$num_data]['platform_id'] = $platform_id;
                $arr_info[$num_data]['create_time'] = date('Y-m-d H:i:s');

            } else {

                $arr_del_ad[] = $timestamp;
                $arr_info_1[$num_data]['date'] = $timestamp;
                $arr_info_1[$num_data]['ad_id'] = '';
                $arr_info_1[$num_data]['ad_name'] = '';
                $arr_info_1[$num_data]['platform_app_id'] = isset($p_v['App ID']) ? trim(trim($p_v['App ID'],'='), '"') : '';
                $arr_info_1[$num_data]['platform_app_name'] = isset($p_v['App Name']) ? addslashes(trim(trim($p_v['App Name'],'='), '"')): '';
                $arr_info_1[$num_data]['app_id'] = 0;
                $arr_info_1[$num_data]['os_id'] = 0;
                $arr_info_1[$num_data]['income'] = isset($p_v['Revenue']) ? floatval($p_v['Revenue']) : 0;
                $arr_info_1[$num_data]['biding_income'] = isset($p_v['Bidding Revenue']) ? floatval(trim($p_v['Bidding Revenue'])) : 0;
                $arr_info_1[$num_data]['platform_id'] = $platform_id;
                $arr_info_1[$num_data]['create_time'] = date('Y-m-d H:i:s');

                $app_id = isset($p_v['App ID']) ? trim(trim($p_v['App ID'],'='), '"') : '';
                $app_name = isset($p_v['App Name']) ? addslashes(trim(trim($p_v['App Name'],'='), '"')): '';
                $error_log_arr['app_id'][] = $app_id.'('.$app_name.')';
            }
            $num_data++;

        }

        $result_data['arr_info'] = $arr_info;
        $result_data['arr_info_1'] = $arr_info_1;
        $result_data['arr_del_ad'] = array_unique($arr_del_ad);

        self::saveHandWorkError($error_log_arr,'app_id',$platform_id,'facebook');

        return $result_data;
    }

    public static function saveHandWorkError($error_log_arr,$error_field,$source_id,$source_name){
        // 保存错误信息
        if ($error_log_arr){
            $error_msg_array = [];
            $error_msg_mail = [];
            if (isset($error_log_arr[$error_field])){
                $error_info_str = implode(',',array_unique($error_log_arr[$error_field]));
                $error_msg_array[] = $error_field.'匹配失败,ID为:'.$error_info_str;
                $error_msg_mail[] = $error_field.'匹配失败，ID为：'.$error_info_str;
            }

            DataImportImp::saveDataErrorLog(4,$source_id,$source_name,2,implode(';',$error_msg_array));
            // 发送邮件
//            CommonFunction::sendMail($error_msg_mail,$source_name.'推广平台数据处理error');
        }
    }

}