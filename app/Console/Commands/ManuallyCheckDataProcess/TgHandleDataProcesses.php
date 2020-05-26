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

class TgHandleDataProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'TgHandleDataProcesses';

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
    public  function handle(){

    }


    // 今日头条 推广
    public static function toutiaoDataProcess($plat_config,$platform_id,$arr_data){
        $result_data = [];
        $arr_del_tg = [];
        $arr_info = [];
        $arr_info_1 = [];
        $num_data = 0;
        $app_id = '';
        $os_id = '';
        $error_log_arr = [];
        foreach ($arr_data as $p_k => $p_v) {
//            if(count($p_v)<6){
//                continue;
//            }
            if($p_v['总花费(元)']==0){
                continue;
            }
            $num = 0;
            foreach ($plat_config as $k => $v) {
                if (isset($p_v['广告组id']) && !empty($p_v['广告组id'])) {
                    $campaign_id = trim(str_replace('"','',$p_v['广告组id']));
                    if ($campaign_id == $v['campaign_id']) {
                        $app_id = $v['app_id'];
                        $os_id = $v['os_id'];
                        $num = 1;
                        break;
                    }

                }
            }
            if($num ==1){
                $arr_del_tg[] = $p_v['时间'];
                $arr_info[$num_data]['date'] = $p_v['时间'];
                $arr_info[$num_data]['ad_id'] = isset($p_v['广告组id']) ? trim(str_replace('"','',$p_v['广告组id'])) : '' ;
                $arr_info[$num_data]['ad_name'] = isset($p_v['广告组'])?addslashes($p_v['广告组']):'' ;
                $arr_info[$num_data]['app_id'] = $app_id;
                $arr_info[$num_data]['os_id'] = $os_id;
                $arr_info[$num_data]['cost'] = isset($p_v['总花费(元)'])?floatval($p_v['总花费(元)']):0 ;
                $arr_info[$num_data]['platform_id'] = $platform_id;
                $arr_info[$num_data]['create_time'] = date('Y-m-d H:i:s');

            }else{

                $arr_del_tg[] = $p_v['时间'];
                $arr_info_1[$num_data]['date'] = $p_v['时间'];
                $arr_info_1[$num_data]['ad_id'] = isset($p_v['广告组id']) ? trim(str_replace('"','',$p_v['广告组id'])) : '' ;
                $arr_info_1[$num_data]['ad_name'] = isset($p_v['广告组'])?addslashes($p_v['广告组']):'' ;
                $arr_info_1[$num_data]['app_id'] = 0;
                $arr_info_1[$num_data]['os_id'] = 0;
                $arr_info_1[$num_data]['cost'] = isset($p_v['总花费(元)'])?floatval($p_v['总花费(元)']):0 ;
                $arr_info_1[$num_data]['platform_id'] = $platform_id;
                $arr_info_1[$num_data]['create_time'] = date('Y-m-d H:i:s');

                $campaign_id= isset($p_v['广告组id']) ? trim(str_replace('"','',$p_v['广告组id'])) : '' ;
                $campaign_name = isset($p_v['广告组'])?addslashes($p_v['广告组']):'' ;
                $error_log_arr['campaign_id'][] = $campaign_id.'('.$campaign_name.')';
            }
            $num_data++;

        }

        $result_data['arr_info'] = $arr_info;
        $result_data['arr_info_1'] = $arr_info_1;
        $result_data['arr_del_tg'] = array_unique($arr_del_tg);

        self::saveHandWorkError($error_log_arr,'campaign_id',$platform_id,'今日头条');

        return $result_data;

    }

    // Tiktok 推广
    public static function tiktokDataProcess($plat_config,$platform_id,$arr_data){
        $result_data = [];
        $arr_del_tg = [];
        $arr_info = [];
        $arr_info_1 = [];
        $num_data = 0;
        $app_id = '';
        $os_id = '';
        $error_log_arr = [];
        foreach ($arr_data as $p_k => $p_v) {
//            if(count($p_v)<6){
//                continue;
//            }

            $timestamp = isset($p_v['Date']) ? date('Y-m-d',strtotime($p_v['Date'])) : '';
            if (empty($timestamp)){
                continue;
            }

            if($p_v['Cost']==0){
                continue;
            }
            $num = 0;
            foreach ($plat_config as $k => $v) {
                if (isset($p_v['Campaign ID']) && !empty($p_v['Campaign ID'])) {
                    $campaign_id = str_replace(',','',number_format($p_v['Campaign ID']));
                    if ($campaign_id == $v['campaign_id']) {
                        $app_id = $v['app_id'];
                        $os_id = $v['os_id'];
                        $num = 1;
                        break;
                    }

                }
            }
            if($num ==1){
                $arr_del_tg[] = $timestamp;
                $arr_info[$num_data]['date'] = $timestamp;
                $arr_info[$num_data]['ad_id'] = isset($p_v['Campaign ID']) ? str_replace(',','',number_format($p_v['Campaign ID'])) : '' ;
                $arr_info[$num_data]['ad_name'] = isset($p_v['Campaign Name'])?addslashes($p_v['Campaign Name']):'' ;
                $arr_info[$num_data]['app_id'] = $app_id;
                $arr_info[$num_data]['os_id'] = $os_id;
                $arr_info[$num_data]['cost'] = isset($p_v['Cost'])?floatval($p_v['Cost']):0 ;
                $arr_info[$num_data]['platform_id'] = $platform_id;
                $arr_info[$num_data]['create_time'] = date('Y-m-d H:i:s');

            }else{

                $arr_del_tg[] = $timestamp;
                $arr_info_1[$num_data]['date'] = $timestamp;
                $arr_info_1[$num_data]['ad_id'] = isset($p_v['Campaign ID']) ? str_replace(',','',number_format($p_v['Campaign ID'])) : '' ;
                $arr_info_1[$num_data]['ad_name'] = isset($p_v['Campaign Name'])?addslashes($p_v['Campaign Name']):'' ;
                $arr_info_1[$num_data]['app_id'] = 0;
                $arr_info_1[$num_data]['os_id'] = 0;
                $arr_info_1[$num_data]['cost'] = isset($p_v['Cost'])?floatval($p_v['Cost']):0 ;
                $arr_info_1[$num_data]['platform_id'] = $platform_id;
                $arr_info_1[$num_data]['create_time'] = date('Y-m-d H:i:s');


                $campaign_id = isset($p_v['Campaign ID']) ? str_replace(',','',number_format($p_v['Campaign ID'])) : '' ;
                $campaign_name = isset($p_v['Campaign Name'])?addslashes($p_v['Campaign Name']):'' ;

                $error_log_arr['campaign_id'][] = $campaign_id.'('.$campaign_name.')';
            }
            $num_data++;

        }

        $result_data['arr_info'] = $arr_info;
        $result_data['arr_info_1'] = $arr_info_1;
        $result_data['arr_del_tg'] = array_unique($arr_del_tg);

        self::saveHandWorkError($error_log_arr,'campaign_id',$platform_id,'Tiktok');

        return $result_data;

    }

    // snapchat 推广
    public static function snapchatDataProcess($plat_config,$platform_id,$arr_data){
        $result_data = [];
        $arr_del_tg = [];
        $arr_info = [];
        $arr_info_1 = [];
        $num_data = 0;
        $app_id = '';
        $os_id = '';
        $error_log_arr = [];
        foreach ($arr_data as $p_k => $p_v) {
//            if(count($p_v)<6){
//                continue;
//            }
            $timestamp = isset($p_v['Start time']) ? date('Y-m-d',strtotime($p_v['Start time'])) : '';
            if (empty($timestamp)){
                continue;
            }

            if($p_v['Spend']==0){
                continue;
            }
            $num = 0;
            foreach ($plat_config as $k => $v) {
                if (isset($p_v['ID']) && !empty($p_v['ID'])) {
                    $campaign_id = trim($p_v['ID']);
                    if ($campaign_id == $v['campaign_id']) {
                        $app_id = $v['app_id'];
                        $os_id = $v['os_id'];
                        $num = 1;
                        break;
                    }

                }
            }
            if($num ==1){
                $arr_del_tg[] = $timestamp;
                $arr_info[$num_data]['date'] = $timestamp;
                $arr_info[$num_data]['ad_id'] = isset($p_v['ID']) ? trim($p_v['ID']) : '' ;
                $arr_info[$num_data]['ad_name'] = isset($p_v['Campaign Name'])?addslashes($p_v['Campaign Name']):'' ;
                $arr_info[$num_data]['app_id'] = $app_id;
                $arr_info[$num_data]['os_id'] = $os_id;
                $arr_info[$num_data]['cost'] = isset($p_v['Spend'])?floatval($p_v['Spend']):0 ;
                $arr_info[$num_data]['platform_id'] = $platform_id;
                $arr_info[$num_data]['create_time'] = date('Y-m-d H:i:s');

            }else{

                $arr_del_tg[] = $timestamp;
                $arr_info_1[$num_data]['date'] = $timestamp;
                $arr_info_1[$num_data]['ad_id'] = isset($p_v['ID']) ? trim($p_v['ID']) : '' ;
                $arr_info_1[$num_data]['ad_name'] = isset($p_v['Campaign Name'])?addslashes($p_v['Campaign Name']):'' ;
                $arr_info_1[$num_data]['app_id'] = 0;
                $arr_info_1[$num_data]['os_id'] = 0;
                $arr_info_1[$num_data]['cost'] = isset($p_v['Spend'])?floatval($p_v['Spend']):0 ;
                $arr_info_1[$num_data]['platform_id'] = $platform_id;
                $arr_info_1[$num_data]['create_time'] = date('Y-m-d H:i:s');

                $campaign_id = isset($p_v['ID']) ? trim($p_v['ID']) : '' ;
                $campaign_name = isset($p_v['Campaign Name'])?addslashes($p_v['Campaign Name']):'' ;

                $error_log_arr['campaign_id'][] = $campaign_id.'('.$campaign_name.')';
            }
            $num_data++;

        }

        $result_data['arr_info'] = $arr_info;
        $result_data['arr_info_1'] = $arr_info_1;
        $result_data['arr_del_tg'] = array_unique($arr_del_tg);

        self::saveHandWorkError($error_log_arr,'campaign_id',$platform_id,'snapchat');

        return $result_data;

    }

    // adcolony 推广
    public static function adcolonyDataProcess($plat_config,$platform_id,$arr_data){
        $result_data = [];
        $arr_del_tg = [];
        $arr_info = [];
        $arr_info_1 = [];
        $num_data = 0;
        $app_id = '';
        $os_id = '';
        $error_log_arr = [];
        foreach ($arr_data as $p_k => $p_v) {
//            if(count($p_v)<6){
//                continue;
//            }

            $timestamp = isset($p_v['Date']) ? date('Y-m-d',strtotime($p_v['Date'])) : '';
            if (empty($timestamp)){
                continue;
            }

            if($p_v['Spend ($)']==0){
                continue;
            }
            $num = 0;
            foreach ($plat_config as $k => $v) {
                if (isset($p_v['Campaign']) && !empty($p_v['Campaign'])) {
                    $campaign_id = trim($p_v['Campaign']);
                    if ($campaign_id == $v['campaign_id']) {
                        $app_id = $v['app_id'];
                        $os_id = $v['os_id'];
                        $num = 1;
                        break;
                    }

                }
            }
            if($num ==1){
                $arr_del_tg[] = $timestamp;
                $arr_info[$num_data]['date'] = $timestamp;
                $arr_info[$num_data]['ad_id'] = isset($p_v['Campaign']) ? trim($p_v['Campaign']) : '' ;
                $arr_info[$num_data]['ad_name'] = isset($p_v['Campaign Name'])?addslashes($p_v['Campaign Name']):'' ;
                $arr_info[$num_data]['app_id'] = $app_id;
                $arr_info[$num_data]['os_id'] = $os_id;
                $arr_info[$num_data]['cost'] = isset($p_v['Spend ($)'])?floatval($p_v['Spend ($)']):0 ;
                $arr_info[$num_data]['platform_id'] = $platform_id;
                $arr_info[$num_data]['create_time'] = date('Y-m-d H:i:s');

            }else{

                $arr_del_tg[] = $timestamp;
                $arr_info_1[$num_data]['date'] = $timestamp;
                $arr_info_1[$num_data]['ad_id'] = isset($p_v['Campaign']) ? trim($p_v['Campaign']) : '' ;
                $arr_info_1[$num_data]['ad_name'] = isset($p_v['Campaign Name'])?addslashes($p_v['Campaign Name']):'' ;
                $arr_info_1[$num_data]['app_id'] = 0;
                $arr_info_1[$num_data]['os_id'] = 0;
                $arr_info_1[$num_data]['cost'] = isset($p_v['Spend ($)'])?floatval($p_v['Spend ($)']):0 ;
                $arr_info_1[$num_data]['platform_id'] = $platform_id;
                $arr_info_1[$num_data]['create_time'] = date('Y-m-d H:i:s');


                $campaign_id = isset($p_v['Campaign']) ? trim($p_v['Campaign']) : '' ;
                $campaign_name = isset($p_v['Campaign Name'])?addslashes($p_v['Campaign Name']):'' ;

                $error_log_arr['campaign_id'][] = $campaign_id.'('.$campaign_name.')';

            }
            $num_data++;

        }

        $result_data['arr_info'] = $arr_info;
        $result_data['arr_info_1'] = $arr_info_1;
        $result_data['arr_del_tg'] = array_unique($arr_del_tg);

        self::saveHandWorkError($error_log_arr,'campaign_id',$platform_id,'adcolony');

        return $result_data;

    }

    // Adwords 推广
    public static function adwordsDataProcess($plat_config,$platform_id,$arr_data){
        $result_data = [];
        $arr_del_tg = [];
        $arr_info = [];
        $arr_info_1 = [];
        $num_data = 0;
        $app_id = '';
        $os_id = '';
        $error_log_arr = [];
        foreach ($arr_data as $p_k => $p_v) {
//            if(count($p_v)<6){
//                continue;
//            }

            $time = isset($p_v['天']) ? $p_v['天'] : '';
            $d = 25569;
            $t= 24*60*60;
            $timestamp = gmdate('Y-m-d',($time-$d)*$t);

            if (empty($timestamp)){
                continue;
            }

            if($p_v['费用']==0){
                continue;
            }
            $num = 0;
            foreach ($plat_config as $k => $v) {
                if (isset($p_v['广告系列 ID']) && !empty($p_v['广告系列 ID'])) {
                    $campaign_id = trim($p_v['广告系列 ID']);
                    if ($campaign_id == $v['campaign_id']) {
                        $app_id = $v['app_id'];
                        $os_id = $v['os_id'];
                        $num = 1;
                        break;
                    }

                }
            }
            if($num ==1){
                $arr_del_tg[] = $timestamp;
                $arr_info[$num_data]['date'] = $timestamp;
                $arr_info[$num_data]['ad_id'] = isset($p_v['广告系列 ID']) ? trim($p_v['广告系列 ID']) : '' ;
                $arr_info[$num_data]['ad_name'] = isset($p_v['广告系列'])?addslashes($p_v['广告系列']):'' ;
                $arr_info[$num_data]['app_id'] = $app_id;
                $arr_info[$num_data]['os_id'] = $os_id;
                $arr_info[$num_data]['cost'] = isset($p_v['费用'])?floatval($p_v['费用']):0 ;
                $arr_info[$num_data]['platform_id'] = $platform_id;
                $arr_info[$num_data]['create_time'] = date('Y-m-d H:i:s');

            }else{

                $arr_del_tg[] = $timestamp;
                $arr_info_1[$num_data]['date'] = $timestamp;
                $arr_info_1[$num_data]['ad_id'] = isset($p_v['广告系列 ID']) ? trim($p_v['广告系列 ID']) : '' ;
                $arr_info_1[$num_data]['ad_name'] = isset($p_v['广告系列'])?addslashes($p_v['广告系列']):'' ;
                $arr_info_1[$num_data]['app_id'] = 0;
                $arr_info_1[$num_data]['os_id'] = 0;
                $arr_info_1[$num_data]['cost'] = isset($p_v['费用'])?floatval($p_v['费用']):0 ;
                $arr_info_1[$num_data]['platform_id'] = $platform_id;
                $arr_info_1[$num_data]['create_time'] = date('Y-m-d H:i:s');

                $campaign_id = isset($p_v['广告系列 ID']) ? trim($p_v['广告系列 ID']) : '' ;
                $campaign_name = isset($p_v['广告系列'])?addslashes($p_v['广告系列']):'' ;

                $error_log_arr['campaign_id'][] = $campaign_id.'('.$campaign_name.')';
            }
            $num_data++;

        }

        $result_data['arr_info'] = $arr_info;
        $result_data['arr_info_1'] = $arr_info_1;
        $result_data['arr_del_tg'] = array_unique($arr_del_tg);

        self::saveHandWorkError($error_log_arr,'campaign_id',$platform_id,'Adwords');

        return $result_data;

    }

    // vungle 推广
    public static function vungleDataProcess($plat_config,$platform_id,$arr_data){
        $result_data = [];
        $arr_del_tg = [];
        $arr_info = [];
        $arr_info_1 = [];
        $num_data = 0;
        $app_id = '';
        $os_id = '';
        $error_log_arr = [];
        foreach ($arr_data as $p_k => $p_v) {
//            if(count($p_v)<6){
//                continue;
//            }
            $timestamp = isset($p_v['Date']) ? date('Y-m-d',strtotime($p_v['Date'])) : '';
            if (empty($timestamp)){
                continue;
            }

            if($p_v['Installs']==0){
                continue;
            }
            $num = 0;
            foreach ($plat_config as $k => $v) {
                if (isset($p_v['Id']) && !empty($p_v['Id'])) {
                    $campaign_id = trim($p_v['Id']);
                    if ($campaign_id == $v['campaign_id']) {
                        $app_id = $v['app_id'];
                        $os_id = $v['os_id'];
                        $num = 1;
                        break;
                    }

                }
            }
            if($num ==1){
                $arr_del_tg[] = $timestamp;
                $arr_info[$num_data]['date'] = $timestamp;
                $arr_info[$num_data]['ad_id'] = isset($p_v['Id']) ? trim($p_v['Id']) : '' ;
                $arr_info[$num_data]['ad_name'] = isset($p_v['Name'])?addslashes($p_v['Name']):'' ;
                $arr_info[$num_data]['app_id'] = $app_id;
                $arr_info[$num_data]['os_id'] = $os_id;
                $arr_info[$num_data]['cost'] = isset($p_v['Installs'])?floatval($p_v['Installs']) * floatval($p_v['Rate']):0 ;
                $arr_info[$num_data]['platform_id'] = $platform_id;
                $arr_info[$num_data]['create_time'] = date('Y-m-d H:i:s');

            }else{

                $arr_del_tg[] = $timestamp;
                $arr_info_1[$num_data]['date'] = $timestamp;
                $arr_info_1[$num_data]['ad_id'] = isset($p_v['Id']) ? trim($p_v['Id']) : '' ;
                $arr_info_1[$num_data]['ad_name'] = isset($p_v['Name'])?addslashes($p_v['Name']):'' ;
                $arr_info_1[$num_data]['app_id'] = 0;
                $arr_info_1[$num_data]['os_id'] = 0;
                $arr_info_1[$num_data]['cost'] = isset($p_v['Installs'])?floatval($p_v['Installs']) * floatval($p_v['Rate']):0 ;
                $arr_info_1[$num_data]['platform_id'] = $platform_id;
                $arr_info_1[$num_data]['create_time'] = date('Y-m-d H:i:s');

                $campaign_id = isset($p_v['Id']) ? trim($p_v['Id']) : '' ;
                $campaign_name = isset($p_v['Name'])?addslashes($p_v['Name']):'' ;

                $error_log_arr['campaign_id'][] = $campaign_id.'('.$campaign_name.')';
            }
            $num_data++;

        }

        $result_data['arr_info'] = $arr_info;
        $result_data['arr_info_1'] = $arr_info_1;
        $result_data['arr_del_tg'] = array_unique($arr_del_tg);

        self::saveHandWorkError($error_log_arr,'campaign_id',$platform_id,'vungle');

        return $result_data;

    }

    // unity 推广
    public static function unityDataProcess($plat_config,$platform_id,$arr_data){
        $result_data = [];
        $arr_del_tg = [];
        $arr_info = [];
        $arr_info_1 = [];
        $num_data = 0;
        $app_id = '';
        $os_id = '';
        $error_log_arr = [];
        foreach ($arr_data as $p_k => $p_v) {
//            if(count($p_v)<6){
//                continue;
//            }

            $timestamp = isset($p_v['timestamp']) ? date('Y-m-d',strtotime($p_v['timestamp'])) : '';
            if (empty($timestamp)){
                continue;
            }

            if($p_v['spend']==0){
                continue;
            }
            $num = 0;
            foreach ($plat_config as $k => $v) {
                if (isset($p_v['target store id']) && !empty($p_v['target store id'])) {
                    $campaign_id = trim(str_replace('"','',$p_v['target store id']));
                    if ($campaign_id == $v['application_id']) {
                        $app_id = $v['app_id'];
                        $os_id = $v['os_id'];
                        $num = 1;
                        break;
                    }

                }
            }
            if($num ==1){
                $arr_del_tg[] = $timestamp;
                $arr_info[$num_data]['date'] = $timestamp;
                $arr_info[$num_data]['ad_id'] = isset($p_v['campaign id']) ? trim(str_replace('"','',$p_v['campaign id'])) : '' ;
                $arr_info[$num_data]['ad_name'] = isset($p_v['campaign name'])?addslashes($p_v['campaign name']):'' ;
                $arr_info[$num_data]['platform_app_id'] = isset($p_v['target store id']) ? trim(str_replace('"','',$p_v['target store id'])) : '' ;
                $arr_info[$num_data]['platform_app_name'] = isset($p_v['target name'])?addslashes($p_v['target name']):'' ;
                $arr_info[$num_data]['app_id'] = $app_id;
                $arr_info[$num_data]['os_id'] = $os_id;
                $arr_info[$num_data]['cost'] = isset($p_v['spend'])?floatval($p_v['spend']):0 ;
                $arr_info[$num_data]['platform_id'] = $platform_id;
                $arr_info[$num_data]['create_time'] = date('Y-m-d H:i:s');

            }else{

                $arr_del_tg[] = $timestamp;
                $arr_info_1[$num_data]['date'] = $timestamp;
                $arr_info_1[$num_data]['ad_id'] = isset($p_v['campaign id']) ? trim(str_replace('"','',$p_v['campaign id'])) : '' ;
                $arr_info_1[$num_data]['ad_name'] = isset($p_v['campaign name'])?addslashes($p_v['campaign name']):'' ;
                $arr_info_1[$num_data]['platform_app_id'] = isset($p_v['target store id']) ? trim(str_replace('"','',$p_v['target store id'])) : '' ;
                $arr_info_1[$num_data]['platform_app_name'] = isset($p_v['target name'])?addslashes($p_v['target name']):'' ;
                $arr_info_1[$num_data]['app_id'] = 0;
                $arr_info_1[$num_data]['os_id'] = 0;
                $arr_info_1[$num_data]['cost'] = isset($p_v['spend'])?floatval($p_v['spend']):0 ;
                $arr_info_1[$num_data]['platform_id'] = $platform_id;
                $arr_info_1[$num_data]['create_time'] = date('Y-m-d H:i:s');

                $target_id = isset($p_v['target store id']) ? trim(str_replace('"','',$p_v['target store id'])) : '';
                $target_name = isset($p_v['target name'])?addslashes($p_v['target name']):'' ;

                $error_log_arr['target_id'][] = $target_id.'('.$target_name.')';
            }
            $num_data++;

        }

        $result_data['arr_info'] = $arr_info;
        $result_data['arr_info_1'] = $arr_info_1;
        $result_data['arr_del_tg'] = array_unique($arr_del_tg);

        self::saveHandWorkError($error_log_arr,'target_id',$platform_id,'unity');

        return $result_data;

    }

    // facebook 推广
    public static function facebookDataProcess($plat_config,$platform_id,$arr_data){
        $result_data = [];
        $arr_del_tg = [];
        $arr_info = [];
        $arr_info_1 = [];
        $num_data = 0;
        $app_id = '';
        $os_id = '';
        $error_log_arr = [];
        foreach ($arr_data as $p_k => $p_v) {
//            if(count($p_v)<6){
//                continue;
//            }
            $timestamp = isset($p_v['Day']) ? date('Y-m-d',strtotime($p_v['Day'])) : '';
            if (empty($timestamp)){
                continue;
            }

            if($p_v['Amount Spent (USD)']==0){
                continue;
            }
            $num = 0;
            foreach ($plat_config as $k => $v) {
                if (isset($p_v['Campaign ID']) && !empty($p_v['Campaign ID'])) {
                    $campaign_id = trim($p_v['Campaign ID']);
                    if ($campaign_id == $v['campaign_id']) {
                        $app_id = $v['app_id'];
                        $os_id = $v['os_id'];
                        $num = 1;
                        break;
                    }

                }
            }
            if($num ==1){
                $arr_del_tg[] = $timestamp;
                $arr_info[$num_data]['date'] = $timestamp;
                $arr_info[$num_data]['ad_id'] = isset($p_v['Campaign ID']) ? trim($p_v['Campaign ID']) : '' ;
                $arr_info[$num_data]['ad_name'] = isset($p_v['Campaign Name'])?addslashes($p_v['Campaign Name']):'' ;
                $arr_info[$num_data]['app_id'] = $app_id;
                $arr_info[$num_data]['os_id'] = $os_id;
                $arr_info[$num_data]['cost'] = isset($p_v['Amount Spent (USD)'])?floatval($p_v['Amount Spent (USD)']):0 ;
                $arr_info[$num_data]['platform_id'] = $platform_id;
                $arr_info[$num_data]['create_time'] = date('Y-m-d H:i:s');

            }else{

                $arr_del_tg[] = $timestamp;
                $arr_info_1[$num_data]['date'] = $timestamp;
                $arr_info_1[$num_data]['ad_id'] = isset($p_v['Campaign ID']) ? trim($p_v['Campaign ID']) : '' ;
                $arr_info_1[$num_data]['ad_name'] = isset($p_v['Campaign Name'])?addslashes($p_v['Campaign Name']):'' ;
                $arr_info_1[$num_data]['app_id'] = 0;
                $arr_info_1[$num_data]['os_id'] = 0;
                $arr_info_1[$num_data]['cost'] = isset($p_v['Amount Spent (USD)'])?floatval($p_v['Amount Spent (USD)']):0 ;
                $arr_info_1[$num_data]['platform_id'] = $platform_id;
                $arr_info_1[$num_data]['create_time'] = date('Y-m-d H:i:s');

                $campaign_id = isset($p_v['Campaign ID']) ? trim($p_v['Campaign ID']) : '' ;
                $campaign_name = isset($p_v['Campaign Name'])?addslashes($p_v['Campaign Name']):'' ;

                $error_log_arr['campaign_id'][] = $campaign_id.'('.$campaign_name.')';
            }
            $num_data++;

        }

        $result_data['arr_info'] = $arr_info;
        $result_data['arr_info_1'] = $arr_info_1;
        $result_data['arr_del_tg'] = array_unique($arr_del_tg);

        self::saveHandWorkError($error_log_arr,'campaign_id',$platform_id,'facebook');

        return $result_data;

    }

    // applovin 推广
    public static function applovinDataProcess($plat_config,$platform_id,$arr_data){
        $result_data = [];
        $arr_del_tg = [];
        $arr_info = [];
        $arr_info_1 = [];
        $num_data = 0;
        $app_id = '';
        $os_id = '';
        $error_log_arr = [];
        foreach ($arr_data as $p_k => $p_v) {
//            if(count($p_v)<6){
//                continue;
//            }

            $timestamp = isset($p_v['Day']) ? date('Y-m-d',strtotime($p_v['Day'])) : '';
            if (empty($timestamp)){
                continue;
            }

            if($p_v['Spend']==0){
                continue;
            }


            $num = 0;
            foreach ($plat_config as $k => $v) {
                if (isset($p_v['Campaign Package Name']) && !empty($p_v['Campaign Package Name']) && isset($p_v['Platform']) && !empty($p_v['Platform'])) {
                    $campaign_name = trim($p_v['Campaign Package Name']);
                    $platform = trim($p_v['Platform']);

                    if($v['os_id'] ==1){
                        $os = 'ios';
                    }elseif ($v['os_id'] ==2) {
                        $os = 'android';
                    }elseif ($v['os_id'] ==3) {
                        $os = 'h5';
                    }else{
                        $os = 'amazon';
                    }

                    if ($campaign_name.'_'.$platform == $v['application_id'].'_'.$os) {
                        $app_id = $v['app_id'];
                        $os_id = $v['os_id'];
                        $num = 1;
                        break;
                    }

                }
            }
            if($num ==1){
                $arr_del_tg[] = $timestamp;
                $arr_info[$num_data]['date'] = $timestamp;
                $arr_info[$num_data]['ad_id'] = isset($p_v['Campaign Package Name']) ? addslashes(trim($p_v['Campaign Package Name'])) : '' ;
                $arr_info[$num_data]['ad_name'] = isset($p_v['Campaign'])?addslashes($p_v['Campaign']):'' ;
                $arr_info[$num_data]['app_id'] = $app_id;
                $arr_info[$num_data]['os_id'] = $os_id;
                $arr_info[$num_data]['cost'] = isset($p_v['Spend'])?floatval($p_v['Spend']):0 ;
                $arr_info[$num_data]['platform_id'] = $platform_id;
                $arr_info[$num_data]['create_time'] = date('Y-m-d H:i:s');

            }else{

                $arr_del_tg[] = $timestamp;
                $arr_info_1[$num_data]['date'] = $timestamp;
                $arr_info_1[$num_data]['ad_id'] = isset($p_v['Campaign Package Name']) ? addslashes(trim($p_v['Campaign Package Name'])) : '' ;
                $arr_info_1[$num_data]['ad_name'] = isset($p_v['Campaign'])?addslashes($p_v['Campaign']):'' ;
                $arr_info_1[$num_data]['app_id'] = 0;
                $arr_info_1[$num_data]['os_id'] = 0;
                $arr_info_1[$num_data]['cost'] = isset($p_v['Spend'])?floatval($p_v['Spend']):0 ;
                $arr_info_1[$num_data]['platform_id'] = $platform_id;
                $arr_info_1[$num_data]['create_time'] = date('Y-m-d H:i:s');

                $target_id = isset($p_v['Campaign Package Name']) ? addslashes(trim($p_v['Campaign Package Name'])) : '' ;
                $target_name = isset($p_v['Campaign'])?addslashes($p_v['Campaign']):'' ;

                $error_log_arr['target_id'][] = $target_id.'('.$target_name.')';
            }
            $num_data++;

        }

        $result_data['arr_info'] = $arr_info;
        $result_data['arr_info_1'] = $arr_info_1;
        $result_data['arr_del_tg'] = array_unique($arr_del_tg);

        self::saveHandWorkError($error_log_arr,'target_id',$platform_id,'applovin');

        return $result_data;

    }

    // ironsourse 推广
    public static function ironsourseDataProcess($plat_config,$platform_id,$arr_data){
        $result_data = [];
        $arr_del_tg = [];
        $arr_info = [];
        $arr_info_1 = [];
        $num_data = 0;
        $app_id = '';
        $os_id = '';
        $error_log_arr = [];
        foreach ($arr_data as $p_k => $p_v) {
//            if(count($p_v)<6){
//                continue;
//            }

            $timestamp = isset($p_v['date']) ? date('Y-m-d',strtotime($p_v['date'])) : '';
            if (empty($timestamp)){
                continue;
            }

            if($p_v['spend']==0){
                continue;
            }
            $num = 0;
            foreach ($plat_config as $k => $v) {
                if (isset($p_v['campaign_name']) && !empty($p_v['campaign_name'])) {
                    $campaign_id = trim($p_v['campaign_name']);
                    if ($campaign_id == $v['campaign_name']) {
                        $app_id = $v['app_id'];
                        $os_id = $v['os_id'];
                        $num = 1;
                        break;
                    }

                }
            }
            if($num ==1){
                $arr_del_tg[] = $timestamp;
                $arr_info[$num_data]['date'] = $timestamp;
                $arr_info[$num_data]['ad_id'] = '' ;
                $arr_info[$num_data]['ad_name'] = isset($p_v['campaign_name'])?addslashes($p_v['campaign_name']):'' ;
                $arr_info[$num_data]['app_id'] = isset($p_v['title_name'])?addslashes($p_v['title_name']):'';
                $arr_info[$num_data]['os_id'] = $os_id;
                $arr_info[$num_data]['cost'] = isset($p_v['spend'])?floatval($p_v['spend']):0 ;
                $arr_info[$num_data]['platform_id'] = $platform_id;
                $arr_info[$num_data]['create_time'] = date('Y-m-d H:i:s');

            }else{

                $arr_del_tg[] = $timestamp;
                $arr_info_1[$num_data]['date'] = $timestamp;
                $arr_info_1[$num_data]['ad_id'] = '' ;
                $arr_info_1[$num_data]['ad_name'] = isset($p_v['campaign_name'])?addslashes(trim($p_v['campaign_name'])):'' ;
                $arr_info_1[$num_data]['app_id'] = 0;
                $arr_info_1[$num_data]['os_id'] = 0;
                $arr_info_1[$num_data]['cost'] = isset($p_v['spend'])?floatval($p_v['spend']):0 ;
                $arr_info_1[$num_data]['platform_id'] = $platform_id;
                $arr_info_1[$num_data]['create_time'] = date('Y-m-d H:i:s');

                $campaign_name = isset($p_v['campaign_name'])?addslashes(trim($p_v['campaign_name'])):'' ;

                $error_log_arr['campaign_name'][] = $campaign_name;
            }
            $num_data++;

        }

        $result_data['arr_info'] = $arr_info;
        $result_data['arr_info_1'] = $arr_info_1;
        $result_data['arr_del_tg'] = array_unique($arr_del_tg);

        self::saveHandWorkError($error_log_arr,'campaign_name',$platform_id,'ironsourse');

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

            DataImportImp::saveDataErrorLog(4,$source_id,$source_name,4,implode(';',$error_msg_array));
            // 发送邮件
//            CommonFunction::sendMail($error_msg_mail,$source_name.'推广平台数据处理error');
        }
    }


}