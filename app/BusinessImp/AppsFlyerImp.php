<?php

namespace App\BusinessImp;

use App\BusinessLogic\ApplicationLogic;
use App\BusinessLogic\ChannelLogic;
use App\BusinessLogic\CommonLogic;
use App\BusinessLogic\DeveloperLogic;
use App\BusinessLogic\OperationLogLogic;
use App\BusinessLogic\PlatformLogic;
use App\Common\ApiResponseFactory;
use App\Common\Service;
use function GuzzleHttp\Psr7\str;
use Illuminate\Support\Facades\App;
use Illuminate\Support\Facades\DB;
use App\BusinessLogic\UserLogic;
use Illuminate\Support\Facades\Redis;
use Illuminate\Support\Facades\Request;


class AppsFlyerImp extends ApiBaseImp
{
    /**
     * 应用信息列表
     * @param $params array 请求数据
     */
    public static function getAppsFlyerDeviceList($params)
    {

        // todo 用户ID
        $userid = isset($params['guid']) ? $params['guid'] : $_SESSION['erm_data']['guid'];
//        $userid = 2;
        ini_set("memory_limit","4096M");
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        // 开始结束时间
        $stime = isset($params['start_time']) ? $params['start_time'] : '';
        $etime = isset($params['end_time']) ? $params['end_time'] : '';
        if(!$stime || !$etime){
            ApiResponseFactory::apiResponse([],[],751);
        }

        $is_export = isset($params['is_export']) ? $params['is_export'] : '';
        // todo 设备数据表名称
        $search_table = 'zplay_appsflyer_device_num';

        $sql =' where 1=1 ';

        //返回用户下可查询的应用ID
        $map1['id'] = $userid;
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        $power = []; // 为空 则拥有全部查询权限
        if($userInfo[0]['app_permission'] != -2){
            $power = $userInfo[0]['app_permission'];
        }

        $app_info_list = [];
        $developer_data_list = [];
        $table_title = [];
        $app = isset($params['app_id']) ? $params['app_id'] : -2;
        if($app){
            $arr_app = [];
            if ($app != -2){
                $arr_app = $app;
                $sql .=" and app_id in ({$arr_app}) ";
            }elseif($power){
                $sql .=" and app_id in ($power) ";
            }
        }


        $platform_id = isset($params['platform_id']) ? $params['platform_id'] : -2;
        if ($platform_id) {
            $table_title['developer_name'] = '开发者名称';
            if ($platform_id != -2){
                $platform_id_list = explode(',',$platform_id);
                $platform_id_str = implode("','",$platform_id_list);
                $sql .=" and platform_id in ('{$platform_id_str}') ";
            }
        }

        // 分区查询
        $partition = '';
        $all_month_arr = Service::dateMonthsSections($params['start_time'],$params['end_time']);
        $all_month = [];
        if ($all_month_arr){
            foreach ($all_month_arr as $month_srt){
                $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
            }
            if ($all_month){
                $partition = " partition (".implode(',',$all_month).")";
            }
        }


        //显示维度字段
        $select_adid = [];
        $select_difa = [];

        // 数据指标
        $target_id = isset($params['target_id']) ? $params['target_id'] : '';
        if(!$target_id) ApiResponseFactory::apiResponse([],[],1008);
        $target_id = explode(',',$target_id);
        $table_title = [];
        $where_sql_adid = '';
        $where_sql_idfa = '';
        foreach ($target_id as $target){
            if($target == 1){
                $table_title[] = 'madid';
                $select_adid[] = 'advertising_id as madid';
                $select_difa[] = 'idfa';
                $where_sql_adid = " and advertising_id <> '' ";
                $where_sql_idfa = " and idfa <> '' ";
            }elseif($target == 2){
                $table_title[] = 'country';
                $select_adid[] = 'country_code as country';
                $select_difa[] = 'country_code';
            }elseif($target == 3){
                $table_title[] = 'st';
                $select_adid[] = 'state as st';
                $select_difa[] = 'state';
            }elseif($target == 4){
                $table_title[] = 'ct';
                $select_adid[] = 'city as ct';
                $select_difa[] = 'city';
            }elseif($target == 5){
                $table_title[] = 'zip';
                $select_adid[] = 'postal_code as zip';
                $select_difa[] = 'postal_code';
            }elseif($target == 6){
                $table_title[] = 'ip';
                $select_adid[] = 'ip';
                $select_difa[] = 'ip';
            }
        }



        $startTime = $params['start_time'];
        $endTime = $params['end_time'];
        $time_sql = " and date between '{$startTime}' and '{$endTime}'";

        $select_adid_str = implode(',',$select_adid);
        $select_difa_str = implode(',',$select_difa);

        $adid_sql = "select ".$select_adid_str." from {$search_table} {$partition} ".$sql.$where_sql_adid.$time_sql;
        $difa_sql = "select ".$select_difa_str." from {$search_table} {$partition} ".$sql.$where_sql_idfa.$time_sql;

        $searchSql = " ( ".$adid_sql ." limit 1000000 ) ". " union all " . " ( ".$difa_sql." limit 1000000) ";
        $searchSql = " select *  from (" . $searchSql .") a";
        // todo 查询sql
//        var_dump($searchSql);
//        var_dump($countSql);

//        $return_data = [];
//        if (!$is_export) {
//            $one_countSql = " select * from {$search_table} {$partition} ".$sql.$time_sql." limit 2400000";
//            $countSql = " select count(1) as count from ( ".$one_countSql." ) a";
//            $count = DB::selectOne($countSql);
//            $count = Service::data($count);
//
//            $return_data['total_count'] = $count['count'];
//            $return_data['total_file'] = ceil($count['count'] / 50000);
//
//            ApiResponseFactory::apiResponse($return_data, []);
//        }

        $all_data = [];
        $answer = [];
        $answer = DB::select($searchSql);
        $answer = Service::data($answer);

//        var_dump(count($answer));die;
        $step = array();
        if ($answer){
            $answer_list = [];
            foreach ($answer as $aa_k => $aa_v) {
                foreach ($table_title as $t_k => $t_v){
                    if (key_exists($t_v,$aa_v)){
                        $answer_list[$aa_k][] = $aa_v[$t_v];
                    }
                }
            }

            $i = 0;
            foreach ($answer_list as $kkkk => $answer_info) {
                if ($kkkk % 50000 == 0) $i++;
                if ($answer_info) {
                    $step[$i][] = $answer_info;
                }
            }
        }

//        if ($step){
//            $report_name = isset($params['report_name']) ? $params['report_name'] : "开发者分成数据";
//            $title = iconv('utf-8','gb2312',implode(',', array_values($table_title)));
//            foreach ($step as $step_key => $step_value){
//                $values = is_array($step_value) ? $step_value : [];
//                $string =Service::csv_output_str($title."\n", $values);
//                $filename = iconv('utf-8','gb2312',$report_name).'-'.date('Ymd').'.csv'; //设置文件名
//                $result = Service::create_export_csv($filename,$string); //导出
//                if ($result){
//                    file_put_contents('../appsflyer_device.csv',$result);
//                }
//            }
//        }

        $return_data = [];
        if ($step) {
            foreach ($step as $step_key => $step_value) {
                $values = is_array($step_value) ? $step_value : [];
                $filename = date('YmdHis') . '-'.$step_key;
                //设置文件名
                if ($values) {
                    // 打开文件资源，不存在则创建
                    $fp = fopen('./storage/appsflyer/' . $filename.'.csv', 'a');
                    // 处理头部标题
                    $header = implode(',', $table_title) . PHP_EOL;
                    // 处理内容
                    $content = '';
                    foreach ($values as $k => $v) {
                        $content .= implode(',', $v) . PHP_EOL;
                    }
                    // 拼接
                    $csv = $header . $content;
                    // 写入并关闭资源
                    fwrite($fp, $csv);
                    fclose($fp);
                    $return_data[$step_key]['file_name'] = $filename;
//                    $return_data[$step_key]['file_url'] = env('AF_FILE_URL').'/storage/appsflyer/'.$filename.'.csv';
                    $return_data[$step_key]['file_url'] = '/storage/appsflyer/'.$filename.'.csv';
                }
            }
        }
        sort($return_data);
        ApiResponseFactory::apiResponse($return_data,[]);

    }



    public static function getAppsflyerPushData($params)
    {
        $fileName = date('YmdH',time());
        $fileData = date('Y-m-d',time());
        $dir = '/data/af_push/'.$fileData;

        if (!is_dir($dir)) {
            mkdir($dir,0777,true);
        }
        $logFilename = $dir.'/'.$fileName.'.log';
        //生成日志
        file_put_contents( $logFilename,json_encode($params) . "\n",FILE_APPEND);

//        Redis::select(1);
//        if ($params) {
//
//            $appsflyer_key = env('REDIS_APPSFLYER_KEYS');
//            Redis::rpush($appsflyer_key, json_encode($params));
//
//        }
    }

}
