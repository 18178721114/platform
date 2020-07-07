<?php

namespace App\BusinessImp;

use App\BusinessLogic\CommonLogic;
use App\BusinessLogic\DataImportLogic;
use App\BusinessLogic\OperationLogLogic;
use App\Common\ApiResponseFactory;
use App\Common\Service;
use App\Common\CurlRequest;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\UserLogic;
use Illuminate\Support\Facades\DB;
use App\BusinessLogic\RoleLogic;

class DataImportImp extends ApiBaseImp
{

    /**
     * 获取渠道数据最后时间
     * @param $params array 请求数据
     */
    public static function getChannelDataTime($params)
    {
        // 必填参数判断
        $data_type = isset($params['data_type']) ? $params['data_type'] : ''; // 数据类型
        $channel_id = isset($params['channel_id']) ? $params['channel_id'] : ''; // 渠道ID

        // 为空错误提示
        if (!$data_type) ApiResponseFactory::apiResponse([],[],806);
        if (!$channel_id) ApiResponseFactory::apiResponse([],[],807);

        // 根据数据类型选择存储表
        if ($data_type == 1){
            // 统计数据
            $schema = 'tj_data';
        }else if ($data_type == 2){
            // 广告数据
            $schema = 'ad_data';
        }else if ($data_type == 3){
            // 计费数据
            $schema = 'ff_data';
        }else if ($data_type == 4){
            // 推广数据
            $schema = 'tg_data';
        }

        $table_name = 'erm_data';

        // 处理数据
        $map = [];
        $map[] = ['source_id',$channel_id];
        $fields = ['dayid'];
        $channel_data = DataImportLogic::getChannelData($schema,$table_name,$map,$fields)->orderByDesc('dayid')->first();
        $channel_data = Service::data($channel_data);
        ApiResponseFactory::apiResponse($channel_data,[]);
    }

    /**
     * 渠道数据导入
     * @param $params array 请求数据
     */
    public static function importChannelData($params)
    {
//        $params = $_POST;
        // 必填参数判断
        $data_type = isset($params['data_type']) ? $params['data_type'] : ''; // 数据类型
        $channel_id = isset($params['channel_id']) ? $params['channel_id'] : ''; // 渠道ID
        $channel_data = isset($params['channel_data']) ? $params['channel_data'] : ''; // 渠道数据

        // 为空错误提示
        if (!$data_type) ApiResponseFactory::apiResponse([],[],806);
        if (!$channel_id) ApiResponseFactory::apiResponse([],[],807);
        if (!$channel_data) ApiResponseFactory::apiResponse([],[],808);

//        $channel_data = json_decode(urldecode($channel_data),true);
        // 根据数据类型选择存储表
        if ($data_type == 1){
            // 统计数据
            $schema = 'tj_data';
        }else if ($data_type == 2){
            // 广告数据
            $schema = 'ad_data';
        }else if ($data_type == 3){
            // 计费数据
            $schema = 'ff_data';
        }else if ($data_type == 4){
            // 推广数据
            $schema = 'tg_data';
        }

        $table_name = 'erm_data';

        // 处理数据
        // 第一步，查询删除
        $app_ids = [];
        $app_names = [];
        $data_date_arr = [];
        // 待完善
        $app_id_fields = ['应用ID', 'APPID', 'APPId'];
        $app_name_fields = ['应用名称', '应用', '游戏名称', 'APP'];

        foreach ($channel_data as $key => $data_info){
            $data_date = isset($data_info['date_time']) ? $data_info['date_time'] : '';
            if (!$data_date) ApiResponseFactory::apiResponse([],[],812);

            if (strlen($data_date) <= 8){
                ApiResponseFactory::apiResponse([],[],812);
            }

            if (!$data_date) ApiResponseFactory::apiResponse([],[],812);

            if (strstr($data_date, '/')){
                if (count(explode('/',$data_date)) != 3){
                    ApiResponseFactory::apiResponse([],[],812);
                }
            }

            if (strstr($data_date, '-')) {
                if (count(explode('-', $data_date)) != 3) {
                    ApiResponseFactory::apiResponse([],[],812);
                }
            }


            if ($data_date){
                $data_date_arr[] = $data_date;
            }
            foreach ($data_info as $d_k => $d_v){
                if (in_array($d_k,$app_id_fields)){
                    if ($d_v){
                        $data_info['app_id'] = $d_v;
//                        unset($data_info[$d_k]);
//                        $app_ids[] = $d_v;
                    }
                }

                if (in_array($d_k,$app_name_fields)){
                    if ($d_v){
                        $data_info['app_name'] = $d_v;
//                        unset($data_info[$d_k]);
//                        $app_names[] = $d_v;
                    }
                }
            }
            $channel_data[$key] = $data_info;
        }
//        $app_ids = array_unique($app_ids);
//        $app_names = array_unique($app_names);
        $data_date_arr = array_unique($data_date_arr);

        $map = [];
        $map[] = ['source_id',$channel_id];
        $map['in'] = ['dayid', $data_date_arr];
//        $map['or_in'] = ['app_name', $app_names];
        $history_data_list = DataImportLogic::getChannelData($schema,$table_name,$map)->get();
        $history_data_list = Service::data($history_data_list);

        if ($history_data_list){
            DB::beginTransaction();
            $result = DataImportLogic::deleteHistoryData($schema,$table_name,$map);
            if (!$result){
                DB::rollBack();
                ApiResponseFactory::apiResponse([],[],810);
            }
            DB::commit();
        }

        // 第二部，插入数据
        $create_time = date("Y-m-d H:i:s",time());
        $insert_data = [];
        foreach ($channel_data as $key => $data_info){
            $app_id = isset($data_info['app_id']) ? $data_info['app_id'] : '';
            $app_name = isset($data_info['app_name']) ? $data_info['app_name'] : '';
            $insert_data[$key]['type'] = 1;
            $insert_data[$key]['app_id'] = $app_id;
            $insert_data[$key]['app_name'] = $app_name;
            $insert_data[$key]['source_id'] = $channel_id;
            $insert_data[$key]['json_data'] = json_encode($data_info);
            $insert_data[$key]['dayid'] = $data_info['date_time'];
            $insert_data[$key]['create_time'] = $create_time;
            $insert_data[$key]['year'] = date("Y",strtotime($data_date));
            $insert_data[$key]['month'] = date("m",strtotime($data_date));
        }

        if ($insert_data) {
            DB::beginTransaction();
            //拆分批次
            $step = array();
            $i = 0;
            foreach ($insert_data as $kkkk => $insert_data_info) {
                if ($kkkk % 1000 == 0) $i++;
                if ($insert_data_info) {
                    $step[$i][] = $insert_data_info;
                }
            }

            if ($step) {
                foreach ($step as $k => $v) {
                    $result = DataImportLogic::insertChannelData($schema, $table_name, $v);
                    if (!$result) {
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([], [], 811);
                    }
                }
            }
            DB::commit();
        }
        // 调用处理过程
        if ($data_date_arr){
            if ($data_type == 3){
                if ($channel_id == 54){
                    Artisan::call('HuaweiHandworkFfHandleProcesses' ,['dayid' => $data_date_arr]);
                }elseif ($channel_id == 31){
                    Artisan::call('MeizuHandworkFfHandleProcesses' ,['dayid' => $data_date_arr]);
                }elseif ($channel_id == 50){
                    Artisan::call('OppoHandworkFfHandleProcesses' ,['dayid' => $data_date_arr]);
                }elseif ($channel_id == 52){
                    Artisan::call('TencentHandworkFfHandleProcesses' ,['dayid' => $data_date_arr]);
                }elseif ($channel_id == 60){
                    Artisan::call('VivoHandworkFfHandleProcesses' ,['dayid' => $data_date_arr]);
                }elseif ($channel_id == 9){
                    Artisan::call('XiaomiHandworkFfHandleProcesses' ,['dayid' => $data_date_arr]);
                }elseif ($channel_id == 425){
                    Artisan::call('ZhifubaoHandworkFfHandleProcesses' ,['dayid' => $data_date_arr,'channel_id' => 425]);
                }elseif ($channel_id == 426){
                    Artisan::call('WechatHandworkFfHandleProcesses' ,['dayid' => $data_date_arr,'channel_id' => 426 ]);
                }

            }elseif ($data_type == 2) {
                if($channel_id == 49){
                    Artisan::call('a4399HandworkHandleProcesses', ['dayid' => $data_date_arr]);
                }elseif($channel_id == 27){
                    Artisan::call('BaiduHandworkHandleProcesses', ['dayid' => $data_date_arr]);
                }elseif($channel_id == 31){
                    Artisan::call('MeizuHandworkHandleProcesses', ['dayid' => $data_date_arr]);
                }elseif($channel_id == 50){
                    Artisan::call('OppoHandworkHandleProcesses', ['dayid' => $data_date_arr]);
                }elseif($channel_id == 60){
                    Artisan::call('VivoHandworkHandleProcesses', ['dayid' => $data_date_arr]);
                }elseif($channel_id == 9){
                    Artisan::call('XiaomiHandworkHandleProcesses', ['dayid' => $data_date_arr]);
                }elseif($channel_id == 54){
                    Artisan::call('HuaweiHandworkHandleProcesses', ['dayid' => $data_date_arr]);
                }elseif($channel_id == 428){
                    Artisan::call('TiktokHandworkHandleProcesses', ['dayid' => $data_date_arr]);
                }elseif($channel_id == 432){
                    Artisan::call('PangolinHandworkHandleProcesses', ['dayid' => $data_date_arr]);
                }elseif($channel_id == 434){
                    Artisan::call('TencentHandworkHandleProcesses', ['dayid' => $data_date_arr]);
                }
            }elseif ($data_type == 1) {
                if($channel_id == 50){
                    Artisan::call('OppoHandworkTjHandleProcesses', ['dayid' => $data_date_arr]);
                }elseif($channel_id == 9){
                    Artisan::call('XiaomiHandworkTjHandleProcesses', ['dayid' => $data_date_arr]);
                }elseif($channel_id == 54){
                    Artisan::call('HuaweiHandworkTjHandleProcesses', ['dayid' => $data_date_arr]);
                }
            }

        }

        ApiResponseFactory::apiResponse([],[]);
    }

    /**
     * 数据报错日志列表
     * @param $params array 请求数据
     */
    public static function getDateErrorLog($params){

        // 筛选条件判断
        // 错误类型 1、系统处理错误；2、数据获取错误；3、数据处理错误；4、手工核对错误
        $error_type = isset($params['error_type']) ? $params['error_type'] : '';
        // 处理类型 1,待处理;2,处理中;3,已完成;
        $status = isset($params['status']) ? $params['status'] : '';
        // 平台类型 1、统计平台；2、广告平台；3、付费平台；4、推广平台；5、其他
        $platform_type = isset($params['platform_type']) ? $params['platform_type'] : '';
        // 开始时间
        $start_date = isset($params['begin_time']) && $params['begin_time'] ? $params['begin_time'] : date("Y-m-d 00:00:00",time()-(6 * 86400));
        // 结束时间
        $end_date = isset($params['end_time']) && $params['end_time'] ? $params['end_time'] : date("Y-m-d 00:00:00",time()+86400);
        $platform = isset($params['platform']) ? $params['platform'] : ''; // 平台名称及ID


        $page = isset($params['page']) ? $params['page'] : 1 ;
        $page_size = isset($params['size']) ? $params['size'] : 1000 ;

        $map = [];
        if ($start_date && $end_date){
            $map['between'] = ['create_time',[$start_date, $end_date]];
        }
        if ($error_type) $map['error_type'] = $error_type;
        if ($platform_type) $map['platform_type'] = $platform_type;

        if ($platform) $map['like'][] = ['platform_id','like', $platform];
        if ($platform) $map['like'][] = ['platform_name','like', $platform];

        $fields = ['id','create_time as date','error_type as error_type_id','platform_name','platform_id','platform_type as plat_type_id','status','remark'];

        // 按照处理状态查询
        $unique_error_return = [];
        if ($status) {
            $unique_error_return = self::getErrorData($status,$fields);
        }else{
            for ($status = 1;$status <= 3;$status++) {
                $unique_error_single = self::getErrorData($status,$fields);
                $unique_error_return = array_merge($unique_error_return,$unique_error_single);
            }
        }

        ApiResponseFactory::apiResponse(['table_list' => array_values($unique_error_return)], []);
    }


    public static function getErrorData($status,$fields){
        // 待处理
        $map['status'] = $status;
        // 所有数据
        $data_error_list = DataImportLogic::getDataErrorList('error_log', 'error_data', $map, $fields)->orderby("create_time", "asc")->get();
        $data_error_list = Service::data($data_error_list);

        // 错误信息
        $fields = ['remark'];
        $remark_error_list = DataImportLogic::getDataErrorList('error_log', 'error_data', $map, $fields)->get();
        $remark_error_list = Service::data($remark_error_list);
        $unique_arr = [];
        $unique_error = [];
        $unique_error_list = [];
        $return_unique_error_list = [];
        if ($remark_error_list) {
            foreach ($remark_error_list as $remark_error_info) {
                $unique_arr[] = $remark_error_info['remark'];
            }
            $unique_arr = array_unique($unique_arr);
            if ($unique_arr) {
                foreach ($unique_arr as $unique_arr_k => $unique_arr_v) {
                    $unique_error[$unique_arr_k]['remark'] = $unique_arr_v;
                }
            }
            if ($data_error_list && $unique_error) {
                foreach ($data_error_list as $data_error_info) {
                    foreach ($unique_error as $unique_error_k => $unique_data) {
                        if ($unique_data['remark'] == $data_error_info['remark']) {
                            $unique_error[$unique_error_k]['all'][] = $data_error_info;
                            break;
                        }
                    }
                }
            }
        }
        if ($unique_error) {
            foreach ($unique_error as $unique_error_key => $unique_error_info) {
                $err_msg = json_decode($unique_error_info['remark'], true);
                $err_msg = implode(';', array_values($err_msg));
                $err_ids = [];
                foreach ($unique_error_info['all'] as $unique_error_msg) {
                    $err_ids[] = $unique_error_msg['id'];
                }

                $unique_error[$unique_error_key]['error_num'] = count($unique_error_info['all']);
                $unique_error[$unique_error_key]['error_detail'] = $err_msg;
                $unique_error[$unique_error_key]['error_profile'] = $err_msg;
                $unique_error[$unique_error_key]['error_type_id'] = $unique_error_info['all'][0]['error_type_id'];
                $unique_error[$unique_error_key]['platform_name'] = $unique_error_info['all'][0]['platform_name'];
                $unique_error[$unique_error_key]['platform_id'] = $unique_error_info['all'][0]['platform_id'];
                $unique_error[$unique_error_key]['plat_type_id'] = $unique_error_info['all'][0]['plat_type_id'];
                $unique_error[$unique_error_key]['status'] = $unique_error_info['all'][0]['status'];
                $unique_error[$unique_error_key]['first_time'] = $unique_error_info['all'][0]['date'];
                $unique_error[$unique_error_key]['id'] = implode(',', $err_ids);
                unset($unique_error[$unique_error_key]['all']);
                unset($unique_error[$unique_error_key]['remark']);
            }
        }
        $unique_error = array_values($unique_error);
        $unique_error = Service::sortArrByManyField($unique_error, 'platform_id', SORT_ASC, 'error_num', SORT_DESC);
        return $unique_error;
    }

    /**
     * 数据报错日志处理状态修改
     * @param $params
     */
    public static function changeErrorStatus($params){
        // 错误类型
        $ids = isset($params['id']) ? $params['id'] : '';
        if (!$ids) ApiResponseFactory::apiResponse([], [], 1050);

        $status = isset($params['status']) ? $params['status'] : '';
        if (!$status) ApiResponseFactory::apiResponse([], [], 1051);

        // 老数据
        $map['in'] = ['id',explode(',',$ids)];
        $updata['status'] = $status;
        $table_name = "error_log.error_data";
        DB::beginTransaction();
        $bool = DataImportLogic::updateErrorStatus($table_name,$map, $updata);
        if (!$bool){
            DB::rollBack();
            ApiResponseFactory::apiResponse([],[],1052);
        }
        DB::commit();

        ApiResponseFactory::apiResponse([],[]);
    }

    /**
     * 数据配置报错列表
     * @param $params array 请求数据
     */
    public static function getConfigErrorLog($params){

        // 筛选条件判断
//        $start_date = isset($params['begin_time']) && $params['begin_time'] ? $params['begin_time'] : date("Y-m-d 00:00:00",time()-(6 * 86400)); // 开始时间
//        $end_date = isset($params['end_time']) && $params['end_time'] ? $params['end_time'] : date("Y-m-d 00:00:00",time()+86400); // 结束时间
        $platform = isset($params['platform']) ? $params['platform'] : ''; // 平台名称及ID
        $platform_type = isset($params['platform_type']) ? $params['platform_type'] : ''; // 平台类型
        $user_company_id = isset($params['user_company_id']) ? $params['user_company_id'] : 1; // 平台类型

        $page = isset($params['page']) ? $params['page'] : 1 ;
        $page_size = isset($params['size']) ? $params['size'] : 1000 ;

        $map = [];

//        $map['between'] = ['create_time',[$start_date, $end_date]];
        if ($platform_type) $map['platform_type'] = $platform_type;

        if ($platform) $map['like'][] = ['platform_id','like', $platform];
        if ($platform) $map['like'][] = ['platform_name','like', $platform];
        $map['status'] = 0;
        if ($user_company_id == 9){
            $map['account'] = ['noodlecake'];
        }elseif($user_company_id == 1){
            $map[] = ['account','<>','noodlecake'];
        }

        $group_by = ['err_date','platform_name','platform_id','platform_type','first_level_id','first_level_name','second_level_id','second_level_name'];
        $data_error_list = DataImportLogic::getDataErrorList('error_log','error_info',$map)
            ->select(DB::raw("sum(money) as money"),DB::raw("count(1) as count"),'platform_name','platform_id','platform_type','first_level_id','first_level_name','second_level_id','second_level_name','err_date')
            ->where("money",">",0)
            ->groupBy($group_by)
            ->orderby("err_date","desc")
            ->orderby("platform_id","desc")
            ->orderby(DB::raw("count(1)"),"desc")
            ->get();
        $data_error_list = Service::data($data_error_list);

        if (!$data_error_list) ApiResponseFactory::apiResponse([], [], 1000);
        $new_group_by = ['platform_name','platform_id','platform_type','first_level_id','second_level_id'];
        $data_config_list = DataImportLogic::getDataErrorList('error_log','error_info',$map)
            ->select('platform_name','platform_id','platform_type','first_level_id','second_level_id')
            ->where("money",">",0)
            ->groupBy($new_group_by)
            ->orderby(DB::raw("count(1)"),"desc")
            ->get();
        $data_config_list = Service::data($data_config_list);
        if (!$data_config_list) ApiResponseFactory::apiResponse([], [], 1000);

        foreach ($data_config_list as $conf_key => $data_config_info){
            $data_config_list[$conf_key]['money'] = 0;
            $data_config_list[$conf_key]['err_num'] = 0;
            foreach ($data_error_list as $err_key => $data_error_info){
                if ($data_error_info['first_level_id'] == $data_config_info['first_level_id'] && $data_error_info['second_level_id'] == $data_config_info['second_level_id'] && $data_error_info['platform_id'] == $data_config_info['platform_id']){
                    if ($data_error_info['first_level_name']){
                        $data_config_list[$conf_key]['first_level_id'] = $data_error_info['first_level_id'].'('.$data_error_info['first_level_name'].')';
                    }else{
                        $data_config_list[$conf_key]['first_level_id'] = $data_error_info['first_level_id'];
                    }

                    if ($data_error_info['second_level_name']) {
                        $data_config_list[$conf_key]['second_level_id'] = $data_error_info['second_level_id'] . '(' . $data_error_info['second_level_name'] . ')';
                    }else{
                        $data_config_list[$conf_key]['second_level_id'] = $data_error_info['second_level_id'];
                    }
                    $data_config_list[$conf_key]['err_date'] = $data_error_info['err_date'];
                    $data_config_list[$conf_key]['money'] += floatval($data_error_info['money']);
                    $data_config_list[$conf_key]['err_num'] += $data_error_info['count'];
                    unset($data_error_list[$err_key]);
                }
            }
        }

        ApiResponseFactory::apiResponse(['table_list' => $data_config_list], []);
    }

    /**
     * 数据报错日志保存
     */
    public static function saveDataErrorLog($error_type,$platform_id,$platform_name,$platform_type,$error_msg){

        $insert_data['error_type'] = $error_type; // 1、数据获取错误；2、数据处理错误3、数据对数错误
        $insert_data['platform_id'] = $platform_id;
        $insert_data['platform_name'] = $platform_name;
        $insert_data['platform_type'] = $platform_type; // 1、统计；2、广告；3、计费；4、推广5.开发者分成或渠道分成6数据对数报错;
        $insert_data['remark'] = json_encode(['error_msg' => $error_msg],JSON_UNESCAPED_UNICODE);
        $insert_data['create_time'] = date("Y-m-d H:i:s");

        DB::beginTransaction();
        if ($insert_data) {
            //  pgsql 错误日志插入
            $result = DataImportLogic::insertDataErrorLog('error_log', 'error_data', $insert_data);
            // mysql 错误日志插入
//            $result = DataImportLogic::insertMysqlDataErrorLog($insert_data);
            if (!$result){
                DB::rollBack();
            }
        }
        DB::commit();
    }

        /**
     * 数据报错日志保存
     */
    public static function saveDataErrorMoneyLog($platform_id,$date,$insert_data,$td_err_type = 0){
        $map =[];
        DB::beginTransaction();
        $map['err_date'] = $date;
        $map['platform_id'] = $platform_id;
        if ($td_err_type) $map['td_err_type'] = $td_err_type;
        $count = DataImportLogic::getChannelData('error_log','error_info',$map)->count();
        if($count>0){        
                    //删除数据
            DataImportLogic::deleteHistoryData('error_log','error_info',$map);
        }

        $step = [];
        $i = 0;
        foreach ($insert_data as $kkkk => $insert_data_info) {
            if ($kkkk % 2000 == 0) $i++;
            if ($insert_data_info) {
                $step[$i][] = $insert_data_info;
            }
        }

        if ($step) {
            foreach ($step as $k => $v) {
                $result = DataImportLogic::insertDataErrorLog('error_log', 'error_info', $v);
                if (!$result) {
                    DB::rollBack();
                    echo 'mysql_error'. PHP_EOL;
                }
            }
        }
        DB::commit();
    }


    public static function getPlatformExchangeRate($effective_time){
        //获取当月美元汇率
        if (is_array($effective_time)){
            $usd_ex_info = DB::table('c_currency_ex')->whereIn('effective_time', $effective_time)->where(['currency_id' => 60])->get();
        }else{
            $usd_ex_info = DB::table('c_currency_ex')->where(['effective_time' => $effective_time,'currency_id' => 60])->first();
        }
        $usd_ex_info = Service::data($usd_ex_info);
        return $usd_ex_info;
    }
}
