<?php

namespace App\BusinessImp;

use App\BusinessLogic\CommonLogic;
use App\BusinessLogic\DataImportLogic;
use App\BusinessLogic\DataUploadLogic;
use App\BusinessLogic\OperationLogLogic;
use App\Common\ApiResponseFactory;
use App\Common\Service;
use App\Common\CurlRequest;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\UserLogic;
use Illuminate\Support\Facades\DB;
use App\BusinessLogic\RoleLogic;
use Illuminate\Support\Facades\Storage;
use PHPExcel_IOFactory;

class DataUploadImp extends ApiBaseImp
{
    /**
     * 文件上传
     */
    public static function postOriginalDataUpload($request) {

        $platform_type = isset($_POST['platform_type']) ? $_POST['platform_type'] : '';
        $currency_id = isset($_POST['currency_id']) ? $_POST['currency_id'] : '';
        $platform_id = isset($_POST['platform_id']) ? $_POST['platform_id'] : ''; // 平台
        if(!$platform_id) ApiResponseFactory::apiResponse([],[],505);
        if (!$platform_type) ApiResponseFactory::apiResponse([],[],538);

        // 此时 $this->upload如果成功就返回文件名不成功返回false
        if (!$_FILES){
            ApiResponseFactory::apiResponse([],[],1001);
        }

        $file_name = static::upload($_FILES);
        if (!$file_name){
            ApiResponseFactory::apiResponse([],[],1001);
        }

        // 保存文件路径
        $insert_data = [];
        $insert_data['platform_id'] = $platform_id;
        $insert_data['platform_type'] = $platform_type;
        $insert_data['file_name'] = $file_name;
        $insert_data['status'] = 0;
        $insert_data['currency_id'] = $currency_id;
        $insert_data['date'] = date("Ymd");
        $insert_data['createtime'] = date("Y-m-d H:i:s");
        $result = DB::table('c_uploadfile')->insert($insert_data);
        if (!$result) ApiResponseFactory::apiResponse([],[],1001);

        ApiResponseFactory::apiResponse([],[]);
    }

    /**
     * 验证文件是否合法
     */
    public static function upload($file, $disk='upload') {
        // 1.是否上传成功
        $file_arrays = $_FILES;
        $file_name_arrays = [];
        foreach ($file_arrays as $file_array) {
            if ($file_array['error'] > 0) {
                ApiResponseFactory::apiResponse([], [], 1009);
            }

            // 2.是否符合文件类型 getClientOriginalExtension 获得文件后缀名
            // 定义存放上传文件的真实路径名字
            $name = $file_array['name'];
            $fileExtension = explode('.', $name);
            $file_extension = $fileExtension[count($fileExtension) - 1];
            if (!in_array($file_extension, ['csv', 'xlsx', 'xls'])) {
                ApiResponseFactory::apiResponse([], [], 1007);
            }


            // 3.判断大小是否符合 2M
            $tmpFile = $file_array['tmp_name'];
            if (filesize($tmpFile) >= 5 * 2048000) {
                ApiResponseFactory::apiResponse([], [], 1003);
            }

            // 5.每天一个文件夹,分开存储, 生成一个随机文件名
            $file_name = md5(time()) . mt_rand(0, 9999) . '.' . $file_extension;
            $fileName = date('Ymd') . '/' . $file_name;
//        $fileName = $platform_id.'/'.date('Y_m_d').'/'. $data_file_name;
            if (Storage::disk($disk)->put($fileName, file_get_contents($tmpFile))) {
//            return Storage::url($fileName);
//            return env("UPLOAD_PATH").'/'.$fileName;
                $file_name_arrays[] = $file_name;
            }
        }
        return json_encode($file_name_arrays);

    }


    /**
     * 文件上传
     */
    public static function postPlatformDataUpload($request) {

        $platform_type = isset($_POST['platform_type']) ? $_POST['platform_type'] : '';
        $currency_id = isset($_POST['currency_id']) ? $_POST['currency_id'] : '';
        $platform_id = isset($_POST['platform_id']) ? $_POST['platform_id'] : ''; // 平台
        if(!$platform_id) ApiResponseFactory::apiResponse([],[],505);
        if (!$platform_type) ApiResponseFactory::apiResponse([],[],538);

        // 此时 $this->upload如果成功就返回文件名不成功返回false
        if (!$_FILES){
            ApiResponseFactory::apiResponse([],[],1001);
        }

        static::uploadFiles($platform_id,$platform_type,$_FILES);

    }

    /**
     * 验证文件是否合法
     */
    public static function uploadFiles($platform_id,$platform_type,$file, $disk='upload') {
        // 1.是否上传成功
        $file_arrays = $_FILES;
        $arr_data = [];

        foreach ($file_arrays as $file_array) {
            if ($file_array['error'] > 0) {
                ApiResponseFactory::apiResponse([], [], 1009);
            }

            // 2.是否符合文件类型 getClientOriginalExtension 获得文件后缀名
            // 定义存放上传文件的真实路径名字
            $name = $file_array['name'];
            $fileExtension = explode('.', $name);
            $file_extension = $fileExtension[count($fileExtension) - 1];
            if (!in_array($file_extension, ['csv', 'xlsx', 'xls'])) {
                ApiResponseFactory::apiResponse([], [], 1007);
            }


            // 3.判断大小是否符合 2M
            $tmpFile = $file_array['tmp_name'];
            if (filesize($tmpFile) >= 5 * 2048000) {
                ApiResponseFactory::apiResponse([], [], 1003);
            }

            // 5.每天一个文件夹,分开存储, 生成一个随机文件名
            $file_name = md5(time()) . mt_rand(0, 9999) . '.' . $file_extension;
            $fileName = '/' . $file_name;
            if (Storage::disk($disk)->put($fileName, file_get_contents($tmpFile))) {
                //解析文件
                $file_path = env('UPLOAD_PATH') . '/' . $file_name;
                // 获取文件数据内容 返回数组
                $arr_data_one = self::getCsvOrExcelData($file_path);
                if ($arr_data_one){
                    $arr_data = array_merge($arr_data,$arr_data_one);
                }else{
                    ApiResponseFactory::apiResponse([], [], 1011);
                }
            }
        }

        if(empty($arr_data)){
            ApiResponseFactory::apiResponse([], [], 1011);
        }

        if ($arr_data){
            if ($platform_id == 'pcr03' || $platform_id == 'pcr01jd'){
                self::operatorsData($platform_id,$arr_data);
            }else{
                self::savePgsqlData($platform_id,$platform_type,$arr_data);
            }
        }

    }


    public static function savePgsqlData($platform_id,$platform_type,$arr_data){

        // 根据数据类型选择存储表
        if ($platform_type == 1){
            // 统计数据
            $schema = 'tj_data';
        }else if ($platform_type == 2){
            // 广告数据
            $schema = 'ad_data';
        }else if ($platform_type == 3){
            // 计费数据
            $schema = 'ff_data';
        }else if ($platform_type == 4){
            // 推广数据
            $schema = 'tg_data';
        }

        $table_name = 'erm_data';


        // 处理数据
        // 第一步，查询删除
        $app_ids = [];
        $app_names = [];
        $data_date_arr = [];
        $data_app_arr = [];
        // 待完善
        $app_id_fields = ['应用ID', 'APPID', 'APPId'];
        $app_name_fields = ['应用名称', '应用', '游戏名称', 'APP'];
        foreach ($arr_data as $key => $data_info){

            $data_date =  isset($data_info['日期']) ? $data_info['日期'] : '';
            $is_date_num = strtotime($data_date);
            if (!$is_date_num){
                $d = 25569;
                $t= 24*60*60;
                $data_date = gmdate('Y-m-d',($data_date-$d)*$t);
            }
            if (strlen($data_date) < 8){
                ApiResponseFactory::apiResponse([],[],812);
            }elseif(strlen($data_date) == 8){
                $data_date = date('Y-m-d',strtotime($data_date));
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
            $data_info['date_time'] = $data_date;
            foreach ($data_info as $d_k => $d_v){

                if (in_array($d_k,$app_id_fields)){
                    if ($d_v){
                        $data_info['app_id'] = $d_v;
                        $data_app_arr[] = $d_v;
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
        $data_app_arr = array_unique($data_app_arr);


        $map = [];
        $map[] = ['source_id',$platform_id];
        $map['in'] = ['dayid', $data_date_arr];
        if ($platform_id == 'pad275' && $data_app_arr){
            $map['in'] = ['app_id', $data_app_arr];
        }
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
            $insert_data[$key]['source_id'] = $platform_id;
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


        if($platform_id == 'ptg279'){
            Artisan::call('MediaTgHandleProcesses' ,['dayid' => $data_date_arr]);
        }elseif($platform_id == 'pad59'){//微信广告
            Artisan::call('WeixinAdiHandworkHandleProcesses' ,['dayid' => $data_date_arr]);
        }elseif($platform_id == 'pad275'){//巨量广告
            Artisan::call('JuliangAdiHandworkHandleProcesses' ,['dayid' => $data_date_arr]);
        }elseif($platform_id == 'pad271'){//tiktok广告
            Artisan::call('TiktokPlatHandworkHandleProcesses' ,['dayid' => $data_date_arr]);
        }elseif($platform_id == 'pad272'){//穿山甲广告
            Artisan::call('PangolinPlatHandworkHandleProcesses' ,['dayid' => $data_date_arr]);
        }

        ApiResponseFactory::apiResponse([],[]);

    }

    /**
     *  运营商 数据处理
     */
    public static function operatorsData($platform_id,$arr_data){

        // 处理数据
        $data_date_arr = [];
        $day_operators_data = [];
        $month_operators_data = [];
        foreach ($arr_data as $key => $data_info){
            $data_date = '';
            if (key_exists('日',$data_info)) {
                $data_date = isset($data_info['日']) ? $data_info['日'] : '';
            }elseif(key_exists('日期',$data_info)){
                $data_date = isset($data_info['日期']) ? $data_info['日期'] : '';
            }
            $is_date_num = strtotime($data_date);
            if (!$is_date_num){
                $d = 25569;
                $t= 24*60*60;
                $data_date = gmdate('Y-m-d',($data_date-$d)*$t);
            }
            if (strlen($data_date) < 8){
                ApiResponseFactory::apiResponse([],[],812);
            }elseif(strlen($data_date) == 8){
                $data_date = date('Y-m-d',strtotime($data_date));
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

            $data_date = date('Ymd',strtotime($data_date));
            if ($data_date){
                $data_date_arr[] = $data_date;
            }
            $data_info['date_time'] = $data_date;
            if (key_exists('套餐总平台流水',$data_info)){
                $month_operators_data[$key]['TIME'] = $data_info['date_time'];
                $month_operators_data[$key]['CP_ID_CR'] = isset($data_info['企业代码']) ? $data_info['企业代码'] : '';
                $month_operators_data[$key]['CP_NAME_CR'] = isset($data_info['企业名称']) ? $data_info['企业名称'] : '';
                $month_operators_data[$key]['PRODUCT_ID_JD'] = isset($data_info['套餐包代码']) ? $data_info['套餐包代码'] : '';
                $month_operators_data[$key]['PRODUCT_NAME_JD'] = isset($data_info['套餐包名称']) ? $data_info['套餐包名称'] : '';
                $month_operators_data[$key]['CHANNEL_CP_ID'] = isset($data_info['渠道公司代码']) ? $data_info['渠道公司代码'] : '';
                $month_operators_data[$key]['CHANNEL_CP_NAME'] = isset($data_info['渠道公司名称']) ? $data_info['渠道公司名称'] : '';
                $month_operators_data[$key]['CHANNEL_ID_JD'] = isset($data_info['渠道代码']) ? $data_info['渠道代码'] : '';
                $month_operators_data[$key]['CHANNEL_NAME_JD'] = isset($data_info['渠道名称']) ? $data_info['渠道名称'] : '';
                $month_operators_data[$key]['EARNING'] = isset($data_info['套餐总平台流水']) ? $data_info['套餐总平台流水'] : '';
                $month_operators_data[$key]['PAY_USER'] = isset($data_info['付费用户数（流水）']) ? $data_info['付费用户数（流水）'] : '';
                $month_operators_data[$key]['MPAY_INCOME'] = isset($data_info['套餐功能费平台流水']) ? $data_info['套餐功能费平台流水'] : '';
                $month_operators_data[$key]['FIRSTBOOK_USER'] = isset($data_info['新增订购用户数']) ? $data_info['新增订购用户数'] : '';
                $month_operators_data[$key]['UNSUBSCIRBE_USER'] = isset($data_info['新增退订用户数']) ? $data_info['新增退订用户数'] : '';
                $month_operators_data[$key]['REBOOK_USER'] = isset($data_info['续订用户数']) ? $data_info['续订用户数'] : '';
                $month_operators_data[$key]['BUSINESS_DIVIDE'] = '';
                $month_operators_data[$key]['PROVINCE'] = isset($data_info['省份']) ? $data_info['省份'] : '';
                $month_operators_data[$key]['CREATE_TIME'] = date('Y-m-d H:i:s',time());
                $month_operators_data[$key]['UPDATE_TIME'] = date('Y-m-d H:i:s',time());
            }elseif(key_exists('日总平台流水',$data_info)){
                $day_operators_data[$key]['TIME'] = $data_info['date_time'];
                $day_operators_data[$key]['BUSINESS_ID'] = isset($data_info['业务代码']) ? $data_info['业务代码'] : '';
                $day_operators_data[$key]['BUSINESS_NAME'] = isset($data_info['业务名称']) ? $data_info['业务名称'] : '';
                $day_operators_data[$key]['BUSINESS_TYPE'] = isset($data_info['业务分类']) ? $data_info['业务分类'] : '';
                $day_operators_data[$key]['BUSINESS_NAME_SPLIT'] = '';
                $day_operators_data[$key]['BUSINESS_DIVIDE'] = '';
                $day_operators_data[$key]['CP_ID_CR'] = isset($data_info['企业代码']) ? $data_info['企业代码'] : '';
                $day_operators_data[$key]['CP_NAME_CR'] = isset($data_info['企业名称']) ? $data_info['企业名称'] : '';
                $day_operators_data[$key]['CHANNEL_COMPANY_ID'] = isset($data_info['渠道商代码']) ? $data_info['渠道商代码'] : '';
                $day_operators_data[$key]['CHANNEL_COMPANY_NAME'] = isset($data_info['渠道商名称']) ? $data_info['渠道商名称'] : '';
                $day_operators_data[$key]['CHANNEL_ID_CR'] = isset($data_info['渠道代码']) ? $data_info['渠道代码'] : '';
                $day_operators_data[$key]['CHANNEL_NAME_CR'] = isset($data_info['渠道名称']) ? $data_info['渠道名称'] : '';
                $day_operators_data[$key]['PAY_USER'] = isset($data_info['日付费用户数（流水）']) ? $data_info['日付费用户数（流水）'] : '';
                $day_operators_data[$key]['PAY_TIME'] = isset($data_info['日付费次数（流水）']) ? $data_info['日付费次数（流水）'] : '';
                $day_operators_data[$key]['EARNING'] = isset($data_info['日总平台流水']) ? $data_info['日总平台流水'] : '';
                $day_operators_data[$key]['PROVINCE'] = isset($data_info['省']) ? $data_info['省'] : '';
                $day_operators_data[$key]['POINT_ID'] = '';
                $day_operators_data[$key]['POINT_NAME'] = '';
                $day_operators_data[$key]['BUSINESS_TACTICS'] = '';
                $day_operators_data[$key]['REMARK5'] = '';
                $day_operators_data[$key]['REMARK'] = '';
                $day_operators_data[$key]['CREATE_TIME'] = date('Y-m-d H:i:s',time());
                $day_operators_data[$key]['UPDATE_TIME'] = date('Y-m-d H:i:s',time());
            }elseif(key_exists('包月产品名称',$data_info)){
                $month_operators_data[$key]['TIME'] = $data_info['date_time'];
                $month_operators_data[$key]['BUSINESS_NAME'] = isset($data_info['游戏名称']) ? $data_info['游戏名称'] : '';
                $month_operators_data[$key]['PRODUCT_NAME'] = isset($data_info['包月产品名称']) ? $data_info['包月产品名称'] : '';
                $month_operators_data[$key]['FIRSTBOOK_USER'] = '';
                $month_operators_data[$key]['ACTIVE_USER'] = '';
                $month_operators_data[$key]['UNSUBSCRIBE_USER'] = isset($data_info['退订用户数']) ? $data_info['退订用户数'] : '';
                $month_operators_data[$key]['FIRSTBOOK_PAY'] = isset($data_info['首订用户数']) ? $data_info['首订用户数'] : '';
                $month_operators_data[$key]['REBOOK_PAY'] = isset($data_info['续订用户数']) ? $data_info['续订用户数'] : '';
                $month_operators_data[$key]['TOTAL_INCOME'] = isset($data_info['出账金额']) ? $data_info['出账金额'] : '';
                $month_operators_data[$key]['BUSINESS_DIVIDE'] = '';
                $month_operators_data[$key]['BUSINESS_ID'] = '';
                $month_operators_data[$key]['REBOOK_USER'] = '';
                $month_operators_data[$key]['PAY_USER'] = isset($data_info['出账用户数']) ? $data_info['出账用户数'] : '';
                $month_operators_data[$key]['CREATE_TIME'] = date('Y-m-d H:i:s',time());
                $month_operators_data[$key]['UPDATE_TIME'] = date('Y-m-d H:i:s',time());
                $month_operators_data[$key]['CHANNEL_ID_CR'] = isset($data_info['渠道ID']) ? $data_info['渠道ID'] : '';
                $month_operators_data[$key]['CHANNEL_NAME_CR'] = isset($data_info['渠道名称']) ? $data_info['渠道名称'] : '';
            }elseif(key_exists('渠道CODE',$data_info)){
                $day_operators_data[$key]['TIME'] = $data_info['date_time'];
                $day_operators_data[$key]['BUSINESS_ID'] = isset($data_info['游戏ID']) ? $data_info['游戏ID'] : '';
                $day_operators_data[$key]['BUSINESS_NAME'] = isset($data_info['游戏名称']) ? $data_info['游戏名称'] : '';
                $day_operators_data[$key]['BUSINESS_TYPE'] = '';
                $day_operators_data[$key]['BUSINESS_DIVIDE'] = '';
                $day_operators_data[$key]['CP_ID_CR'] = '';
                $day_operators_data[$key]['CP_NAME_CR'] = '';
                $day_operators_data[$key]['CHANNEL_ID_CR'] = isset($data_info['渠道CODE']) ? $data_info['渠道CODE'] : '';
                $day_operators_data[$key]['CHANNEL_NAME_CR'] = isset($data_info['渠道名称']) ? $data_info['渠道名称'] : '';
                $day_operators_data[$key]['PAY_USER'] = isset($data_info['付费用户数']) ? $data_info['付费用户数'] : '';
                $day_operators_data[$key]['PAY_TIME'] = '';
                $day_operators_data[$key]['EARNING'] = isset($data_info['游戏收入']) ? $data_info['游戏收入'] : '';
                $day_operators_data[$key]['CARRIER'] = isset($data_info['运营商来源']) ? $data_info['运营商来源'] : '';
                $day_operators_data[$key]['REMARK2'] = '';
                $day_operators_data[$key]['REMARK3'] = '';
                $day_operators_data[$key]['REMARK4'] = '';
                $day_operators_data[$key]['REMARK5'] = '';
                $day_operators_data[$key]['REMARK'] = '';
                $day_operators_data[$key]['CREATE_TIME'] = date('Y-m-d H:i:s',time());
                $day_operators_data[$key]['UPDATE_TIME'] = date('Y-m-d H:i:s',time());
            }

        }
//        $app_ids = array_unique($app_ids);
//        $app_names = array_unique($app_names);
        $data_date_arr = array_unique($data_date_arr);

        if ($day_operators_data){
            if ($platform_id == 'pcr01jd'){
                self::saveOperatorsData($data_date_arr,'o_ff_cmcc_jd_daily',$day_operators_data);
            }elseif($platform_id == 'pcr03'){
                self::saveOperatorsData($data_date_arr,'o_ff_ctcc_daily',$day_operators_data);
            }

        }

        if($month_operators_data){
            if ($platform_id == 'pcr01jd') {
                self::saveOperatorsData($data_date_arr, 'o_ff_cmcc_jd_month', $month_operators_data);
            }elseif($platform_id == 'pcr03'){
                self::saveOperatorsData($data_date_arr, 'o_ff_ctcc_month', $month_operators_data);
            }
        }

        sort($data_date_arr);
        $day_begin = isset($data_date_arr[0]) ? $data_date_arr[0] : date("Y-m-01",strtotime("-1 month"));
        $day_end = isset($data_date_arr[count($data_date_arr) - 1]) ? $data_date_arr[count($data_date_arr) - 1] : date("Y-m-d",strtotime("$day_begin +1 month -1 day"));

        if ($platform_id == 'pcr01jd'){
            // 移动基地日付处理过程
            Artisan::call('CmccJdDailyHandleProcesses',['begin_date'=>$day_begin,'end_date'=>$day_end]);
            // 移动基地 月付处理过程
            Artisan::call('CmccJdMonthHandleProcesses',['begin_date'=>$day_begin,'end_date'=>$day_end]);
            // 小表到大表
            Artisan::call('FfSummaryProcesses',['begin_date'=>$day_begin,'end_date'=>$day_end,'platform_id'=>'pcr01jd']);
        }elseif($platform_id == 'pcr03'){
            // 电信日付处理过程
            Artisan::call('CtccDailyHandleProcesses',['begin_date'=>$day_begin,'end_date'=>$day_end]);
            // 电信月付处理过程
            Artisan::call('CtccMonthHandleProcesses',['begin_date'=>$day_begin,'end_date'=>$day_end]);
            // 小表到大表
            Artisan::call('FfSummaryProcesses',['begin_date'=>$day_begin,'end_date'=>$day_end,'platform_id'=>'pcr03']);

        }

        ApiResponseFactory::apiResponse([],[]);
    }

    // 保存运营商数据
    public static function saveOperatorsData($data_date_arr,$table_name,$operators_data){
        $map = [];
        $map['in'] = ['TIME', $data_date_arr];
        DB::beginTransaction();
        DataImportLogic::deleteOperatorsData($table_name,$map);

        if ($operators_data) {
            //拆分批次
            $step = array();
            $i = 0;
            foreach ($operators_data as $kkkk => $insert_data_info) {
                if ($kkkk % 1000 == 0) $i++;
                if ($insert_data_info) {
                    $step[$i][] = $insert_data_info;
                }
            }

            if ($step) {
                foreach ($step as $k => $v) {
                    $result = DataImportLogic::insertOperatorsData($table_name, $v);
                    if (!$result) {
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([], [], 811);
                    }
                }
            }
        }

        DB::commit();
    }

    /**
     * 读取excel数据内容  返回数组
     * @param $inputFileName
     * @return array
     * @throws \PHPExcel_Exception
     * @throws \PHPExcel_Reader_Exception
     */
    public static function getCsvOrExcelData($inputFileName){
        $data = [];
        try {
            // 载入当前文件
            $phpExcel = PHPExcel_IOFactory::load($inputFileName);
            // 设置为默认表
            $phpExcel->setActiveSheetIndex(0);
            // 获取表格数量
            $sheetCount = $phpExcel->getSheetCount();
            // 获取行数
            $row = $phpExcel->getActiveSheet()->getHighestRow();
            // 获取列数
            $column = $phpExcel->getActiveSheet()->getHighestColumn();

            $fields = [];
            // 行数循环
            for ($i = 1; $i <= $row; $i++) {
                // 列数循环
                if ($i == 1) {
                    for ($c = 'A'; $c <= $column; $c++) {
                        $fields[] = $phpExcel->getActiveSheet()->getCell($c . $i)->getValue();
                    }
//                var_dump($fields);
                } else {
                    $row_data = [];
                    for ($c = 'A'; $c <= $column; $c++) {
                        $row_data[] = $phpExcel->getActiveSheet()->getCell($c . $i)->getValue();
                    }

                    $data[] = array_combine($fields, $row_data);
//                var_dump($row_data);

                }

            }
        } catch (\Exception $e) {
            // todo

        }

        return $data;
    }

    
}
