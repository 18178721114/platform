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
            self::savePgsqlData($platform_id,$platform_type,$arr_data);
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


        if ($platform_id == 'pcr03'){
            // todo 电信
        }elseif($platform_id == 'pcr01jd'){
            // todo 移动基地
        }elseif($platform_id == 'ptg279'){//媒体推广
            Artisan::call('MediaTgHandleProcesses' ,['dayid' => $data_date_arr]);
        }elseif($platform_id == 'pad59'){//微信广告
            Artisan::call('WeixinAdiHandworkHandleProcesses' ,['dayid' => $data_date_arr]);
        }elseif($platform_id == 'pad275'){//巨量广告
            Artisan::call('JuliangAdiHandworkHandleProcesses' ,['dayid' => $data_date_arr]);
        }

        ApiResponseFactory::apiResponse([],[]);

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
