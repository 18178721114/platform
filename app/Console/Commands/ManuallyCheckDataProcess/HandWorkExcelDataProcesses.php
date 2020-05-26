<?php

namespace App\Console\Commands\ManuallyCheckDataProcess;

use App\Common\CurlRequest;
use App\Common\Service;
use App\Console\Commands\ManuallyCheckDataProcess\TgHandleDataProcesses;
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

class HandWorkExcelDataProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'HandWorkExcelDataProcesses {dayid?} ';

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
        ini_set('memory_limit','3072M');
        set_time_limit(0);
        $sql = 'select * from c_uploadFile where status = 0';
        $info=DB::select($sql);
        $info = Service::data($info);
        if(!$info){
            echo '暂无处理数据';die;

        }

        foreach ($info as $k_1 => $v_1) {
            $name_arrays = json_decode($v_1['file_name']);
            $platform_id = $v_1['platform_id'];
            $platform_type = $v_1['platform_type'];
            $date = $v_1['date'];
            $arr_data = [];
            $num = 0;
            $arr = [];
            foreach ($name_arrays as $name) {
                //解析文件
                $file_path = env('UPLOAD_PATH') . '/' . $date . '/' . $name;

                // 获取文件数据内容 返回数组
                $arr_data_one = self::getCsvOrExcelData($file_path);
//                var_dump(count($arr_data_one));
                if ($arr_data_one){
                    $arr_data = array_merge($arr_data,$arr_data_one);
                }
            }

//            var_dump(count($arr_data));
            if(empty($arr_data)){
                echo '文件数据为空'."\r\n";die;
            }

            //   2/4 广告/推广匹配
            if($platform_type == 2){//广告
                $plat_config =self::platform_ad_cofig($platform_id);
                if(empty($plat_config)){
                    echo '广告配置查询失败'."\r\n";die;
                }

                // 匹配数据返回结果
                $result_data = [];

                // todo 分平台匹配数据
                if ($platform_id == 'pad24'){
                    $result_data = AdHandleDataProcesses::unityDataProcess($plat_config,$platform_id,$arr_data);
                }elseif ($platform_id == 'pad01'){
                    $result_data = AdHandleDataProcesses::admobDataProcess($plat_config,$platform_id,$arr_data);
                }elseif ($platform_id == 'pad03'){
                    $result_data = AdHandleDataProcesses::chartboostDataProcess($plat_config,$platform_id,$arr_data);
                }elseif ($platform_id == 'pad05'){
                    $result_data = AdHandleDataProcesses::ironsoursDataProcess($plat_config,$platform_id,$arr_data);
                }elseif ($platform_id == 'pad10'){
                    $result_data = AdHandleDataProcesses::guangdiantongDataProcess($plat_config,$platform_id,$arr_data);
                }elseif ($platform_id == 'pad11'){
                    $result_data = AdHandleDataProcesses::baiduDataProcess($plat_config,$platform_id,$arr_data);
                }elseif ($platform_id == 'pad50'){
                    $result_data = AdHandleDataProcesses::mintegalDataProcess($plat_config,$platform_id,$arr_data);
                }elseif ($platform_id == 'pad271'){
                    $result_data = AdHandleDataProcesses::tiktokDataProcess($plat_config,$platform_id,$arr_data);
                }elseif ($platform_id == 'pad272'){
                   // 穿山甲
                    $result_data = AdHandleDataProcesses::pangolinDataProcess($plat_config,$platform_id,$arr_data);
                }elseif ($platform_id == 'pad23'){
                    // 穿山甲
                    $result_data = AdHandleDataProcesses::facebookDataProcess($plat_config,$platform_id,$arr_data);
                }

                if (empty($result_data)){
                    echo '匹配数据为空'."\r\n";die;
                }
//var_dump($result_data);die;
                $arr_info = $result_data['arr_info'];
                $arr_info_1 = $result_data['arr_info_1'];
                $arr_del_ad = $result_data['arr_del_ad'];

//                var_dump(count($arr_info));
//                var_dump(count($arr_info_1));
//                var_dump(count($arr_del_ad));

                DB::beginTransaction();

                if ($arr_del_ad) {
                    $map = [];
                    $map['in'] = ['date',$arr_del_ad];
                    $map['platform_id'] = $platform_id;
                    $count = DataUploadLogic::getMysqlData('zplay_ad_handwork_daily', $map)->count();
                    if ($count) {
                        $del = DataUploadLogic::deleteMysqlData('zplay_ad_handwork_daily', $map);
                        if (!$del) {
                            DB::rollBack();
                        }
                    }
                }

                $step = array();
                $i = 0;
                if($arr_info){
                //匹配成功
                    foreach ($arr_info as $kkkk => $insert_data_info) {
                        if ($kkkk % 200 == 0) $i++;
                        if ($insert_data_info) {
                            $step[$i][] = $insert_data_info;
                        }
                    }
                    $is_success = [];

                    if ($step) {
                        foreach ($step as $k => $v) {
                            $result = DataUploadLogic::insertMysqlData('zplay_ad_handwork_daily',$v);
                            if (!$result) {
                                DB::rollBack();
                                $is_success[] = $k;
                            }
                        }
                    }
                }
                $step =[];
                $i = 0;
                if($arr_info_1){
                //匹配失败
                    foreach ($arr_info_1 as $kkkk => $insert_data_info) {
                        if ($kkkk % 200 == 0) $i++;
                        if ($insert_data_info) {
                            $step[$i][] = $insert_data_info;
                        }
                    }
                    $is_success = [];
                    if ($step) {
                        foreach ($step as $k => $v) {
                            $result = DataUploadLogic::insertMysqlData('zplay_ad_handwork_daily',$v);
                            if (!$result) {
                                DB::rollBack();
                                $is_success[] = $k;
                            }
                        }

                    }
                }
                
            }elseif($platform_type == 4){//推广
                $plat_config = self::platform_tg_cofig($platform_id);
                if(empty($plat_config)){
                    echo '推广配置查询失败'."\r\n";die;
                }

                // 匹配数据返回结果
                $result_data = [];

                // todo 分平台匹配数据
                if ($platform_id == 'ptg66'){
                    $result_data = TgHandleDataProcesses::toutiaoDataProcess($plat_config,$platform_id,$arr_data);
                }elseif ($platform_id == 'ptg76'){
                    $result_data = TgHandleDataProcesses::tiktokDataProcess($plat_config,$platform_id,$arr_data);
                }elseif ($platform_id == 'ptg75'){
                    $result_data = TgHandleDataProcesses::snapchatDataProcess($plat_config,$platform_id,$arr_data);
                }elseif ($platform_id == 'ptg63'){
                    $result_data = TgHandleDataProcesses::adcolonyDataProcess($plat_config,$platform_id,$arr_data);
                }elseif ($platform_id == 'ptg40'){
                    $result_data = TgHandleDataProcesses::adwordsDataProcess($plat_config,$platform_id,$arr_data);
                }elseif ($platform_id == 'ptg37'){
                    $result_data = TgHandleDataProcesses::vungleDataProcess($plat_config,$platform_id,$arr_data);
                }elseif ($platform_id == 'ptg36'){
                    $result_data = TgHandleDataProcesses::unityDataProcess($plat_config,$platform_id,$arr_data);
                }elseif ($platform_id == 'ptg33'){
                    $result_data = TgHandleDataProcesses::facebookDataProcess($plat_config,$platform_id,$arr_data);
                }elseif ($platform_id == 'ptg21'){
                    $result_data = TgHandleDataProcesses::applovinDataProcess($plat_config,$platform_id,$arr_data);
                }elseif ($platform_id == 'ptg03'){
                    $result_data = TgHandleDataProcesses::ironsourseDataProcess($plat_config,$platform_id,$arr_data);
                }

                if (empty($result_data)){
                    echo '匹配数据为空'."\r\n";die;
                }

                $arr_info = $result_data['arr_info'];
                $arr_info_1 = $result_data['arr_info_1'];
                $arr_del_tg = $result_data['arr_del_tg'];

//                var_dump(count($arr_info));
//                var_dump(count($arr_info_1));
//                var_dump(count($arr_del_tg));

                DB::beginTransaction();

                if ($arr_del_tg){
                    $map=[];
                    $map['in'] = ['date',$arr_del_tg];
                    $map['platform_id'] = $platform_id;
                    $count = DataUploadLogic::getMysqlData('zplay_tg_handwork_daily',$map)->count();
                    if($count){
                        $del = DataUploadLogic::deleteMysqlData('zplay_tg_handwork_daily',$map);
                        if(!$del){
                            DB::rollBack();
                        }
                    }
                }

                $step = array();
                $i = 0;
                if($arr_info){
                //匹配成功
                    foreach ($arr_info as $kkkk => $insert_data_info) {
                        if ($kkkk % 200 == 0) $i++;
                        if ($insert_data_info) {
                            $step[$i][] = $insert_data_info;
                        }
                    }
                    $is_success = [];

                    if ($step) {
                        foreach ($step as $k => $v) {
                            $result = DataUploadLogic::insertMysqlData('zplay_tg_handwork_daily',$v);
                            if (!$result) {
                                DB::rollBack();
                                $is_success[] = $k;
                            }
                        }
                    }
                }
                $step =[];
                $i = 0;
                if($arr_info_1){
                //匹配失败
                    foreach ($arr_info_1 as $kkkk => $insert_data_info) {
                        if ($kkkk % 200 == 0) $i++;
                        if ($insert_data_info) {
                            $step[$i][] = $insert_data_info;
                        }
                    }
                    $is_success = [];
                    if ($step) {
                        foreach ($step as $k => $v) {
                            $result = DataUploadLogic::insertMysqlData('zplay_tg_handwork_daily',$v);
                            if (!$result) {
                                DB::rollBack();
                                $is_success[] = $k;
                            }
                        }
                    }
                }

            }
            $update_sql = "update c_uploadFile set status = 1 where id ='".$v_1['id']."'";
            $update = DB::update($update_sql);

            if(!$update){
                DB::rollBack();
            }
            DB::commit();

        }

        //往大表和数据
        self::zplay_basic_handwork_daily();

    }

    public static function platform_ad_cofig($source_id){
        $sql = "SELECT 
            c_app.os_id,
            c_app_ad_platform.platform_app_id,
            c_app_ad_platform.platform_app_name,
            `c_platform`.`divide_ad`,
            `c_app`.`id`,
            `c_app`.`app_id`,
            `c_app_ad_platform`.`platform_id`,
            `c_app_ad_slot`.`zone`,
            `c_app_ad_slot`.`ad_slot_id`,
            `c_app_ad_slot`.`ad_slot_name`,
            `c_app_ad_slot`.`video_placement_id`,
            `c_app_ad_slot`.`interstitial_placement_id`,
            `c_app_ad_slot`.`ad_type`,
            `c_platform`.`currency_type_id`,
            `c_app_ad_platform`.`flow_type` 
            FROM
            `c_app`
            LEFT JOIN `c_app_ad_platform` ON `c_app_ad_platform`.`app_id` = `c_app`.`id`
            LEFT JOIN `c_app_ad_slot` ON `c_app_ad_slot`.`app_ad_platform_id` = `c_app_ad_platform`.`id`

            LEFT JOIN (
            SELECT
            `c_platform`.`bad_account_rate`,c_platform.currency_type_id,c_platform.platform_id, c_divide.*
            FROM
            c_platform
            LEFT JOIN c_divide ON `c_divide`.`app_channel_id` = `c_platform`.`id`
            AND `c_divide`.`type` = 3
            WHERE c_platform.`platform_id` ='$source_id'
            ORDER BY
            c_divide.effective_date DESC LIMIT 1
            ) AS c_platform ON `c_platform`.`platform_id` = `c_app_ad_platform`.`platform_id`

            WHERE
            (
            `c_app_ad_platform`.`platform_id` = '$source_id'
        )";
        $app_list = DB::select($sql);
        $app_list = Service::data($app_list);
        return  $app_list;
    }


    public static function platform_tg_cofig($source_id){
        $sql = "SELECT  distinct
        c_app.os_id,c_app.id,c_app.app_id,c_generalize.platform_id,c_generalize.data_account,c_generalize.application_id,c_generalize.application_name,c_generalize.agency_platform_id,c_generalize_ad_app.campaign_id,c_generalize_ad_app.campaign_name,c_generalize_ad_app.ad_group_id,c_platform.currency_type_id,cpp.currency_type_id as ageccy_currency_type_id
        FROM c_app 
        LEFT JOIN c_generalize ON c_app.id = c_generalize.app_id 
        LEFT JOIN c_generalize_ad_app ON c_generalize.id = c_generalize_ad_app.generalize_id 
        LEFT JOIN c_platform ON c_generalize.platform_id = c_platform.platform_id 
        LEFT JOIN c_platform as cpp ON c_generalize.agency_platform_id = cpp.platform_id 
        WHERE 
        c_generalize.platform_id = '$source_id'";
        $app_list = DB::select($sql);
        $app_list = Service::data($app_list);
        return  $app_list;
    }

    public static function zplay_basic_handwork_daily(){
        $date = date('Y-m-d');

        DB::beginTransaction();

        $count = DB::select(" select count(1) as count from zplay_basic_handwork_daily a WHERE
a.date in (
select distinct all_date.date from (
SELECT date from zplay_ad_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m-%d') = '$date' and app_id is not null  GROUP BY date
union all 
SELECT date from zplay_tg_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m-%d') = '$date' and app_id is not null  GROUP BY date
) all_date
) and
a.app_id in (
        select distinct all_app.app_id from (
SELECT app_id from zplay_ad_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m-%d') = '$date' and app_id is not null  GROUP BY app_id
union all 
SELECT app_id from zplay_tg_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m-%d') = '$date' and app_id is not null  GROUP BY app_id
) all_app
       ) and
a.platform_id in (select distinct all_app.platform_id from (
SELECT platform_id from zplay_ad_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m-%d') = '$date' and app_id is not null  GROUP BY platform_id
union all 
SELECT platform_id from zplay_tg_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m-%d') = '$date' and app_id is not null  GROUP BY platform_id
) all_app
        ); ");
        $count = Service::data($count);
        if( $count && $count[0]['count'] > 0 ){
            var_dump('原：'.$count[0]['count']);
            $del = DB::delete(" delete from zplay_basic_handwork_daily WHERE
date in (
select distinct all_date.date from (
SELECT date from zplay_ad_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m-%d') = '$date' and app_id is not null  GROUP BY date
union all 
SELECT date from zplay_tg_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m-%d') = '$date' and app_id is not null  GROUP BY date
) all_date
) and
app_id in (
        select distinct all_app.app_id from (
SELECT app_id from zplay_ad_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m-%d') = '$date' and app_id is not null  GROUP BY app_id
union all 
SELECT app_id from zplay_tg_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m-%d') = '$date' and app_id is not null  GROUP BY app_id
) all_app
       ) and
platform_id in (select distinct all_app.platform_id from (
SELECT platform_id from zplay_ad_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m-%d') = '$date' and app_id is not null  GROUP BY platform_id
union all 
SELECT platform_id from zplay_tg_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m-%d') = '$date' and app_id is not null  GROUP BY platform_id
) all_app
        ); ");
            if(!$del){
                DB::rollBack();
            }
            var_dump('删：'.$del);
        }

        $sql = "insert into zplay_basic_handwork_daily( date,
        platform_id,
        app_id,
        os_id,
        handwork_income,
        income,
        handwork_cost,
        cost,
        create_time)
        SELECT  s.date,
        s.platform_id,
        s.app_id,
        s.os_id,
        sum(s.handwork_incom)AS handwork_incom,
        sum(s.incom) AS income,
        sum(s.handwork_cost)AS handwork_cost,
        sum(s.cost) AS cost, 
        now() AS create_time
        from  (SELECT
        date,
        platform_id,
        app_id,
        os_id,
        sum(income) + sum(biding_income) AS handwork_incom,
        0 AS incom,
        0 AS handwork_cost,
        0 AS cost
        FROM
        zplay_ad_handwork_daily
        WHERE
        DATE_FORMAT(create_time, '%Y-%m-%d') = '$date'
        AND app_id IS NOT NULL
        GROUP BY
        date,
        platform_id,
        app_id
        union all 
        SELECT
        date,
        platform_id,
        app_id,
        os_id,
        0 AS handwork_incom,
        0 AS incom,
        sum(cost) AS handwork_cost,
        0 AS cost
        FROM
        zplay_tg_handwork_daily
        WHERE
        DATE_FORMAT(create_time, '%Y-%m-%d') = '$date'
        AND app_id IS NOT NULL
        GROUP BY
        date,
        platform_id,
        app_id
        union all
        SELECT
        a.date,
        a.platform_id,
        a.app_id,
        b.os_id as os_id,
        0 AS handwork_incom,
        sum(a.earning) AS incom,
        0 AS handwork_cost,
        0 AS cost
        FROM
        zplay_ad_report_daily a ,c_app b
        WHERE
        a.app_id = b.app_id AND
        a.date in (SELECT date from zplay_ad_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m-%d') = '$date' and app_id is not null  GROUP BY date) and 
        a.app_id in (SELECT app_id from zplay_ad_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m-%d') = '$date' and app_id is not null  GROUP BY app_id) and
        a.platform_id in (SELECT platform_id from zplay_ad_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m-%d') = '$date' and app_id is not null  GROUP BY platform_id) 
        and  a.earning!=0 and statistics = 0  and flow_type  = 1 and b.`company_id` <> 9
        GROUP BY
        date,
        platform_id,
        app_id
        union all
        SELECT
        a.date,
        a.platform_id,
        a.app_id,
        b.os_id as os_id,
        0 AS handwork_incom,
        0 AS incom,
        0 AS handwork_cost,
        sum(a.cost) AS cost
        FROM
        zplay_tg_report_daily a ,c_app b
        WHERE
        a.app_id = b.app_id AND
        a.date in (SELECT date from zplay_tg_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m-%d') = '$date' and app_id is not null  GROUP BY date) and 
        a.app_id in (SELECT app_id from zplay_tg_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m-%d') = '$date' and app_id is not null  GROUP BY app_id) and
        a.platform_id in (SELECT platform_id from zplay_tg_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m-%d') = '$date' and app_id is not null  GROUP BY platform_id) 
        and  a.cost!=0 and b.`company_id` <> 9
        GROUP BY
        date,
        platform_id,
        app_id) as s GROUP BY
        s.date,
        s.platform_id,
        s.app_id";
        $insert_res = DB::insert($sql);
        if (!$insert_res){
            DB::rollBack();
        }
        var_dump('insert：'.$insert_res);
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