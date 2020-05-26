<?php

namespace App\Console\Commands\ShellScript;

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

class HandWorkDataProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'HandWorkDataProcesses {dayid?} ';

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
        $sql = 'select * from c_uploadFile where status = 0';
        $info=DB::select($sql);
        $info = Service::data($info);
        if(!$info){
            echo '暂无处理数据';die;

        }
        $arr_del_ad = [];
        $arr_del_tg = [];
        foreach ($info as $k_1 => $v_1) {
            # code...
        
            $name = $v_1['file_name'];
            $platform_id = $v_1['platform_id'];
            $platform_type = $v_1['platform_type'];
            $date = $v_1['date'];
            //解析文件
            $file_path =env('UPLOAD_PATH').'/'.$date.'/'.$name;
            $num = 0;
            $arr = [];
            $arr_data = [];
            $fp = fopen($file_path,'r');

            while(!feof($fp)){

                $str= fgets($fp,1024);
                $str_arr = explode(',', $str);
                if($num ==0){
                    $arr=$str_arr;
                }else{
                    if ($str_arr ){
                        foreach ($str_arr as $aa_k => $aa_v){
                            if( !empty($aa_v) ){
                                $fileType = mb_detect_encoding($aa_v , array('UTF-8','GBK','LATIN1','BIG5')) ;
                                if( $fileType != 'UTF-8'){
                                    $aa_v = mb_convert_encoding($aa_v ,'utf-8' , $fileType);
                                    $str_arr[$aa_k] = trim($aa_v);
                                }
                            }
                        }
                    }
                    $arr_data[] = $str_arr;
                }
                $num++;
                

            }

            //去除空格
            foreach ($arr as $key => $value) {
                $arr[$key] = trim($value);
                # code...
            }
            // 表头和数据合并成key=>value 形式
            foreach ($arr_data as &$value) {
                if(count($arr) ==count($value)){
                    $value[0] = date('Y-m-d',strtotime($value[0]));
                    $value = array_combine($arr, $value);
                }
                
            }
            if(empty($arr_data)){
                echo '文件数据为空'."\r\n";die;
            }
            //   2/4 广告/推广匹配
            if($platform_type == 2){//广告
                $plat_config =self::platform_ad_cofig($platform_id);
                if(empty($plat_config)){
                    echo '广告配置查询失败'."\r\n";die;
                }
                $num_data = 0;
                $arr_info = [];
                $arr_info_1 = [];
                $arr_del = [];
                $app_id = '';
                $os_id = '';
                DB::beginTransaction();
                foreach ($arr_data as $p_k => $p_v) {
                    if(count($p_v)<7){
                        continue;
                    }
                    if($p_v['Revenue']==0 &&$p_v['Bidding Revenue']==0){
                        continue;
                    }
                    $num = 0;
                    foreach ($plat_config as $k => $v) {

                        if (!empty($p_v['Ad_Id'])) {
                            if ($p_v['Ad_Id'] == $v['ad_slot_id'] || $p_v['Ad_Id'] == $v['zone']  ) {
                                $app_id = $v['app_id'];
                                $os_id = $v['os_id'];
                                $num = 1;
                            }

                        }elseif(!empty($p_v['Ad_Name'])) {
                            if ($p_v['Ad_Name'] == $v['ad_slot_name']) {
                                $app_id = $v['app_id'];
                                $os_id = $v['os_id'];
                                $num = 1;
                            }

                        }elseif(!empty($p_v['App_Id'])) {
                            if ($p_v['App_Id'] == $v['platform_app_id']) {
                                $app_id = $v['app_id'];
                                $os_id = $v['os_id'];
                                $num = 1;
                            }

                        }elseif(!empty($p_v['App_Name/Package_Storeid'])) {
                            if ($p_v['App_Name/Package_Storeid'] == $v['platform_app_name']) {
                                $app_id = $v['app_id'];
                                $os_id = $v['os_id'];
                                $num = 1;
                            }

                        }
                    }
                    if($num ==1){
                            $arr_del_ad[$p_v['Data_Time'].'_'.$platform_id] =$platform_id;
                            $arr_info[$num_data]['date'] = $p_v['Data_Time'];
                            $arr_info[$num_data]['ad_id'] =isset($p_v['Ad_Id'])?$p_v['Ad_Id']:'' ;
                            $arr_info[$num_data]['ad_name'] = isset($p_v['Ad_Name'])?addslashes($p_v['Ad_Name']):'' ;
                            $arr_info[$num_data]['platform_app_id'] = isset($p_v['App_Id'])?$p_v['App_Id']:'' ;
                            $arr_info[$num_data]['platform_app_name'] = isset($p_v['App_Name/Package_Storeid'])?addslashes($p_v['App_Name/Package_Storeid']):'' ;
                            $arr_info[$num_data]['app_id'] = $app_id;
                            $arr_info[$num_data]['os_id'] = $os_id;
                            $arr_info[$num_data]['income'] = isset($p_v['Revenue'])?trim($p_v['Revenue']):0;
                            $arr_info[$num_data]['biding_income'] = isset($p_v['Bidding Revenue'])?trim($p_v['Bidding Revenue']):0 ;
                            $arr_info[$num_data]['platform_id'] = $platform_id;
                            $arr_info[$num_data]['create_time'] = date('Y-m-d H:i:s');
                            
                        }else{

                            $arr_del_ad[$p_v['Data_Time'].'_'.$platform_id] =$platform_id;
                            $arr_info_1[$num_data]['date'] = $p_v['Data_Time'];
                            $arr_info_1[$num_data]['ad_id'] =isset($p_v['Ad_Id'])?$p_v['Ad_Id']:'' ;
                            $arr_info_1[$num_data]['ad_name'] = isset($p_v['Ad_Name'])?addslashes($p_v['Ad_Name']):'' ;
                            $arr_info_1[$num_data]['platform_app_id'] = isset($p_v['App_Id'])?$p_v['App_Id']:'' ;
                            $arr_info_1[$num_data]['platform_app_name'] = isset($p_v['App_Name/Package_Storeid'])?addslashes($p_v['App_Name/Package_Storeid']):'' ;
                            $arr_info_1[$num_data]['app_id'] = 0;
                            $arr_info_1[$num_data]['os_id'] = 0;
                            $arr_info_1[$num_data]['income'] = isset($p_v['Revenue'])?$p_v['Revenue']:0 ;
                            $arr_info_1[$num_data]['biding_income'] = isset($p_v['Bidding Revenue'])?$p_v['Bidding Revenue']:0 ;
                            $arr_info_1[$num_data]['platform_id'] = $platform_id;
                            $arr_info_1[$num_data]['create_time'] = date('Y-m-d H:i:s');
                        }
                        $num_data++;

                }
                $map=[];
                foreach ( $arr_del_ad as $key => $value) {
                    $del = explode('_',$key);
                    $map['date'] = $del[0];
                    $map['platform_id'] = $del[1];
                    $count = DataUploadLogic::getMysqlData('zplay_ad_handwork_daily',$map)->count();
                    if($count){
                        $del = DataUploadLogic::deleteMysqlData('zplay_ad_handwork_daily',$map);
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
                
                $num_data = 0;
                $arr_info = [];
                $arr_info_1 = [];

                $app_id = '';
                $os_id = '';
                DB::beginTransaction();
                foreach ($arr_data as $p_k => $p_v) {
                    if(count($p_v)<6){
                        continue;
                    }
                    if($p_v['Spend']==0){
                        continue;
                    }
                    $num = 0;
                    foreach ($plat_config as $k => $v) {

                        if (!empty($p_v['Campaign_Id'])) {
                            if ($p_v['Campaign_Id'] == $v['campaign_id']) {
                                $app_id = $v['app_id'];
                                $os_id = $v['os_id'];
                                $num = 1;
                            }

                        }elseif(!empty($p_v['Campaign_Name'])) {
                            if ($p_v['Campaign_Name'] == $v['campaign_name']) {
                                $app_id = $v['app_id'];
                                $os_id = $v['os_id'];
                                $num = 1;
                            }

                        }elseif(!empty($p_v['App_Id'])) {
                            if ($p_v['App_Id'] == $v['application_id']) {
                                $app_id = $v['app_id'];
                                $os_id = $v['os_id'];
                                $num = 1;
                            }

                        }elseif(!empty($p_v['App_Name/Package_Storeid'])) {
                            if ($p_v['App_Name/Package_Storeid'] == $v['application_name']) {
                                $app_id = $v['app_id'];
                                $os_id = $v['os_id'];
                                $num = 1;
                            }

                        }
                    }
                    if($num ==1){
                            $arr_del_tg[$p_v['Data_Time'].'_'.$platform_id] =$platform_id;
                            $arr_info[$num_data]['date'] = $p_v['Data_Time'];
                            $arr_info[$num_data]['ad_id'] =isset($p_v['Campaign_Id'])?$p_v['Campaign_Id']:'' ;
                            $arr_info[$num_data]['ad_name'] = isset($p_v['Campaign_Name'])?addslashes($p_v['Campaign_Name']):'' ;
                            $arr_info[$num_data]['platform_app_id'] = isset($p_v['App_Id'])?$p_v['App_Id']:'' ;
                            $arr_info[$num_data]['platform_app_name'] = isset($p_v['App_Name/Package_Storeid'])?addslashes($p_v['App_Name/Package_Storeid']):'' ;
                            $arr_info[$num_data]['app_id'] = $app_id;
                            $arr_info[$num_data]['os_id'] = $os_id;
                            $arr_info[$num_data]['cost'] = isset($p_v['Spend'])?$p_v['Spend']:0 ;
                            $arr_info[$num_data]['platform_id'] = $platform_id;
                            $arr_info[$num_data]['create_time'] = date('Y-m-d H:i:s');
                            
                        }else{

                            $arr_del_tg[$p_v['Data_Time'].'_'.$platform_id] =$platform_id;
                            $arr_info_1[$num_data]['date'] = $p_v['Data_Time'];
                            $arr_info_1[$num_data]['ad_id'] =isset($p_v['Campaign_Id'])?$p_v['Campaign_Id']:'' ;
                            $arr_info_1[$num_data]['ad_name'] = isset($p_v['Campaign_Name'])?addslashes($p_v['Campaign_Name']):'' ;
                            $arr_info_1[$num_data]['app_id'] = 0;
                            $arr_info_1[$num_data]['os_id'] = 0;
                            $arr_info_1[$num_data]['platform_app_id'] = isset($p_v['App_Id'])?$p_v['App_Id']:'' ;
                            $arr_info_1[$num_data]['platform_app_name'] = isset($p_v['App_Name/Package_Storeid'])?addslashes($p_v['App_Name/Package_Storeid']):'' ;
                            $arr_info_1[$num_data]['cost'] = isset($p_v['Spend'])?$p_v['Spend']:0 ;
                            $arr_info_1[$num_data]['platform_id'] = $platform_id;
                            $arr_info_1[$num_data]['create_time'] = date('Y-m-d H:i:s');
                        }
                        $num_data++;

                }

                $map=[];
                foreach ( $arr_del_tg as $key => $value) {
                    $del = explode('_',$key);
                    $map['date'] = $del[0];
                    $map['platform_id'] = $del[1];
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
//        DB::beginTransaction();
        //往大表和数据
//        $arr_info_del = array_merge($arr_del_tg,$arr_del_ad);
//        foreach ( $arr_info_del as $key => $value) {
//            $del = explode('_',$key);
//            $map['date'] = $del[0];
//            $map['platform_id'] = $del[1];
//            $count = DataUploadLogic::getMysqlData('zplay_basic_handwork_daily',$map)->count();
//            if($count){
//                $del = DataUploadLogic::deleteMysqlData('zplay_basic_handwork_daily',$map);
//                if(!$del){
//                    DB::rollBack();
//                }
//            }
//
//        }
        $insert = self::zplay_basic_handwork_daily();
//        if(!$insert){
//            DB::rollBack();
//        }
//         DB::commit();
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

}