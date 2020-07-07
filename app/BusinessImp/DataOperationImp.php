<?php

namespace App\BusinessImp;

use App\BusinessLogic\ApplicationLogic;
use App\BusinessLogic\CommonLogic;
use App\BusinessLogic\DataImportLogic;
use App\BusinessLogic\DataSearchLogic;
use App\BusinessLogic\OperationLogLogic;
use App\Common\ApiResponseFactory;
use App\Common\CommonFunction;
use App\Common\Service;
use App\Common\CurlRequest;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\UserLogic;
use Illuminate\Support\Facades\DB;
use App\BusinessLogic\RoleLogic;

class DataOperationImp extends ApiBaseImp
{

    /**
     * 商业化运营数据报表 变现数据
     * @param  [type] $params [description]
     * @return [type]         [description]
     */
    public static function getRealizationData($params){
        //验证用户是否有权限登录
        $userid = $_SESSION['erm_data']['guid'];;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        //验证用户是否有权限登录
        $map1 = [];
        $map1['id'] = $userid;
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        // //返回用户下用权限列表
        $app_permission = [];
        if($userInfo[0]['app_permission'] != -2){
            $app_permission = explode(',', $userInfo[0]['app_permission']);
        }

        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 60;
        $basic_tg_table = 'zplay_basic_realization_report_total';
        if ($currency_type_id == 60){
            $basic_tg_table = 'zplay_basic_realization_report_total';
        }

        $data = [];

        // 查询条件
        $get_game = isset($params['app_select']) ? $params['app_select'] : '';
        $os_id = isset($params['os_id']) ? $params['os_id'] : '';
        $country_id = isset($params['country_id']) ? $params['country_id'] : '';

        $where = " where 1=1 ";
        $game_id_list = '';
        if($get_game){
            $where .= " and b.app_id  in ($get_game) ";
        }elseif($app_permission){
            $game_id_list = implode(',',$app_permission);
            $where .= " and b.app_id in($game_id_list)";
        }

        if ($os_id){
            $where .= " and b.os_id  = {$os_id}";
        }

        if ($country_id){
            $where .= " and b.country_id  = {$country_id}";
        }

        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        if($company == 9){
            $where .= " and b.company_id = 9 ";
        }elseif($company == 1 ){
            $where .= " and b.company_id != 9 ";
        }

        // 分区查询
        $start_time = isset($params['start_time']) ? $params['start_time'] : date('Y-m-d',strtotime("-7 days"));
        $end_time = isset($params['end_time']) ? $params['end_time'] : date('Y-m-d');
        $where .= " and date between '{$start_time}' and '{$end_time}'";

        // 分区查询
        $partition = '';
        $all_month_arr = Service::dateMonthsSections($start_time,$end_time);
        $all_month = [];
        if ($all_month_arr){
            foreach ($all_month_arr as $month_srt){
                $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
            }
            if ($all_month){
                $partition = " partition (".implode(',',$all_month).")";
            }
        }

//        $where .= ' and (new > 0 or active > 0 or new_nature > 0 or new_nonature > 0 or cost > 0 or earning_all > 0)';
        // 分组
        $group_by = ' group by b.date,b.app_id,b.country_id';
        $order_by = ' order by b.date desc,sum(new) desc';
        $sql = "select 
        date,
        app.id as appid,
        app.app_id,
        app.app_name,
        app.release_region_id,
        app.os_id,
        c.china_name,
        b.country_id,
        sum(new) as new,
        sum(active) as active,
        sum(tg_new) as tg_new,
        sum(session_time) as session_time,
        sum(session_length) as session_length,
        sum(interst_ad_imp) as interst_ad_imp,
        sum(video_ad_imp) as video_ad_imp,
        sum(video_ad_income) as video_ad_income,
        sum(interst_ad_income) as interst_ad_income,
        sum(ad_income) as ad_income,
        sum(ff_income) as ff_income,
        sum(tg_cost) as cost
        from {$basic_tg_table} {$partition} b
        left join c_app app on b.app_id = app.id
        left join c_country_standard c on b.country_id = c.id
        ".$where.$group_by.$order_by;


        $pageSize = isset($params['size']) ? $params['size'] : 99999;
        $p = isset($params['page']) ? $params['page'] : 1;


        $searchSql ="select SQL_CALC_FOUND_ROWS a.* from ($sql)a  ";
        $countSql = "select count(*) c from ($sql)a";

        $start = ($p-1) * $pageSize;
        $searchSql_p = $searchSql." limit {$start},{$pageSize}";

        $total_data = Db::select($searchSql_p);
        $total_data = Service::data($total_data);

        $array =[];

        foreach ($total_data as $key => $value) {

            $array[$key]['date'] =$value['date'];

            // 发行区域ID(1,全球;2,国外;3,国内;)
            if ($value['release_region_id'] == 1){
                $release_region_id = '全球-';
            }elseif ($value['release_region_id'] == 2){
                $release_region_id = '国外-';
            }elseif ($value['release_region_id'] == 3){
                $release_region_id = '国内-';
            }else{
                $release_region_id = '未知区域-';
            }

            // 操作系统(1,ios;2,Android;3,h5;4,Amazon;)
            if ($value['os_id'] == 1){
                $os_id = 'ios-';
            }elseif ($value['os_id'] == 2){
                $os_id = 'Android-';
            }elseif ($value['os_id'] == 3){
                $os_id = 'h5-';
            }elseif ($value['os_id'] == 4){
                $os_id = 'Amazon-';
            }else{
                $os_id = '未知系统-';
            }

            $array[$key]['app_id'] = $value['appid'];
            $array[$key]['app_name'] = $release_region_id.$os_id.$value['app_name'].'-'.$value['app_id'];
            $array[$key]['country_id'] = $value['country_id'];

            // 系统
            if ($value['os_id'] == 1){
                $os_id = 'iOS';
            }elseif ($value['os_id'] == 2){
                $os_id = 'Android';
            }else{
                $os_id = '未知';
            }
            $array[$key]['os'] = $os_id;

            $array[$key]['country'] =$value['china_name'] ? $value['china_name'] : '未知国家';
            $array[$key]['add_total'] =$value['new'] ? $value['new'] : 0;//总新增
            $array[$key]['active_total'] =$value['active'] ? $value['active'] : 0;//总活跃
            $array[$key]['session_time_per'] = $value['active'] ? round($value['session_time'] / $value['active'],2) : 0; // 人均启动次数
            $array[$key]['session_length_per'] = $value['session_time'] ? round($value['session_length'] / $value['session_time']/60, 2) : 0;// 单次游戏时长

            $array[$key]['insert_imp_per'] = $value['active'] ? round($value['interst_ad_imp'] / $value['active'],2) : 0; // 插屏人均展示
            $array[$key]['video_imp_per'] = $value['active'] ? round($value['video_ad_imp'] / $value['active'],2) : 0; // 视频人均展示

            $array[$key]['insert_ecpm'] = $value['interst_ad_imp'] ? round(($value['interst_ad_income'] * 1000) / $value['interst_ad_imp'], 2): 0; // 插屏ecpm
            $array[$key]['vider_ecpm'] = $value['video_ad_imp'] ? round(($value['video_ad_income'] * 1000) / $value['video_ad_imp'],2): 0; // 视频ecpm


            $array[$key]['cost'] = $value['cost'] ? round($value['cost'],2) : 0;// 推广成本
            $array[$key]['cost_per'] = $value['tg_new'] ? round($value['cost']/$value['tg_new'],2) : 0; // 推广单价

            $total_income = floatval($value['ad_income'])+floatval($value['ff_income']);
            $array[$key]['income'] = $total_income ? round($total_income,2) : 0; // 总收入

            $gross_profit = $total_income - $value['cost'] ; // 毛利润
            $array[$key]['gross_profit'] = $gross_profit ? round($gross_profit, 2) : 0;//毛利润

            $array[$key]['arpu'] = $value['active'] ? round($total_income / $value['active'],4) : 0.0000;//arpu

        }

        // 合计
        $table_total_sql = "select 
        date,
        app.id as appid,
        app.app_id,
        app.app_name,
        app.release_region_id,
        app.os_id,
        c.china_name,
        b.country_id,
        sum(new) as new,
        sum(active) as active,
        sum(tg_new) as tg_new,
        sum(session_time) as session_time,
        sum(session_length) as session_length,
        sum(interst_ad_imp) as interst_ad_imp,
        sum(video_ad_imp) as video_ad_imp,
        sum(video_ad_income) as video_ad_income,
        sum(interst_ad_income) as interst_ad_income,
        sum(ad_income) as ad_income,
        sum(ff_income) as ff_income,
        sum(tg_cost) as cost
        from {$basic_tg_table} {$partition} b
        left join c_app app on b.app_id = app.id
        left join c_country_standard c on b.country_id = c.id
        ".$where;

        $total_answer_data = [];
        $total_answer = DB::select($table_total_sql);
        $total_answer = Service::data($total_answer);

        if ($total_answer){
            foreach ($total_answer as $t_key => $t_data){

                $total_answer_data[$t_key]['date'] = '-';
                $total_answer_data[$t_key]['app_id'] = '-';
                $total_answer_data[$t_key]['app_name'] = '-';
                $total_answer_data[$t_key]['country_id'] = '-';
                $total_answer_data[$t_key]['os'] = '-';
                $total_answer_data[$t_key]['country'] = '-';
                $total_answer_data[$t_key]['add_total'] = $t_data['new'] ? $t_data['new'] : 0;//总新增
                $total_answer_data[$t_key]['active_total'] =$t_data['active'] ? $t_data['active'] : 0;//总活跃
                $total_answer_data[$t_key]['session_time_per'] = $t_data['active'] ? round($t_data['session_time'] / $t_data['active'],2) : 0; // 人均启动次数
                $total_answer_data[$t_key]['session_length_per'] = $t_data['session_time'] ? round($t_data['session_length'] / $t_data['session_time']/60, 2) : 0;// 单次游戏时长

                $total_answer_data[$t_key]['insert_imp_per'] = $t_data['active'] ? round($t_data['interst_ad_imp'] / $t_data['active'],2) : 0; // 插屏人均展示
                $total_answer_data[$t_key]['video_imp_per'] = $t_data['active'] ? round($t_data['video_ad_imp'] / $t_data['active'],2) : 0; // 视频人均展示

                $total_answer_data[$t_key]['insert_ecpm'] = $t_data['interst_ad_imp'] ? round(($t_data['interst_ad_income'] * 1000) / $t_data['interst_ad_imp'], 2): 0; // 插屏ecpm
                $total_answer_data[$t_key]['vider_ecpm'] = $t_data['video_ad_imp'] ? round(($t_data['video_ad_income'] * 1000) / $t_data['video_ad_imp'],2): 0; // 视频ecpm


                $total_answer_data[$t_key]['cost'] = $t_data['cost'] ? round($t_data['cost'],2) : 0;// 推广成本
                $total_answer_data[$t_key]['cost_per'] = $t_data['tg_new'] ? round($t_data['cost']/$t_data['tg_new'],2) : 0; // 推广单价

                $total_income = floatval($t_data['ad_income'])+floatval($t_data['ff_income']);
                $total_answer_data[$t_key]['income'] = $total_income ? round($total_income,2) : 0; // 总收入

                $gross_profit = $total_income - $t_data['cost'] ; // 毛利润
                $total_answer_data[$t_key]['gross_profit'] = $gross_profit ? round($gross_profit, 2) : 0;//毛利润

                $total_answer_data[$t_key]['arpu'] = $t_data['active'] ? round($total_income / $t_data['active'],4) : 0.0000;//arpu
            }
        }

        $c_answer = DB::select($countSql);
        $c_answer = Service::data($c_answer);
        $count = $c_answer['0']['c'];

        $pageAll = ceil($count/$pageSize);
        $data['total'] = $count;
        $data['page_total'] = $pageAll;
        $data['table_list'] = $array;
        $data['table_total'] = $total_answer_data;

        ApiResponseFactory::apiResponse($data,[]);
    }

    /**
     * 游戏发行运营数据报表 发行数据
     * @param  [type] $params [description]
     * @return [type]         [description]
     */
    public static function getPublishData($params){
        //验证用户是否有权限登录
        $userid = $_SESSION['erm_data']['guid'];;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        //验证用户是否有权限登录
        $map1 = [];
        $map1['id'] = $userid;
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        // //返回用户下用权限列表
        $app_permission = [];
        if($userInfo[0]['app_permission'] != -2){
            $app_permission = explode(',', $userInfo[0]['app_permission']);
        }

        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 60;

        $is_show = isset($params['is_show']) ? $params['is_show'] : 2 ; // 1 展示 2 不展示
        $language = isset($params['language']) ? $params['language'] : 'cn'; // 中英文  en 英文  cn 中文

        $basic_tg_table = 'zplay_basic_publish_report_total';
        if ($is_show == 1){
            // 按国家展示
            $basic_tg_table = 'zplay_basic_publish_country_report_total';
        }

        $data = [];

        // 查询条件
        $get_game = isset($params['app_select']) ? $params['app_select'] : '';
        $os_id = isset($params['os_id']) ? $params['os_id'] : '';
        $country_id = isset($params['country_id']) ? $params['country_id'] : '';


        $where = " where 1=1 ";
        $game_id_list = '';
        if($get_game){
            $where .= " and b.app_id in ($get_game)";
        }elseif($app_permission){
            $game_id_list = implode(',',$app_permission);
            $where .= " and b.app_id in($game_id_list)";
        }

        if ($os_id){
            $where .= " and b.os_id  = {$os_id}";
        }

        if ($is_show == 1) {
            if ($country_id) {
                $where .= " and b.country_id  = {$country_id}";
            }
        }


        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        if($company == 9){
            $where .= " and b.company_id = 9 ";
        }elseif($company == 1 ){
            $where .= " and b.company_id != 9 ";
        }

        // 分区查询
        $start_time = isset($params['start_time']) ? $params['start_time'] : date('Y-m-d',strtotime("-7 days"));
        $end_time = isset($params['end_time']) ? $params['end_time'] : date('Y-m-d');
        $where .= " and date between '{$start_time}' and '{$end_time}'";

        // 分区查询
        $partition = '';
        $all_month_arr = Service::dateMonthsSections($start_time,$end_time);
        $all_month = [];
        if ($all_month_arr){
            foreach ($all_month_arr as $month_srt){
                $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
            }
            if ($all_month){
                $partition = " partition (".implode(',',$all_month).")";
            }
        }

//        $where .= ' and (new > 0 or active > 0 or new_nature > 0 or new_nonature > 0 or cost > 0 or earning_all > 0)';
        // 分组
        $group_by = ' group by b.date,b.app_id';
        $order_by = ' order by b.date desc,sum(new) desc';
        if ($is_show == 1){
            $group_by .= ',b.country_id';
        }

        $new_select = '';
        if ($language == 'cn'){
            $new_select .= " c.china_name as china_name, ";
        }elseif($language == 'en'){
            $new_select .= " c.full_name as china_name, ";
        }

        if ($is_show == 1) {
            $sql = "select 
            date,
            app.id as appid,
            app.app_id,
            app.app_name,
            app.release_region_id,
            app.os_id,
            {$new_select}
            b.country_id,
            sum(new) as new,
            sum(active) as active,
            sum(tg_new) as tg_new,
            sum(session_time) as session_time,
            sum(session_length) as session_length,
            sum(interst_ad_imp) as interst_ad_imp,
            sum(video_ad_imp) as video_ad_imp,
            sum(ad_income) as ad_income,
            sum(ff_income) as ff_income,
            sum(tg_cost) as cost
            from {$basic_tg_table} {$partition} b
            left join c_app app on b.app_id = app.id
            left join c_country_standard c on b.country_id = c.id
            " . $where . $group_by . $order_by;
        }else{
            $sql = "select 
            date,
            app.id as appid,
            app.app_id,
            app.app_name,
            app.release_region_id,
            app.os_id,
            sum(new) as new,
            sum(active) as active,
            sum(tg_new) as tg_new,
            sum(session_time) as session_time,
            sum(session_length) as session_length,
            sum(interst_ad_imp) as interst_ad_imp,
            sum(video_ad_imp) as video_ad_imp,
            sum(keep_day2) as keep_day2,
            sum(keep_day7) as keep_day7,
            sum(ad_income) as ad_income,
            sum(ff_income) as ff_income,
            sum(tg_cost) as cost
            from {$basic_tg_table} {$partition} b
            left join c_app app on b.app_id = app.id
            " . $where . $group_by . $order_by;
        }



        $pageSize = isset($params['size']) ? $params['size'] : 99999;
        $p = isset($params['page']) ? $params['page'] : 1;


        $searchSql ="select SQL_CALC_FOUND_ROWS a.* from ($sql)a  ";
        $countSql = "select count(*) c from ($sql)a";

        $start = ($p-1) * $pageSize;
        $searchSql_p = $searchSql." limit {$start},{$pageSize}";

        $total_data = Db::select($searchSql_p);
        $total_data = Service::data($total_data);

        $array =[];

        foreach ($total_data as $key => $value) {

            $array[$key]['date'] =$value['date'];

            // 发行区域ID(1,全球;2,国外;3,国内;)
            if ($value['release_region_id'] == 1){
                $release_region_id = '全球-';
            }elseif ($value['release_region_id'] == 2){
                $release_region_id = '国外-';
            }elseif ($value['release_region_id'] == 3){
                $release_region_id = '国内-';
            }else{
                $release_region_id = '未知区域-';
            }

            // 操作系统(1,ios;2,Android;3,h5;4,Amazon;)
            if ($value['os_id'] == 1){
                $os_id = 'ios-';
            }elseif ($value['os_id'] == 2){
                $os_id = 'Android-';
            }elseif ($value['os_id'] == 3){
                $os_id = 'h5-';
            }elseif ($value['os_id'] == 4){
                $os_id = 'Amazon-';
            }else{
                $os_id = '未知系统-';
            }

            $array[$key]['app_id'] = $value['appid'];
            $array[$key]['app_name'] = $release_region_id.$os_id.$value['app_name'].'-'.$value['app_id'];

            // 系统
            if ($value['os_id'] == 1){
                $os_id = 'iOS';
            }elseif ($value['os_id'] == 2){
                $os_id = 'Android';
            }else{
                $os_id = '未知';
            }
            $array[$key]['os'] = $os_id;
            $array[$key]['add_total'] = $value['new'] ? $value['new'] : 0;//总新增
            $array[$key]['active_total'] = $value['active'] ? $value['active'] : 0;//总活跃
            $array[$key]['session_time_per'] = $value['active'] ? round($value['session_time'] / $value['active'] ,2): 0; // 人均启动次数
            $array[$key]['session_length_per'] = $value['session_time'] ? round($value['session_length'] / $value['session_time']/60,2) : 0;// 单次游戏时长

            // 分国家展示
            if ($is_show == 1) {
                $array[$key]['country_id'] = $value['country_id'];
                if ($language == 'cn') {
                    $array[$key]['country'] = $value['china_name'] ? $value['china_name'] : '未知国家';
                }elseif($language == 'en'){
                    $array[$key]['country'] = $value['china_name'] ? $value['china_name'] : 'Unkonw';
                }
            }else{
                $array[$key]['keep_day7_rate'] = $value['new'] ? round(($value['keep_day7'] * 100) / $value['new'], 2) . "%" : "0.00%"; // 7日留存
                $array[$key]['keep_day2_rate'] = $value['new'] ? round(($value['keep_day2'] * 100) / $value['new'], 2) . "%" : "0.00%"; // 次日留存
            }

            $array[$key]['insert_imp_per'] = $value['active'] ? round($value['interst_ad_imp'] / $value['active'],2) : 0; // 插屏人均展示
            $array[$key]['video_imp_per'] = $value['active'] ? round($value['video_ad_imp'] / $value['active'],2) : 0; // 视频人均展示


            $array[$key]['cost'] = $value['cost'] ? round($value['cost'],2) : 0;// 推广成本
//            $array[$key]['cost_per'] = $value['tg_new'] ? round($value['cost']/$value['tg_new'],2) : 0; // 推广单价

            $total_income = floatval($value['ad_income'])+floatval($value['ff_income']);
            $array[$key]['income'] = $total_income ? round($total_income,2) : 0; // 总收入

            $gross_profit = $total_income - $value['cost'] ; // 毛利润
            $array[$key]['gross_profit'] = $gross_profit ? round($gross_profit, 2) : 0;//毛利润

            $array[$key]['arpu'] = $value['active'] ? round($total_income / $value['active'],4) : 0.0000;//arpu

        }

        // 合计

        if ($is_show == 1) {
            $table_total_sql = "select 
            date,
            app.id as appid,
            app.app_id,
            app.app_name,
            app.release_region_id,
            app.os_id,
            {$new_select}
            b.country_id,
            sum(new) as new,
            sum(active) as active,
            sum(tg_new) as tg_new,
            sum(session_time) as session_time,
            sum(session_length) as session_length,
            sum(interst_ad_imp) as interst_ad_imp,
            sum(video_ad_imp) as video_ad_imp,
            sum(ad_income) as ad_income,
            sum(ff_income) as ff_income,
            sum(tg_cost) as cost
            from {$basic_tg_table} {$partition} b
            left join c_app app on b.app_id = app.id
            left join c_country_standard c on b.country_id = c.id
            " . $where ;
        }else{
            $table_total_sql = "select 
            date,
            app.id as appid,
            app.app_id,
            app.app_name,
            app.release_region_id,
            app.os_id,
            sum(new) as new,
            sum(active) as active,
            sum(tg_new) as tg_new,
            sum(session_time) as session_time,
            sum(session_length) as session_length,
            sum(interst_ad_imp) as interst_ad_imp,
            sum(video_ad_imp) as video_ad_imp,
            sum(keep_day2) as keep_day2,
            sum(keep_day7) as keep_day7,
            sum(ad_income) as ad_income,
            sum(ff_income) as ff_income,
            sum(tg_cost) as cost
            from {$basic_tg_table} {$partition} b
            left join c_app app on b.app_id = app.id
            " . $where;
        }

        $total_answer_data = [];
        $total_answer = DB::select($table_total_sql);
        $total_answer = Service::data($total_answer);

        if ($total_answer){
            foreach ($total_answer as $t_key => $t_data){

                $total_answer_data[$t_key]['date'] = '-';
                $total_answer_data[$t_key]['app_id'] = '-';
                $total_answer_data[$t_key]['app_name'] = '-';
                $total_answer_data[$t_key]['os'] = '-';
                $total_answer_data[$t_key]['add_total'] = $t_data['new'] ? $t_data['new'] : 0;//总新增
                $total_answer_data[$t_key]['active_total'] = $t_data['active'] ? $t_data['active'] : 0;//总活跃
                $total_answer_data[$t_key]['session_time_per'] = $t_data['active'] ? round($t_data['session_time'] / $t_data['active'] ,2): 0; // 人均启动次数
                $total_answer_data[$t_key]['session_length_per'] = $t_data['session_time'] ? round($t_data['session_length'] / $t_data['session_time']/60,2) : 0;// 单次游戏时长

                // 分国家展示
                if ($is_show == 1) {
                    $total_answer_data[$t_key]['country_id'] = '-';
                    $total_answer_data[$t_key]['country'] = '-';
                }else{
                    $total_answer_data[$t_key]['keep_day7_rate'] = '-'; // 7日留存
                    $total_answer_data[$t_key]['keep_day2_rate'] = '-'; // 次日留存
                }

                $total_answer_data[$t_key]['insert_imp_per'] = $t_data['active'] ? round($t_data['interst_ad_imp'] / $t_data['active'],2) : 0; // 插屏人均展示
                $total_answer_data[$t_key]['video_imp_per'] = $t_data['active'] ? round($t_data['video_ad_imp'] / $t_data['active'],2) : 0; // 视频人均展示


                $total_answer_data[$t_key]['cost'] = $t_data['cost'] ? round($t_data['cost'],2) : 0;// 推广成本
//            $total_answer_data[$t_key]['cost_per'] = $t_data['tg_new'] ? round($t_data['cost']/$t_data['tg_new'],2) : 0; // 推广单价

                $total_income = floatval($t_data['ad_income'])+floatval($t_data['ff_income']);
                $total_answer_data[$t_key]['income'] = $total_income ? round($total_income,2) : 0; // 总收入

                $gross_profit = $total_income - $t_data['cost'] ; // 毛利润
                $total_answer_data[$t_key]['gross_profit'] = $gross_profit ? round($gross_profit, 2) : 0;//毛利润

                $total_answer_data[$t_key]['arpu'] = $t_data['active'] ? round($total_income / $t_data['active'],4) : 0.0000;//arpu
            }
        }


        $c_answer = DB::select($countSql);
        $c_answer = Service::data($c_answer);
        $count = $c_answer['0']['c'];

        $pageAll = ceil($count/$pageSize);
        $data['total'] = $count;
        $data['page_total'] = $pageAll;
        $data['table_list'] = $array;
        $data['table_total'] = $total_answer_data;

        ApiResponseFactory::apiResponse($data,[]);
    }

}
