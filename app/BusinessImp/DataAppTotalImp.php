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

class DataAppTotalImp extends ApiBaseImp
{

    /**
     * // 累计页面 左侧列表接口
     * @param  [type] $params [description]
     * @return [type]         [description]
     */
    public static function getAppTotalList($params){
        $userid = $_SESSION['erm_data']['guid'];
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        // 列表参数：1、应用大类 ; 2、应用名称
        $app_type_id = isset($params['app_type_id']) ? $params['app_type_id'] : 1;
        // 时间参数：1、累计；2、本月；3、上月；4、今年；5、去年
        $period_id = isset($params['period_id']) ? $params['period_id'] : 1;
        // 排序指标：1、新增用户；2、活跃用户；3、付费收入；4、广告收入；5、总收入；6、推广成本；7、毛利润；8、开发者分成；9、总利润
        $target_id = isset($params['target_id']) ? $params['target_id'] : 1;
        // 分页信息
        $size = isset($params['limit']) ? $params['limit'] : '';
        // 公司
        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;

        //判断参数是否正确
        if (!in_array($app_type_id,[1,2])){
            ApiResponseFactory::apiResponse([],[],1020);
        }
        if (!in_array($period_id,[1,2,3,4,5])){
            ApiResponseFactory::apiResponse([],[],1021);
        }
        if (!in_array($target_id,[1,2,3,4,5,6,7,8,9])){
            ApiResponseFactory::apiResponse([],[],1022);
        }

        if ($company){
            $company_info = DB::select(" select * from c_zplay_company where id = {$company}");
            $company_info = Service::data($company_info);
            if (!$company_info){
                ApiResponseFactory::apiResponse([],[],1023);
            }
        }

        // 拼接查询条件
        $where = '';
        $app_where = '';
        $group_by = '';
        $select = '';
        $order_by = '';
        $limit = '';

        if($company == 9){
            $where .= ' where company_id = 9 ' ;
            $app_where .= ' where company_id = 9 ' ;
        }elseif($company != 9 ){
            $where .= ' where company_id <> 9 ' ;
            $app_where .= ' where company_id <> 9 ' ;
        }

        //验证用户是否有权限登录
        $map1 = [];
        $map1['id'] = $userid;
        $user_info = UserLogic::Userlist($map1)->get();
        $user_info =Service::data($user_info);
        if(!$user_info) ApiResponseFactory::apiResponse([],[],741);
        //返回用户下用权限列表
        $app_permission = '';
        if($user_info[0]['app_permission'] != -2){
            $app_permission = $user_info[0]['app_permission'];
        }

        if($app_permission) {
            $where .= " and app_id in ($app_permission) ";
            $app_where .= " and id in ($app_permission) ";
        }

        if ($app_type_id == 1){
            $select .= ' app_full_name as value ';
            $group_by .= ' group by app_full_name ';
        }else{
            $select .= ' app_id,app_name as value ';
            $group_by .= ' group by app_name ';
        }

        if ($period_id == 1){

        }elseif($period_id == 2){
            $currency_month = date("Y-m",time());
            $where .= " and date_time = '{$currency_month}' ";
        }elseif($period_id == 3){
            $last_month = date("Y-m",strtotime('-1 month'));
            $where .= " and date_time = '{$last_month}' ";
        }elseif($period_id == 4){
            $start_month = date("Y-01",strtotime('-0 year'));
            $end_month = date("Y-m",strtotime('-0 month'));
            $where .= " and date_time between '{$start_month}' and '{$end_month}' ";
        }elseif($period_id == 5){
            $start_month = date("Y-01",strtotime('-1 year'));
            $end_month = date("Y-12",strtotime('-1 year'));
            $where .= " and date_time between '{$start_month}' and '{$end_month}' ";
        }

        // 1、新增用户；2、活跃用户；3、付费收入；4、广告收入；5、总收入；6、推广成本；7、毛利润；8、开发者分成；9、总利润
        if ($target_id == 1){
            $select .= ' ,sum(new_user) as num ';
            $order_by .= ' order by sum(new_user) desc ';
        }elseif ($target_id == 2){
            $select .= ' ,sum(active_user) as num ';
            $order_by .= ' order by sum(active_user) desc ';
        }elseif ($target_id == 3){
            $select .= ' ,sum(ff_income)  as num ';
            $order_by .= ' order by sum(ff_income) desc ';
        }elseif ($target_id == 4){
            $select .= ' ,sum(ad_income) as num ';
            $order_by .= ' order by sum(ad_income) desc ';
        }elseif ($target_id == 5){
            $select .= ' ,sum(total_income) as num';
            $order_by .= ' order by sum(total_income) desc ';
        }elseif ($target_id == 6){
            $select .= ' ,sum(tg_cost) as num';
            $order_by .= ' order by sum(tg_cost) desc ';
        }elseif ($target_id == 7){
            $select .= ' ,sum(gross_profit) as num ';
            $order_by .= ' order by sum(gross_profit) desc ';
        }elseif ($target_id == 8){
            $select .= ' ,sum(developer_divide) as num ';
            $order_by .= ' order by sum(developer_divide) desc ';
        }elseif ($target_id == 9){
            $select .= ' ,sum(total_profit) as num ';
            $order_by .= ' order by sum(total_profit) desc ';
        }

        $table_name = 'zplay_app_total_report';
        if ($size){
            $limit .= " limit {$size} ";
        }
        $sql = " select {$select} from $table_name $where $group_by $order_by $limit ;";

        $data_list = DB::select($sql);
        $data_list = Service::data($data_list);

        $app_sql = " select app_full_name,id from c_app $app_where group by app_full_name,id;";
        $app_ids = DB::select($app_sql);
        $app_ids = Service::data($app_ids);

        $app_list = [];
        if ($data_list){
            if ($app_type_id == 1){
                foreach ($data_list as $data_key => $data_info){
                    $app_full_name = $data_info['value'];
                    $app_ids_arr = [];
                    if ($app_ids){
                        foreach ($app_ids as $app_id){
                            if ($app_full_name == $app_id['app_full_name']) {
                                $app_ids_arr[] = $app_id['id'];
                            }
                        }
                    }
                    $data_list[$data_key]['app_id'] = implode(',',$app_ids_arr);
                }
            }
            $app_list['list'] = $data_list;
        }else{

            ApiResponseFactory::apiResponse([],[],302);
        }

        ApiResponseFactory::apiResponse($app_list,[]);
    }

    /**
     * // 累计页面 右侧列表接口
     * @param  [type] $params [description]
     * @return [type]         [description]
     */
    public static function getAppTotalData($params){
        $userid = $_SESSION['erm_data']['guid'];
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        // 列表参数：1、应用大类 ; 2、应用名称
        $app_type_id = isset($params['app_type_id']) ? $params['app_type_id'] : 1;
        // 公司
        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        // 应用ID
        $app_id = isset($params['app_id']) ? $params['app_id'] : '';
        // 开始时间
        $start_date = isset($params['start_date']) ? $params['start_date'] : date("Y-m",strtotime("-1 month"));
        // 结束时间
        $end_date = isset($params['end_date']) ? $params['end_date'] : date("Y-m",time());


        //判断参数是否正确
        if (!in_array($app_type_id,[1,2])){
            ApiResponseFactory::apiResponse([],[],1020);
        }
        if ($company){
            $company_info = DB::select(" select * from c_zplay_company where id = {$company}");
            $company_info = Service::data($company_info);
            if (!$company_info){
                ApiResponseFactory::apiResponse([],[],1023);
            }
        }

        // 拼接查询条件
        $where = '';
        $app_where = '';
        $total_where = '';
        $group_by = '';
        $total_group_by = '';
        $total_select = '';
        $select = '';
        $order_by = '';
        $limit = '';

        if($company == 9){
            $where .= ' where company_id = 9 ' ;
            $app_where .= ' where company_id = 9 ' ;
            $total_where .= ' where company_id = 9 ' ;
        }elseif($company != 9 ){
            $where .= ' where company_id <> 9 ' ;
            $app_where .= ' where company_id <> 9 ' ;
            $total_where .= ' where company_id <> 9 ' ;
        }

        // 时间范围
        $where .= " and date_time >= '{$start_date}' and date_time <= '{$end_date}' ";
//        $app_where .= " and date_time >= '{$start_date}' and date_time <= '{$end_date}' ";

        //验证用户是否有权限登录
        $map1 = [];
        $map1['id'] = $userid;
        $user_info = UserLogic::Userlist($map1)->get();
        $user_info =Service::data($user_info);
        if(!$user_info) ApiResponseFactory::apiResponse([],[],741);
        //返回用户下用权限列表
        $app_permission = '';
        if($user_info[0]['app_permission'] != -2){
            $app_permission = $user_info[0]['app_permission'];
        }
        $function_permission = [];
        if($user_info[0]['function_permission'] != -2){
            $function_permission = explode(',',$user_info[0]['function_permission']);
        }

        $select .= ' date_time, ';
        $group_by .= ' group by date_time ';
        if ($app_id){
            $where .= " and app_id in ($app_id) ";
            if ($app_type_id == 1){
                $select .= ' app_full_name as data_name, ';
                $total_select .= ' app_full_name as data_name, ';
                $group_by .= ' ,app_full_name ';
                $total_group_by .= ' group by app_full_name ';
            }elseif($app_type_id == 2){
                $select .= ' app_name as data_name, ';
                $total_select .= ' app_name as data_name, ';
                $group_by .= ' ,app_name ';
                $total_group_by .= ' group by app_name ';
            }
            $app_where .= " and id in ($app_id) ";
            $total_where .= " and app_id in ($app_id) ";
        }elseif($app_permission) {
            $where .= " and app_id in ($app_permission) ";
            $app_where .= " and id in ($app_permission) ";
            $total_where .= " and app_id in ($app_permission) ";
        }

        $order_by = " order by date_time ";

        // 1、新增用户；2、活跃用户；3、付费收入；4、广告收入；5、总收入；6、推广成本；7、毛利润；8、开发者分成；9、总利润

        $target_list = [];
        if ($function_permission){
            if (in_array(58,$function_permission)){
                $select .= ' sum(new_user) as new_user ,';
                $total_select .= ' sum(new_user) as new_user ,';
                $target_list[] = 58;
            }
            if (in_array(59,$function_permission)){
                $select .= ' sum(active_user) as active_user, ';
                $total_select .= ' sum(active_user) as active_user, ';
                $target_list[] = 59;
            }
            if (in_array(60,$function_permission)){
                $select .= ' sum(ff_income)  as ff_income, ';
                $total_select .= ' sum(ff_income)  as ff_income, ';
                $target_list[] = 60;
            }
            if (in_array(61,$function_permission)){
                $select .= ' sum(ad_income) as ad_income, ';
                $total_select .= ' sum(ad_income) as ad_income, ';
                $target_list[] = 61;
            }
            if (in_array(62,$function_permission)){
                $select .= ' sum(total_income) as total_income, ';
                $total_select .= ' sum(total_income) as total_income, ';
                $target_list[] = 62;
            }
            if (in_array(63,$function_permission)){
                $select .= ' sum(tg_cost) as tg_cost, ';
                $total_select .= ' sum(tg_cost) as tg_cost, ';
                $target_list[] = 63;
            }
            if (in_array(64,$function_permission)){
                $select .= ' sum(gross_profit) as gross_profit, ';
                $total_select .= ' sum(gross_profit) as gross_profit, ';
                $target_list[] = 64;
            }
            if (in_array(65,$function_permission)){
                $select .= ' sum(developer_divide) as developer_divide, ';
                $total_select .= ' sum(developer_divide) as developer_divide, ';
                $target_list[] = 65;
            }
            if (in_array(66,$function_permission)){
                $select .= ' sum(total_profit) as total_profit, ';
                $total_select .= ' sum(total_profit) as total_profit, ';
                $target_list[] = 66;
            }

            if (!$target_list) ApiResponseFactory::apiResponse([],[],1026);
            if ($select) $select = rtrim(rtrim($select),',');
            if ($total_select) $total_select = rtrim(rtrim($total_select),',');

        }else{
            $select .= ' sum(new_user) as new_user ,sum(active_user) as active_user,sum(ff_income)  as ff_income ,sum(ad_income) as ad_income ,sum(total_income) as total_income,sum(tg_cost) as tg_cost,sum(gross_profit) as gross_profit ,sum(developer_divide) as developer_divide ,sum(total_profit) as total_profit ';
            $total_select .= ' sum(new_user) as new_user ,sum(active_user) as active_user,sum(ff_income)  as ff_income ,sum(ad_income) as ad_income ,sum(total_income) as total_income,sum(tg_cost) as tg_cost,sum(gross_profit) as gross_profit ,sum(developer_divide) as developer_divide ,sum(total_profit) as total_profit ';
        }



        $table_name = 'zplay_app_total_report';

        $sql = " select {$select} from $table_name $where $group_by $order_by ;";
        $data_list = DB::select($sql);
        $data_list = Service::data($data_list);

        $sum_sql = " select {$total_select} from $table_name $where $total_group_by ;";
        $sum_data_list = DB::select($sum_sql);
        $sum_data_list = Service::data($sum_data_list);

        $total_sql = " select {$total_select} from $table_name $total_where $total_group_by ;";
        $total_data_list = DB::select($total_sql);
        $total_data_list = Service::data($total_data_list);
//        var_dump($total_sql);var_dump($total_data_list);die;

        $app_sql = " select distinct app_full_name,concat(
            	(case when release_region_id = 1 then '全球'
            		 when release_region_id = 2 then '国外'
            		 when release_region_id = 3 then '国内'
            		 when release_region_id = 4 then '线下' 
             		 end),'-',
             	(case when os_id = 1 then 'IOS'
             		when os_id = 2 then '安卓'
             		when os_id = 3 then 'h5'
             		when os_id = 4 then 'Amazon'
             		end),'-',
             		(case when app_name is null then '未知应用' else app_name end),'-',
             		(case when app_id is null then '未知ID' else app_id end)			
            ) as app_name from c_app $app_where ";
        $app_ids = DB::select($app_sql);
        $app_ids = Service::data($app_ids);

        if (!$app_ids){
            ApiResponseFactory::apiResponse([],[],1025);
        }


        if ($target_list){
            foreach ($target_list as $target_list_key){
                $empty_table_list[] = 0;
            }
        }else{
            $empty_table_list = [0, 0, 0, 0, 0, 0, 0, 0, 0];
        }
        $app_list = [];
        if ($data_list){
            if ($app_type_id == 1 && $app_id){
                foreach ($data_list as $data_key => $data_info){
                    $app_full_name = $data_info['data_name'];
                    $app_name_arr = [];
                    if ($app_full_name) {
                        if ($app_ids){
                            foreach ($app_ids as $app_id){
                                if ($app_full_name == $app_id['app_full_name']) {
                                    $app_name_arr[] = $app_id['app_name'];
                                }
                            }
                        }
                    }
                    $data_list[$data_key]['app_list'] = implode(',',$app_name_arr);
                }
            }

            // 获取两个日期之间的所有月份信息
            $all_month = [];
            $all_month_arr = Service::getAllMonthNum($start_date,$end_date);
            for ($m = 0;$m <= count($all_month_arr); $m++){
                $all_month[$m] = date('Y-m',strtotime("{$start_date} +$m month"));
            }

            // 应用ID为空 默认是所有应用 只分时间展示
            if(!$app_id){
                // 日期 总
                $chartList = [];
                foreach ($all_month as $dtak => $dtav){
                    $chartList[$dtav]['table_list'] = $empty_table_list;
                }
                foreach ($all_month as $dtak => $dtav){
                    foreach($data_list as $chart_data){
                        if ($dtav == $chart_data['date_time']) {
                            unset($chart_data['date_time']);
                            $chartList[$dtav]['table_list'] = array_values($chart_data);
                            break;
                        }
                    }
                }
                if ($total_data_list){
                    $chartList['total']['table_list'] = array_values($total_data_list[0]);
                }else{
                    $chartList['total']['table_list'] = $empty_table_list;
                }

                if ($sum_data_list){
                    $chartList['sum']['table_list'] = array_values($sum_data_list[0]);
                }else{
                    $chartList['sum']['table_list'] = $empty_table_list;
                }



                ApiResponseFactory::apiResponse(['table_list' => $chartList],[]);
            }elseif ($app_type_id == 1 && $app_id){
                // 不分应用
                $chartList = [];
                $chartList_old = [];
                foreach($data_list as $data_k => $chart_data){
                    $chartList_old[] = $chart_data['data_name'];
                }
                $chartList_old = array_values(array_unique($chartList_old));
                $chartList_new = [];
                if ($chartList_old){
                    foreach ($chartList_old as $chartList_old_k => $chartList_old_v){
                        $chartList_new[$chartList_old_k]['data_name'] = $chartList_old_v;
                        $chartList_new[$chartList_old_k]['table_list'] = [];
                    }
                }
                if ($chartList_new){
                    foreach($data_list as $data_k => $chart_data){
                        foreach ($chartList_new as $chartList_new_k => $chartList_new_v){
                            if ($chartList_new_v['data_name'] == $chart_data['data_name']){
                                $chartList_new[$chartList_new_k]['app_list'] = $chart_data['app_list'];
                                unset($chart_data['data_name']);
                                unset($chart_data['app_list']);
                                $chartList_new[$chartList_new_k]['table_list'][] = $chart_data;
                                break;
                            }
                        }
                    }

                    if ($total_data_list){
                        foreach ($chartList_new as $chartList_new_kk => $chartList_info) {
                            $new_table_list = $chartList_info['table_list'];
                            $table_new_list = [];
                            if ($new_table_list) {
                                foreach ($all_month as $dtak => $dtav) {
                                    foreach ($new_table_list as $new_table_v) {
                                        if ($dtav == $new_table_v['date_time']) {
                                            unset($new_table_v['date_time']);
                                            $table_new_list[$dtav]['table_list'] = array_values($new_table_v);
                                            break;
                                        }
                                    }
                                }
                            }
                            foreach ($all_month as $month_single){
                                if (!isset($table_new_list[$month_single])){
                                    $table_new_list[$month_single]['table_list'] = $empty_table_list;
                                }
                            }
                            ksort($table_new_list);
                            foreach ($total_data_list as $total_data_list_k => $total_data_list_v){
                                if ($chartList_info['data_name'] == $total_data_list_v['data_name']){
                                    unset($total_data_list_v['data_name']);
                                    $table_new_list['total']['table_list'] = array_values($total_data_list_v);
                                    break;
                                }
                            }

                            if ($sum_data_list) {
                                foreach ($sum_data_list as $sum_data_list_k => $sum_data_list_v) {
                                    if ($chartList_info['data_name'] == $sum_data_list_v['data_name']) {
                                        unset($sum_data_list_v['data_name']);
                                        $table_new_list['sum']['table_list'] = array_values($sum_data_list_v);
                                        break;
                                    }
                                }
                            }else{
                                $table_new_list['sum']['table_list'] = $empty_table_list;
                            }
                            $chartList_new[$chartList_new_kk]['table_list'] = $table_new_list;
                        }
                    }

                }


                // 分应用大类
                $app_data_names = [];
                $app_data_names_no_data = [];
                foreach ($app_ids as $app_ids_key => $app_id){
                    $app_data_names[] = $app_id['app_full_name'];
                }
                $app_data_names = array_values(array_unique($app_data_names));

                foreach ($app_data_names  as $app_data_name){
                    $app_i = 0;
                    foreach ($chartList_new as $chartList_new_kkk => $chartList_new_vvv){
                        if ($app_data_name == $chartList_new_vvv['data_name']){
                            $app_i++;
                        }
                    }
                    if (!$app_i){
                        $app_data_names_no_data[] = $app_data_name;
                    }
                }
                $chartList_no = [];
                if ($app_data_names_no_data) {
                    foreach ($app_data_names_no_data as $app_data_names_key => $app_data_name){
                        $chartList_no[$app_data_names_key]['data_name'] = $app_data_name;
                        $app_name_arr = [];
                        if ($app_data_name) {
                            if ($app_ids){
                                foreach ($app_ids as $app_id){
                                    if ($app_data_name == $app_id['app_full_name']) {
                                        $app_name_arr[] = $app_id['app_name'];
                                    }
                                }
                            }
                        }
                        $chartList_no[$app_data_names_key]['app_list'] = implode(',',$app_name_arr);
                        foreach ($all_month as $month_single){
                            $chartList_no[$app_data_names_key]['table_list'][$month_single]['table_list'] = $empty_table_list;
                        }
                        if ($total_data_list){
                            foreach ($total_data_list as $total_data_list_k => $total_data_list_v){
                                if ($app_data_name == $total_data_list_v['data_name']){
                                    unset($total_data_list_v['data_name']);
                                    $chartList_no[$app_data_names_key]['table_list']['total']['table_list'] = array_values($total_data_list_v);
                                    break;
                                }
                            }
                        }else {
                            $chartList_no[$app_data_names_key]['table_list']['total']['table_list'] = $empty_table_list;
                        }

                        if ($sum_data_list){
                            foreach ($sum_data_list as $sum_data_list_k => $sum_data_list_v){
                                if ($app_data_name == $sum_data_list_v['data_name']){
                                    unset($sum_data_list_v['data_name']);
                                    $chartList_no[$app_data_names_key]['table_list']['sum']['table_list'] = array_values($sum_data_list_v);
                                    break;
                                }
                            }
                        }else {
                            $chartList_no[$app_data_names_key]['table_list']['sum']['table_list'] = $empty_table_list;
                        }
                    }
                }
                $chartList_new = array_merge($chartList_new,$chartList_no);

                ApiResponseFactory::apiResponse(['table_list' => $chartList_new],[]);
            }else{
                // 分应用
                $chartList = [];
                $chartList_old = [];
                foreach($data_list as $data_k => $chart_data){
                    $chartList_old[] = $chart_data['data_name'];
                }
                $chartList_old = array_values(array_unique($chartList_old));
                $chartList_new = [];
                if ($chartList_old){
                    foreach ($chartList_old as $chartList_old_k => $chartList_old_v){
                        $chartList_new[$chartList_old_k]['data_name'] = $chartList_old_v;
                        $chartList_new[$chartList_old_k]['table_list'] = [];
                    }
                }
                if ($chartList_new){
                    foreach($data_list as $data_k => $chart_data){
                        foreach ($chartList_new as $chartList_new_k => $chartList_new_v){
                            if ($chartList_new_v['data_name'] == $chart_data['data_name']){
                                unset($chart_data['data_name']);
                                $chartList_new[$chartList_new_k]['table_list'][] = $chart_data;
                                break;
                            }
                        }
                    }

                    if ($total_data_list){
                        foreach ($chartList_new as $chartList_new_kk => $chartList_info) {
                            $new_table_list = $chartList_info['table_list'];
                            $table_new_list = [];
                            if ($new_table_list) {
                                foreach ($all_month as $dtak => $dtav) {
                                    foreach ($new_table_list as $new_table_v) {
                                        if ($dtav == $new_table_v['date_time']) {
                                            unset($new_table_v['date_time']);
                                            $table_new_list[$dtav]['table_list'] = array_values($new_table_v);
                                            break;
                                        }
                                    }
                                }
                            }
                            foreach ($all_month as $month_single){
                                if (!isset($table_new_list[$month_single])){
                                    $table_new_list[$month_single]['table_list'] = $empty_table_list;
                                }
                            }
                            ksort($table_new_list);
                            foreach ($total_data_list as $total_data_list_k => $total_data_list_v){
                                if ($chartList_info['data_name'] == $total_data_list_v['data_name']){
                                    unset($total_data_list_v['data_name']);
                                    $table_new_list['total']['table_list'] = array_values($total_data_list_v);
                                    break;
                                }
                            }

                            if ($sum_data_list) {
                                foreach ($sum_data_list as $sum_data_list_k => $sum_data_list_v) {
                                    if ($chartList_info['data_name'] == $sum_data_list_v['data_name']) {
                                        unset($sum_data_list_v['data_name']);
                                        $table_new_list['sum']['table_list'] = array_values($sum_data_list_v);
                                        break;
                                    }
                                }
                            }else{
                                $table_new_list['sum']['table_list'] = $empty_table_list;
                            }

                            $chartList_new[$chartList_new_kk]['app_list'] = [];
                            $chartList_new[$chartList_new_kk]['table_list'] = $table_new_list;
                        }
                    }
                }

                $app_data_names = [];
                $app_data_names_no_data = [];
                // 分应用
                foreach ($app_ids as $app_ids_key => $app_id){
                    $app_data_names[] = $app_id['app_name'];
                }
                $app_data_names = array_values(array_unique($app_data_names));
                foreach ($app_data_names  as $app_data_name){
                    $app_i = 0;
                    foreach ($chartList_new as $chartList_new_kkk => $chartList_new_vvv){
                        if ($app_data_name == $chartList_new_vvv['data_name']){
                            $app_i++;
                        }
                    }
                    if (!$app_i){
                        $app_data_names_no_data[] = $app_data_name;
                    }
                }
                $chartList_no = [];
                if ($app_data_names_no_data) {
                    foreach ($app_data_names_no_data as $app_data_names_key => $app_data_name) {
                        $chartList_no[$app_data_names_key]['data_name'] = $app_data_name;
                        $chartList_no[$app_data_names_key]['app_list'] = '';
                        foreach ($all_month as $month_single) {
                            $chartList_no[$app_data_names_key]['table_list'][$month_single]['table_list'] = $empty_table_list;
                        }
                        if ($total_data_list) {
                            foreach ($total_data_list as $total_data_list_k => $total_data_list_v) {
                                if ($app_data_name == $total_data_list_v['data_name']) {
                                    unset($total_data_list_v['data_name']);
                                    $chartList_no[$app_data_names_key]['table_list']['total']['table_list'] = array_values($total_data_list_v);
                                    break;
                                }
                            }
                        } else {
                            $chartList_no[$app_data_names_key]['table_list']['total']['table_list'] = $empty_table_list;
                        }

                        if ($sum_data_list) {
                            foreach ($sum_data_list as $sum_data_list_k => $sum_data_list_v) {
                                if ($app_data_name == $sum_data_list_v['data_name']) {
                                    unset($sum_data_list_v['data_name']);
                                    $chartList_no[$app_data_names_key]['table_list']['sum']['table_list'] = array_values($sum_data_list_v);
                                    break;
                                }
                            }
                        } else {
                            $chartList_no[$app_data_names_key]['table_list']['sum']['table_list'] = $empty_table_list;
                        }
                    }
                }
                $chartList_new = array_merge($chartList_new,$chartList_no);
                ApiResponseFactory::apiResponse(['table_list' => $chartList_new],[]);
            }

        }else{

            // 获取两个日期之间的所有月份信息
            $all_month = [];
            $all_month_arr = Service::getAllMonthNum($start_date,$end_date);
            for ($m = 0;$m <= count($all_month_arr); $m++){
                $all_month[$m] = date('Y-m',strtotime("{$start_date} +$m month"));
            }

            $chartList = [];
            $app_data_names = [];
            if (!$app_id){
                // 累计
                foreach ($all_month as $month_single){
                    $chartList[$month_single]['table_list'] = $empty_table_list;
                }
                if ($total_data_list){
                    $chartList['total']['table_list'] = array_values($total_data_list[0]);
                }else{
                    $chartList['total']['table_list'] = $empty_table_list;
                }

                if ($sum_data_list){
                    $chartList['sum']['table_list'] = array_values($sum_data_list[0]);
                }else{
                    $chartList['sum']['table_list'] = $empty_table_list;
                }

            }elseif ($app_type_id == 1 && $app_id && $app_ids){
                // 分应用大类
                foreach ($app_ids as $app_ids_key => $app_id){
                    $app_data_names[] = $app_id['app_full_name'];
                }
                $app_data_names = array_values(array_unique($app_data_names));
                foreach ($app_data_names as $app_data_names_key => $app_data_name){
                    $chartList[$app_data_names_key]['data_name'] = $app_data_name;
                    $app_name_arr = [];
                    if ($app_data_name) {
                        if ($app_ids){
                            foreach ($app_ids as $app_id){
                                if ($app_data_name == $app_id['app_full_name']) {
                                    $app_name_arr[] = $app_id['app_name'];
                                }
                            }
                        }
                    }
                    $chartList[$app_data_names_key]['app_list'] = implode(',',$app_name_arr);
                    foreach ($all_month as $month_single){
                        $chartList[$app_data_names_key]['table_list'][$month_single]['table_list'] = $empty_table_list;
                    }
                    if ($total_data_list){
                        foreach ($total_data_list as $total_data_list_k => $total_data_list_v){
                            if ($app_data_name == $total_data_list_v['data_name']){
                                unset($total_data_list_v['data_name']);
                                $chartList[$app_data_names_key]['table_list']['total']['table_list'] = array_values($total_data_list_v);
                                break;
                            }
                        }
                    }else {
                        $chartList[$app_data_names_key]['table_list']['total']['table_list'] = $empty_table_list;
                    }

                    if ($sum_data_list){
                        foreach ($sum_data_list as $sum_data_list_k => $sum_data_list_v){
                            if ($app_data_name == $sum_data_list_v['data_name']){
                                unset($sum_data_list_v['data_name']);
                                $chartList[$app_data_names_key]['table_list']['sum']['table_list'] = array_values($sum_data_list_v);
                                break;
                            }
                        }
                    }else {
                        $chartList[$app_data_names_key]['table_list']['sum']['table_list'] = $empty_table_list;
                    }
                }

            }else{
                // 分应用
                foreach ($app_ids as $app_ids_key => $app_id){
                    $app_data_names[] = $app_id['app_name'];
                }
                $app_data_names = array_values(array_unique($app_data_names));
                foreach ($app_data_names as $app_data_names_key => $app_data_name){
                    $chartList[$app_data_names_key]['data_name'] = $app_data_name;
                    $data_list[$app_data_names_key]['app_list'] = '';
                    foreach ($all_month as $month_single){
                        $chartList[$app_data_names_key]['table_list'][$month_single]['table_list'] = $empty_table_list;
                    }
                    if ($total_data_list){
                        foreach ($total_data_list as $total_data_list_k => $total_data_list_v){
                            if ($app_data_name == $total_data_list_v['data_name']){
                                unset($total_data_list_v['data_name']);
                                $chartList[$app_data_names_key]['table_list']['total']['table_list'] = array_values($total_data_list_v);
                                break;
                            }
                        }
                    }else {
                        $chartList[$app_data_names_key]['table_list']['total']['table_list'] = $empty_table_list;
                    }

                    if ($sum_data_list){
                        foreach ($sum_data_list as $sum_data_list_k => $sum_data_list_v){
                            if ($app_data_name == $sum_data_list_v['data_name']){
                                unset($sum_data_list_v['data_name']);
                                $chartList[$app_data_names_key]['table_list']['sum']['table_list'] = array_values($sum_data_list_v);
                                break;
                            }
                        }
                    }else {
                        $chartList[$app_data_names_key]['table_list']['sum']['table_list'] = $empty_table_list;
                    }
                }

            }


//            $chartList = [];
//            $chartList['total']['table_list'] = [0,0,0,0,0,0,0,0,0];
            ApiResponseFactory::apiResponse(['table_list' => $chartList],[]);
        }

    }

}
