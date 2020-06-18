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

class RedPacketRealizationImp extends ApiBaseImp
{

    /**
     * 红包变现 游戏列表
     * @param  [type] $params [description]
     * @return [type]         [description]
     */
    public static function getRedPacketList($params){
        $userid = $_SESSION['erm_data']['guid'];
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        // 公司
        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;

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
            $where .= " and id in ($app_permission) ";
        }

        $where .= " and id in (select distinct app_id from zplay_red_data_statistics)";

        $sql = " select id,concat(
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
            ) as app_name  from c_app $where  ";


        $data_list = DB::select($sql);
        $data_list = Service::data($data_list);

        $table_list = [];
        if ($data_list){
            foreach ($data_list as $data){
                $table_list[] = [
                    'id' => $data['id'],
                    'value' => $data['app_name'],
                ];
            }
        }else{

            ApiResponseFactory::apiResponse([],[],302);
        }

        ApiResponseFactory::apiResponse(['table_list' => $table_list ],[]);
    }

    /**
     * 红包变现 数据报表
     * @param  [type] $params [description]
     * @return [type]         [description]
     */
    public static function getRedPacketData($params){
        $userid = $_SESSION['erm_data']['guid'];
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        // 应用ID
        $app_id = isset($params['app_id']) ? $params['app_id'] : '';
        // 开始时间
        $start_date = isset($params['start_date']) ? $params['start_date'] : date("Y-m-d",strtotime("-7 day"));
        // 结束时间
        $end_date = isset($params['end_date']) ? $params['end_date'] : date('Y-m-d');
        // 公司
        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        

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
            $where .= ' where app.company_id = 9 ' ;
            $app_where .= ' where app.company_id = 9 ' ;
            $total_where .= ' where app.company_id = 9 ' ;
        }elseif($company != 9 ){
            $where .= ' where app.company_id <> 9 ' ;
            $app_where .= ' where app.company_id <> 9 ' ;
            $total_where .= ' where app.company_id <> 9 ' ;
        }

        // 时间范围
        $app_where .= " and red.date_time between '{$start_date}' and '{$end_date}' ";

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

        $select .= ' red.date_time, ';
        $group_by .= ' group by red.date_time ';
        if ($app_id){
            $where .= " and red.app_id in ($app_id) ";
            $app_where .= " and red.app_id  in ($app_id) ";
            $total_where .= " and red.app_id  in ($app_id) ";
            $group_by .= ' ,red.app_id ';
        }elseif($app_permission) {
            $where .= " and red.app_id  in ($app_permission) ";
            $app_where .= " and red.app_id  in ($app_permission) ";
            $total_where .= " and red.app_id  in ($app_permission) ";
        }

        $order_by = " order by red.date_time desc";

        // 1、新增用户；2、活跃用户；3、付费收入；4、广告收入；5、总收入；6、推广成本；7、毛利润；8、开发者分成；9、总利润
        $select .= " app.app_id, concat(
            	(case when app.release_region_id = 1 then '全球'
            		 when app.release_region_id = 2 then '国外'
            		 when app.release_region_id = 3 then '国内'
            		 when app.release_region_id = 4 then '线下' 
             		 end),'-',
             	(case when app.os_id = 1 then 'IOS'
             		when app.os_id = 2 then '安卓'
             		when app.os_id = 3 then 'h5'
             		when app.os_id = 4 then 'Amazon'
             		end),'-',
             		(case when app.app_name is null then '未知应用' else app.app_name end),'-',
             		(case when app.app_id is null then '未知ID' else app.app_id end)			
            ) as app_name, sum(red.all_card_count) as all_card_count, sum(all_user_count) as all_user_count, sum(all_cat_count) as all_cat_count, sum(all_cat_user_count) as all_cat_user_count, sum(game_total_amount) as game_total_amount, sum(all_9cat_user_count) as all_9cat_user_count, sum(all_today_total) as all_today_total, sum(today_red_bags_user_count) as today_red_bags_user_count, sum(tixian_total) as tixian_total, sum(all_send_money) as all_send_money ";

        $total_select .= " '-' as date_time,'-' as app_id,'-' as app_name, sum(red.all_card_count) as all_card_count, sum(all_user_count) as all_user_count, sum(all_cat_count) as all_cat_count, sum(all_cat_user_count) as all_cat_user_count, sum(game_total_amount) as game_total_amount, sum(all_9cat_user_count) as all_9cat_user_count, sum(all_today_total) as all_today_total, sum(today_red_bags_user_count) as today_red_bags_user_count, sum(tixian_total) as tixian_total, sum(all_send_money) as all_send_money ";

        $table_name = 'zplay_red_data_statistics as red';

        $detail_sql = " select {$select} from $table_name left join c_app app on app.id = red.app_id $where $group_by $order_by ";

        $pageSize = isset($params['size']) ? $params['size'] : 10;
        $p = isset($params['page']) ? $params['page'] : 1;

        $start = ($p-1) * $pageSize;
        $sql = $detail_sql." limit {$start},{$pageSize}";
        $data_list = DB::select($sql);
        $data_list = Service::data($data_list);

        $total_sql = " select {$total_select} from $table_name left join c_app app on app.id = red.app_id $total_where $total_group_by ";
        $total_data_list = DB::select($total_sql);
        $total_data_list = Service::data($total_data_list);

        $countSql = "select count(*) c from ($detail_sql)a";
        $return_data = [];
        $c_answer = DB::select($countSql);
        $c_answer = Service::data($c_answer);
        $count = $c_answer['0']['c'];

        $pageAll = ceil($count/$pageSize);
        $return_data['total'] = $count;
        $return_data['page_total'] = $pageAll;
        $return_data['table_list'] = $data_list;
        $return_data['table_total'] = isset($total_data_list[0]) ? $total_data_list[0] : [];

        ApiResponseFactory::apiResponse(['table_list' => $return_data],[]);

    }

}
