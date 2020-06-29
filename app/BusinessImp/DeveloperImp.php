<?php

namespace App\BusinessImp;

use App\BusinessLogic\ApplicationLogic;
use App\BusinessLogic\CommonLogic;
use App\BusinessLogic\DeveloperLogic;
use App\BusinessLogic\OperationLogLogic;
use App\BusinessLogic\PlatformLogic;
use App\BusinessLogic\UserLogic;
use App\Common\ApiResponseFactory;
use App\Common\CurlRequest;
use App\Common\Service;
use App\User;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Overtrue\Pinyin\Pinyin;

class DeveloperImp extends ApiBaseImp
{
    /**
     * 开发者列表
     * @param $params array 请求数据
     */
    public static function getDeveloperList($params)
    {

        $search_name = isset($params['search_name']) ? $params['search_name'] : ''; // 搜索名称
//        $customer_name = isset($params['customer_name']) ? $params['customer_name'] : ''; // 客户名称
        $company_id = isset($params['company_id']) ? $params['company_id'] : ''; // 公司ID
        $page = isset($params['page']) ? $params['page'] : 1 ;
        $page_size = isset($params['size']) ? $params['size'] : 1000 ;

        $map = []; // 查询条件
        if ($search_name) $map['like'][] = ['c_developer.developer_name','like', $search_name];
        if ($search_name) $map['like'][] = ['c_developer.developer_id','like', $search_name];
        if ($search_name) $map['like'][] = ['c_customer_company.company_name','like', $search_name];
        if ($company_id) $map['c_developer.company_id'] = $company_id;

        $fields = ['c_developer.*','c_customer_company.company_name as customer_company_name','c_zplay_company.company_name as zplay_company_name','c_currency_type.currency_name','c_currency_type.currency_en','c_business_manager.manager_name','user.user_account'];

        $map['leftjoin'] = [
            ['c_customer_company','c_customer_company.id', 'c_developer.customer_id'],
            ['c_zplay_company','c_zplay_company.id', 'c_developer.company_id'],
            ['c_currency_type','c_currency_type.id', 'c_developer.currency_type_id'],
            ['c_business_manager','c_business_manager.id', 'c_developer.business_manager_id'],
            ['user','user.developer_id', 'c_developer.id']
        ];
        // 获取分页数据
        $developer_list = DeveloperLogic::getDeveloperList($map, $fields)->forPage($page,$page_size)->orderby("c_developer.id","desc")->get();
        $developer_list = Service::data($developer_list);
        if (!$developer_list) ApiResponseFactory::apiResponse([],[],1000);

        foreach ($developer_list as $key => $value){
            $currency_name = $value['currency_name'];
            unset($value['currency_name']);
            if ($currency_name){
                $value['currency_en'] = $currency_name."({$value['currency_en']})";
            }else{
                $value['currency_en'] = '';
            }
            $developer_list[$key] = $value;
        }



        // 获取数据总数
        $total = DeveloperLogic::getDeveloperList($map)->count();

        $back_data=[
            'table_list'=>$developer_list,
            'total'=> $total,
            'page_total'=> ceil($total / $page_size),
        ];

        ApiResponseFactory::apiResponse($back_data,[]);
    }

    /**
     * 编辑添加开发者
     * @param $params array 请求数据
     */
    public static function createDeveloper($params)
    {

        // 必填参数判断
        $id = isset($params['id']) ? $params['id'] : ''; // 平台自增ID
        $developer_name = isset($params['developer_name']) ? trim($params['developer_name']) : ''; // 开发者名称
        $customer_name = isset($params['customer_name']) ? trim($params['customer_name']): ''; // 客户公司
        $company_id = isset($params['company_id']) ? $params['company_id'] : ''; // 掌游旗下公司ID
        $developer_email = isset($params['developer_email']) ? $params['developer_email'] : ''; // 开发者邮箱

        if (!$developer_name) ApiResponseFactory::apiResponse([],[],530);
        if (!$developer_email) ApiResponseFactory::apiResponse([],[],539);
        if (!$customer_name) ApiResponseFactory::apiResponse([],[],540);
        if (!$company_id) ApiResponseFactory::apiResponse([],[],541);


        $developer_id_prefix = 'Dev'; // 开发者ID前缀

        self::checkCurrencyType($params);
        self::checkBusManagerType($params);

        $update_time = date('Y-m-d H:i:s');
        $create_time = date('Y-m-d H:i:s');
        unset($params['token']);
        unset($params['sessionid']);
        unset($params['customer_name']);
        unset($params['user_company_id']);
        unset($params['user_account']);
        unset($params['language']);

        $pinyin = new Pinyin(); // 默认
        // 判断 编辑? 还是 创建?
        if ($id){ // 编辑

            // 获取当前ID的数据
            $map = [];
            $map['c_developer.id'] = $id;
            $fields = ['c_developer.*','c_customer_company.company_name as customer_name'];

            $map['leftjoin'] = [
                ['c_customer_company','c_customer_company.id', 'c_developer.customer_id']
            ];
            $old_data = DeveloperLogic::getDeveloperList($map,$fields)->first();
            $old_data = Service::data($old_data);


            $developer_id = isset($params['developer_id']) ? $params['developer_id'] : ''; // 开发者ID
            if (!$developer_id) ApiResponseFactory::apiResponse([],[],535);
            // 编辑逻辑待增加
            $params['update_time'] = $update_time;
            unset($params['id']);

            //校验开发者名称不能重复
            $map = [];
            $map['developer_name'] = $developer_name;
//            $map['developer_email'] = $developer_email;
//            $map['customer_id'] = $customer_company_id;
//            $map['company_id'] = $company_id;
//            $map['developer_id'] = $developer_id;
            $map[] = ['id','<>',$id];

            $developer = DeveloperLogic::getDeveloperList($map)->first();
            if ($developer){ // 开发者信息已经重复
                ApiResponseFactory::apiResponse([],[],826);
            }

            // 开启事物 保存数据
            DB::beginTransaction();

            // 判断客户公司信息是否存在,存在取ID；不存在添加新数据，获取ID
            $map = [];
            $map['company_name'] = $customer_name;
            $customer_info = CommonLogic::getCustomerCompanyList($map)->first();
            $customer_info = Service::data($customer_info);
            $customer_company_id = '';
            if ($customer_info){
                $customer_company_id = $customer_info['id'];
            }else{
                $cus_data['company_name'] = $customer_name;
                $customer_alise = $pinyin->sentence($customer_name);
                $cus_data['alise'] = str_replace(' ','',$customer_alise);
                $cus_data['create_time'] = $create_time;
                $cus_data['update_time'] = $update_time;
                $customer_company_id = PlatformLogic::createCustomerCompany($cus_data);
                if (!$customer_company_id){
                    DB::rollBack();
                    ApiResponseFactory::apiResponse([],[],527);
                }
            }

            $params['customer_id'] = $customer_company_id;
            unset($params['developer_id']);
            unset($params['developer_name']);
            $bool = DeveloperLogic::updateDeveloper($id, $params);
            if (!$bool){
                DB::rollBack();
                ApiResponseFactory::apiResponse([],[],502);
            }
            DB::commit();

            // 保存日志
            $params['customer_name'] = $customer_name;
            OperationLogImp::saveOperationLog(2,5, $params, $old_data);

            ApiResponseFactory::apiResponse($id,[]);

        }else{ // 创建

            //校验开发者名称不能重复
            $map = [];
            $map['developer_name'] = $developer_name;
//            $map['developer_email'] = $developer_email;
//            $map['customer_id'] = $customer_company_id;
//            $map['company_id'] = $company_id;
//            $map['developer_id'] = $developer_id;

            $developer = DeveloperLogic::getDeveloperList($map)->first();
            $developer = Service::data($developer);
            if ($developer){ // 开发者信息已经重复
                ApiResponseFactory::apiResponse([],[],826);
            }

            // 生成开发者ID 规则需要确认
            $developer = DeveloperLogic::getDeveloperList()->orderby("id","desc")->first();
            $developer = Service::data($developer);
            if ($developer){
                $developer_id = $developer['id'] + 1;
                if ($developer_id < 10){
                    $developer_id = $developer_id_prefix . '00' . strval($developer_id);
//                        $developer_id = $developer_id_prefix . strval($developer_id + 200);
                }else if($developer_id < 100){
                    $developer_id = $developer_id_prefix . '0' . strval($developer_id);
//                        $developer_id = $developer_id_prefix . strval($developer_id + 200);
                }else{
                    $developer_id = $developer_id_prefix . strval($developer_id);
//                        $developer_id = $developer_id_prefix . strval($developer_id + 200);
                }
            }else{
                $developer_id = $developer_id_prefix . '001';
            }

            if (!$developer_id) ApiResponseFactory::apiResponse([],[],531);

            $params['developer_id'] = $developer_id;
            $params['create_time'] = $create_time;
            $params['update_time'] = $update_time;

            // 开启事物 保存数据
            DB::beginTransaction();

            // 判断客户公司信息是否存在,存在取ID；不存在添加新数据，获取ID
            $map = [];
            $map['company_name'] = $customer_name;
            $cus_data['create_time'] = $create_time;
            $cus_data['update_time'] = $update_time;
            $customer_info = CommonLogic::getCustomerCompanyList($map)->first();
            $customer_info = Service::data($customer_info);
            if ($customer_info){
                $customer_company_id = $customer_info['id'];
            }else{
                $cus_data['company_name'] = $customer_name;
                $customer_alise = $pinyin->sentence($customer_name);
                $cus_data['alise'] = str_replace(' ','',$customer_alise);
                $customer_company_id = PlatformLogic::createCustomerCompany($cus_data);
                if (!$customer_company_id){
                    DB::rollBack();
                    ApiResponseFactory::apiResponse([],[],527);
                }
            }

            $developer_name_alise = $pinyin->sentence($developer_name);
            $params['alise'] = str_replace(' ','',$developer_name_alise);
            $params['customer_id'] = $customer_company_id;
            // 保存开发者数据
            $new_id = DeveloperLogic::addDeveloper($params);
            if (!$new_id){
                DB::rollBack();
                ApiResponseFactory::apiResponse([],[],533);
            }

            DB::commit();

            // 保存日志
            OperationLogImp::saveOperationLog(1,5,$new_id);

            ApiResponseFactory::apiResponse($new_id,[]);

        }

    }

    /**
     * 开发者平台应用控制列表接口
     * @param $params array 请求数据
     */
    public static function developAppList($params){
        $userid = $_SESSION['erm_data']['guid'];
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        $search = isset($params['search']) ? $params['search'] : ''; // 搜索
        // 公司
        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;

        if ($company){
            $company_info = DB::select(" select * from c_zplay_company where id = {$company}");
            $company_info = Service::data($company_info);
            if (!$company_info){
                ApiResponseFactory::apiResponse([],[],1023);
            }
        }

        $search = addslashes($search);
        // 拼接查询条件
        $where = '';

        if($company == 9){
            $where .= ' where app.company_id = 9 ' ;
        }elseif($company != 9 ){
            $where .= ' where app.company_id <> 9 ' ;
        }

        if ($search){
            $where .= " and (app.app_full_name like '%$search%' or dev.developer_name like '%$search%' ) ";
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
            $where .= " and app.id in ($app_permission) ";
        }

        $dev_sql = " select 
            app.id as app_key,
            app.app_id,
            concat(
            	(case when app.release_region_id = 1 then '全球'
            		 when app.release_region_id = 2 then '国外'
            		 when app.release_region_id = 3 then '国内'
            		 when app.release_region_id = 4 then '线下' 
             		 end),'-',
             	(case when app.os_id = 1 then 'ios'
             		when app.os_id = 2 then 'Android'
             		when app.os_id = 3 then 'h5'
             		when app.os_id = 4 then 'Amazon'
             		end),'-',
             		(case when app.app_name is null then '未知应用' else app.app_name end),'-',
             		(case when app.app_id is null then '未知ID' else app.app_id end)			
            ) as app_name,
            app.app_full_name,
            app.`developer_id`,
            dev.`developer_name`,
            app.is_dev_show
          from c_app app
          left join c_developer dev on dev.id = app.`developer_id` {$where}";

        $dev_app_list = DB::select($dev_sql);
        $dev_app_list = Service::data($dev_app_list);

        $dev_category_sql = "  select 
            app.app_full_name,
            dev.`developer_name`
          from c_app app
          left join c_developer dev on dev.id = app.`developer_id` {$where} group by app.app_full_name,dev.`developer_name` order by is_dev_show desc,app.id desc ";

        $countSql = "select count(*) c from ($dev_category_sql)a";

        $pageSize = isset($params['size']) ? $params['size'] : 10;
        $p = isset($params['page']) ? $params['page'] : 1;

        $start = ($p-1) * $pageSize;
        $dev_category_sql = $dev_category_sql." limit {$start},{$pageSize}";

        $dev_cate_app_list = DB::select($dev_category_sql);
        $dev_cate_app_list = Service::data($dev_cate_app_list);

        if ($dev_app_list && $dev_cate_app_list){

            foreach ($dev_app_list as $dev_app_list_kk => $dev_app_list_vv){
                foreach ($dev_cate_app_list as $dev_cate_app_key => $dev_cate_app_info){
                    if (($dev_cate_app_info['app_full_name'] == $dev_app_list_vv['app_full_name'] && $dev_cate_app_info['developer_name'] == $dev_app_list_vv['developer_name'])){
                        if ($dev_app_list_vv['is_dev_show'] == 1){
                            $dev_cate_app_list[$dev_cate_app_key]['undisplay'][] = [
                                'app_id' => $dev_app_list_vv['app_key'],
                                'app_name' => $dev_app_list_vv['app_name'],
                            ];
                            break;
                        }elseif($dev_app_list_vv['is_dev_show'] == 2){
                            $dev_cate_app_list[$dev_cate_app_key]['display'][] = [
                                'app_id' => $dev_app_list_vv['app_key'],
                                'app_name' => $dev_app_list_vv['app_name'],
                            ];
                            break;
                        }

                    }
                }
            }
        }

        $return_data = [];
        $c_answer = DB::select($countSql);
        $c_answer = Service::data($c_answer);
        $count = $c_answer['0']['c'];

        $pageAll = ceil($count/$pageSize);
        $return_data['total'] = $count;
        $return_data['page_total'] = $pageAll;
        $return_data['table_list'] = $dev_cate_app_list;

        ApiResponseFactory::apiResponse($return_data,[]);
    }

    /**
     * 开发者平台应用控制状态修改接口
     * @param $params array 请求数据
     */
    public static function developAppStatus($params){
        $userid = $_SESSION['erm_data']['guid'];
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        $app_key = isset($params['app_id']) ? $params['app_id'] : ''; // 应用主键ID
        $is_dev_show = isset($params['is_dev_show']) ? $params['is_dev_show'] : ''; // 是否展示

        if (!$app_key){
            ApiResponseFactory::apiResponse([],[],1030);
        }
        if (!$is_dev_show || !in_array($is_dev_show,[1,2])){
            ApiResponseFactory::apiResponse([],[],1031);
        }

        // 查询此应用是否存在
        $map = [];
        $map['id'] = $app_key;
        $app_info = ApplicationLogic::getApplicationList($map)->get();
        $app_info = Service::data($app_info);
        if (!$app_info){
            ApiResponseFactory::apiResponse([],[],1032);
        }
        // 更新展示状态
        $update_data['is_dev_show'] = $is_dev_show;
        $result = ApplicationLogic::updateApplication($app_key,$update_data);
        if (!$result){
            ApiResponseFactory::apiResponse(['data'=>['status' => false]],[]);
        }
        ApiResponseFactory::apiResponse([],[]);

    }

    /**
     * 开发者平台用户注册接口
     * @param $params array 请求数据
     */
    public static function developUserRegister($params){
        $userid = $_SESSION['erm_data']['guid'];
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        $user_account = isset($params['user_account']) ? $params['user_account'] : ''; // 开发者登陆邮箱
        $developer_id = isset($params['developer_id']) ? $params['developer_id'] : ''; // 数据平台开发者表主键ID
        $developer_name = isset($params['developer_name']) ? $params['developer_name'] : ''; // 数据平台开发者名称
        $edit_type = isset($params['edit_type']) ? $params['edit_type'] : 1 ; // 1,创建；2,更新

        if (!$user_account){
            ApiResponseFactory::apiResponse([],[],1034);
        }
        if (!$developer_id){
            ApiResponseFactory::apiResponse([],[],1035);
        }

        // 查询有无此开发者
        $developer_info = DB::select(" select * from c_developer where id = {$developer_id}");
        $developer_info = Service::data($developer_info);
        if (!$developer_info){
            ApiResponseFactory::apiResponse([],[],1036);
        }

        if ($edit_type == 1) {
            // 查询有无此开发者登陆信息
            $user_info = DB::select(" select * from `user` where (user_account = '{$user_account}' or developer_id = {$developer_id} ) and account_type = 2 ");
            $user_info = Service::data($user_info);
            if ($user_info) {
                ApiResponseFactory::apiResponse([], [], 1037);
            }
        }else{
            // 查询有无此开发者登陆信息
            $user_info = DB::select(" select * from `user` where developer_id = {$developer_id} and account_type = 2 ");
            $user_info = Service::data($user_info);
            if (!$user_info) {
                ApiResponseFactory::apiResponse([], [], 1042);
            }
        }
        // 注册开发者用户
        // 第一步：外网通行证注册
        $pwd = "Zplay123888";
        $reg_url = "https://passport.zplay.cn/uCenter/api/regist/{$user_account}/{$pwd}";
        $reg_result = CurlRequest::get_response($reg_url);
        if ($reg_result == -2){
            ApiResponseFactory::apiResponse([],[],1039);
        }elseif ($reg_result == -1){
            ApiResponseFactory::apiResponse([],[],1040);
        }elseif ($reg_result == 0){
            ApiResponseFactory::apiResponse([],[],1041);
        }elseif ($reg_result == 1){
            if ($edit_type == 1) {
                // 第二步：数据平台用户表增加用户
                $insert_data['developer_id'] = $developer_id;
                $insert_data['user_account'] = $user_account;
                $insert_data['name'] = $developer_name;
                $insert_data['account_type'] = 2;

                $dev_user_id = UserLogic::userAdd($insert_data);
                if (!$dev_user_id) {
                    ApiResponseFactory::apiResponse([], [], 1038);
                }
            }else{
                // 第二步：数据平台用户表修改用户
                $map = [];
                $map['developer_id'] = $developer_id;
                $map['account_type'] = 2;
                $update_data['user_account'] = $user_account;

                $dev_user_id = UserLogic::userDevEdit($map,$update_data);
                if (!$dev_user_id) {
                    ApiResponseFactory::apiResponse([], [], 1043);
                }
            }
        }

        ApiResponseFactory::apiResponse([],[]);
    }

    // 货币类型验证
    private static function checkCurrencyType($params){
        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : ''; // 货币类型ID
        if (!$currency_type_id) ApiResponseFactory::apiResponse([],[],509);
        return $currency_type_id;
    }

    // 负责人验证
    private static function checkBusManagerType($params)
    {
        $business_manager_id = isset($params['business_manager_id']) ? $params['business_manager_id'] : ''; // 平台负责人ID
        if (!$business_manager_id) ApiResponseFactory::apiResponse([], [], 510);
        return $business_manager_id;
    }
}
