<?php

namespace App\BusinessImp;

use App\BusinessLogic\CommonLogic;
use App\BusinessLogic\OperationLogLogic;
use App\Common\ApiResponseFactory;
use App\Common\Service;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\UserLogic;

class CommonImp extends ApiBaseImp
{
    /**
     * 货币接口
     * @param $params array 请求数据
     */
    public static function getCurrencyList($params)
    {

        $search = isset($params['search']) ? $params['search'] : ''; // 搜索条件
        $map = []; // 查询条件
        if ($search){
            $map['like'] = ['currency_name','like', $search];
        }
//        $map['comm_status'] = 1;
        $fields = ["id", "currency_name as value","currency_en"]; // 查询字段
        $currency_list = CommonLogic::getCurrencyList($map, $fields)->orderBy('comm_status','desc')->orderBy('alise','asc')->get();
        if (!$currency_list) ApiResponseFactory::apiResponse([],[],1000);
        $currency_list = Service::data($currency_list);
        foreach ($currency_list as $key => $value){
            $currency_en = $value['currency_en'];
            unset($value['currency_en']);
            $value['value'] = $value['value']."($currency_en)";
            $currency_list[$key] = $value;

        }
        ApiResponseFactory::apiResponse(['table_list' => $currency_list],[]);
    }

    /**
     * 发行公司(掌游旗下公司列表)
     * @param $params array 请求数据
     */
    public static function getZplayCompanyList($params)
    {
        $search = isset($params['search']) ? $params['search'] : ''; // 搜索条件
        $map = []; // 查询条件
        if ($search){
            $map['like'] = ['company_name','like', $search];
        }
        $fields = ["id", "company_name as value"]; // 查询字段
        $company_list = CommonLogic::getCompanyList($map, $fields)->get()->toArray();
        if (!$company_list) ApiResponseFactory::apiResponse([],[],1000);
        ApiResponseFactory::apiResponse(['table_list' => $company_list],[]);
    }

    /**
     * 应用大类
     * @param $params array 请求数据
     */
    public static function getAppCategoryList($params)
    {
        $search = isset($params['search']) ? $params['search'] : ''; // 搜索条件
        $map = []; // 查询条件
        if ($search){
            $map['like'] = ['app_category_name','like', $search];
        }
        $fields = ["id", "app_category_name as value"]; // 查询字段
        $app_category_list = CommonLogic::getAppCategoryList($map, $fields)->get()->toArray();
        if (!$app_category_list) ApiResponseFactory::apiResponse([],[],1000);
        ApiResponseFactory::apiResponse(['table_list' => $app_category_list],[]);
    }

    /**
     * 广告形式接口
     * @param $params array 请求数据
     */
    public static function getAdTypeList($params)
    {
        $search = isset($params['search']) ? $params['search'] : ''; // 搜索条件
        $map = []; // 查询条件
        if ($search){
            $map['like'] = ['ad_type','like', $search];
        }
        $fields = ["id", "ad_type as value"]; // 查询字段
        $ad_type_list = CommonLogic::getAdTypeList($map, $fields)->get()->toArray();
        if (!$ad_type_list) ApiResponseFactory::apiResponse([],[],1000);
        ApiResponseFactory::apiResponse(['table_list' => $ad_type_list],[]);
    }

    /**
     * 负责人
     * @param $params array 请求数据
     */
    public static function getBusinessManagerList($params)
    {
        $search = isset($params['search']) ? $params['search'] : ''; // 搜索条件
        $map = []; // 查询条件
        if ($search){
            $map['like'] = ['manager_name','like', $search];
        }
        $fields = ["id", "manager_name as value", "manager_type as type"]; // 查询字段
        $manager_list = CommonLogic::getBusinessManagerList($map, $fields)->get()->toArray();
        if (!$manager_list) ApiResponseFactory::apiResponse([],[],1000);
        ApiResponseFactory::apiResponse(['table_list' => $manager_list],[]);
    }

    /**
     * 平台类型
     * @param $params array 请求数据
     */
    public static function getPlatformTypeList($params)
    {
        $search = isset($params['search']) ? $params['search'] : ''; // 搜索条件
        $map = []; // 查询条件
        if ($search){
            $map['like'] = ['platform_type','like', $search];
        }
        $fields = ["id", "platform_type as value"]; // 查询字段
        $platform_type_list = CommonLogic::getPlatformTypeList($map, $fields)->get()->toArray();
        if (!$platform_type_list) ApiResponseFactory::apiResponse([],[],1000);
        ApiResponseFactory::apiResponse(['table_list' => $platform_type_list],[]);
    }

    /**
     * 客户公司
     * @param $params array 请求数据
     */
    public static function getCustomerCompanyList($params)
    {
        $search = isset($params['search']) ? $params['search'] : ''; // 搜索条件
        $map = []; // 查询条件
        if ($search){
            $map['like'] = ['company_name','like', $search];
        }
        $fields = ["id", "company_name as value"]; // 查询字段
        $customer_company_list = CommonLogic::getCustomerCompanyList($map, $fields)->get()->toArray();
        if (!$customer_company_list) ApiResponseFactory::apiResponse([],[],1000);
        ApiResponseFactory::apiResponse(['table_list' => $customer_company_list],[]);
    }

    /**
     * 计费平台
     * @param $params array 请求数据
     */
    public static function getPaymentMethodList($params)
    {
        $search = isset($params['search']) ? $params['search'] : ''; // 搜索条件
        $map = []; // 查询条件
        if ($search){
            $map['like'] = ['platform_name','like', $search];
        }
        $map['platform_type_id'] = 3;
        $map['status'] = 1;
        $fields = ["platform_id as id", "platform_name as value"]; // 查询字段
        $pay_method_list = CommonLogic::getPayPlatformList($map, $fields)->groupBy('platform_id','platform_name')->get()->toArray();
        if (!$pay_method_list) ApiResponseFactory::apiResponse([],[],1000);
        ApiResponseFactory::apiResponse(['table_list' => $pay_method_list],[]);
    }

    /**
     * 广告平台
     * @param $params array 请求数据
     */
    public static function getAdMethodList($params)
    {
        $search = isset($params['search']) ? $params['search'] : ''; // 搜索条件
        $map = []; // 查询条件
        if ($search){
            $map['like'] = ['platform_name','like', $search];
        }
        $map['platform_type_id'] = 2;
        $map['status'] = 1;
        $fields = ["platform_id as id", "platform_name as value"]; // 查询字段
        $pay_method_list = CommonLogic::getPayPlatformList($map, $fields)->groupBy('platform_id','platform_name')->get()->toArray();
        if (!$pay_method_list) ApiResponseFactory::apiResponse([],[],1000);
        ApiResponseFactory::apiResponse(['table_list' => $pay_method_list],[]);
    }

    /**
     * 计费平台
     * @param $params array 请求数据
     */
    public static function getTjMethodList($params)
    {
        $search = isset($params['search']) ? $params['search'] : ''; // 搜索条件
        $map = []; // 查询条件
        if ($search){
            $map['like'] = ['platform_name','like', $search];
        }
        $map['platform_type_id'] = 1;
        $map['status'] = 1;
        $fields = ["platform_id as id", "platform_name as value"]; // 查询字段
        $pay_method_list = CommonLogic::getPayPlatformList($map, $fields)->groupBy('platform_id','platform_name')->get()->toArray();
        if (!$pay_method_list) ApiResponseFactory::apiResponse([],[],1000);
        ApiResponseFactory::apiResponse(['table_list' => $pay_method_list],[]);
    }

    /**
     * 计费平台
     * @param $params array 请求数据
     */
    public static function getTgMethodList($params)
    {
        $search = isset($params['search']) ? $params['search'] : ''; // 搜索条件
        $map = []; // 查询条件
        if ($search){
            $map['like'] = ['platform_name','like', $search];
        }
        $map['platform_type_id'] = 4;
        $map['status'] = 1;
        $fields = ["platform_id as id", "platform_name as value"]; // 查询字段
        $pay_method_list = CommonLogic::getPayPlatformList($map, $fields)->groupBy('platform_id','platform_name')->get()->toArray();
        if (!$pay_method_list) ApiResponseFactory::apiResponse([],[],1000);
        ApiResponseFactory::apiResponse(['table_list' => $pay_method_list],[]);
    }

    /**
     * 计费平台
     * @param $params array 请求数据
     */
    public static function getDlMethodList($params)
    {
        $search = isset($params['search']) ? $params['search'] : ''; // 搜索条件
        $map = []; // 查询条件
        if ($search){
            $map['like'] = ['platform_name','like', $search];
        }
        $map['platform_type_id'] = 5;
        $map['status'] = 1;
        $fields = ["platform_id as id", "platform_name as value"]; // 查询字段
        $pay_method_list = CommonLogic::getPayPlatformList($map, $fields)->groupBy('platform_id','platform_name')->get()->toArray();
        if (!$pay_method_list) ApiResponseFactory::apiResponse([],[],1000);
        ApiResponseFactory::apiResponse(['table_list' => $pay_method_list],[]);
    }

     /**
     * 部门
     * @param $params array 请求数据
     */
    public static function getDepartmentList($params)
    {
         $search = isset($params['search']) ? $params['search'] : ''; // 搜索条件
        $map = []; // 查询条件
        if ($search){
            $map['like'] = ['department_name','like', $search];
        }
        $fields = ["id", "department_name as value"]; // 查询字段
        $app_category_list = CommonLogic::getDepartmentList($map, $fields)->get()->toArray();
        if (!$app_category_list) ApiResponseFactory::apiResponse([],[],1000);
        ApiResponseFactory::apiResponse(['table_list' => $app_category_list],[]);

    }
         /**
     * 功能权限
     * @param $params array 请求数据
     */
    public static function getfunctionPermissionList($params)
    {

        $menu_list = OperationLogLogic::navMenuInfo()->get();
        $menu_list = Service::data($menu_list);
        $info = Service::buildMenuTree($menu_list,[],0,-2);
        if (!$info) ApiResponseFactory::apiResponse([],[],1000);
        ApiResponseFactory::apiResponse($info,[]);
    }

             /**
     * 功能权限
     * @param $params array 请求数据
     */
    public static function getMenuList($params)
    {
//        if(empty($_SESSION['erm_data'])) ApiResponseFactory::apiResponse([],[],742);
         $map['user.user_account'] = $_SESSION['erm_data']['email'];
         $map['user.id'] = $_SESSION['erm_data']['guid'];
        $map['user.status'] = 1;
        $fields = ['user.company_id','user.function_permission','user.id','user.name','user.user_account','user.role_id','role.department_id','user.type'];
        $map['leftjoin'] = [
            ['role','user.role_id', 'role.id']
        ];
        //验证用户是否有权限登录
        $userInfo = UserLogic::Userlist($map,$fields)->get();
        $userInfo =Service::data($userInfo);
       // if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);

        //返回用户下用权限列表
        if($userInfo[0]['function_permission'] == -2){
            $function_permission=-2;
        }else{
            $function_permission=explode(',', $userInfo[0]['function_permission']);
        }
        $menu_list = OperationLogLogic::navMenuInfo()->orderBy('type')->orderBy('order_by')->get();
        $menu_list = Service::data($menu_list);
        $info = Service::buildMenuTree($menu_list,[],0,$function_permission);
        $sql ="select id,company_full_name_en from c_zplay_company where id in (".$userInfo[0]['company_id'].")";
        $user_company=db::select($sql);
        $user_company = Service::data($user_company);
        // 获取树形菜单
        $back_data=[
            'menu'=>$info,
            'guid'=> $userInfo[0]['id'],
            'email'=> $userInfo[0]['user_account'],
            'name'=> $userInfo[0]['name'],
            'type'=> $userInfo[0]['type'],
            'user_company'=>$user_company,
            'department_id'=> $userInfo[0]['department_id'],
            'role_id'=> $userInfo[0]['role_id'],
        ];
         ApiResponseFactory::apiResponse($back_data,[]);

    }


    /**
     * 用户权限下应用列表
     */
    /**
     * 用户权限下应用列表
     */
    public static function getUserAppList($params)
    {
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
        //返回用户下用权限列表
        $app_permission = [];
        if($userInfo[0]['app_permission'] != -2){
            $app_permission = explode(',', $userInfo[0]['app_permission']);
        }

        $gameData = [];
        //当能查询到游戏权限时
        if($app_permission){
            $gameList = DB::table('c_app')->select('id','app_id','app_name','release_region_id','os_id')->where('status',1)->whereIn("id",$app_permission)->orderBy('app_full_name')->get();

        }else{
            $gameList = DB::table('c_app')->select('id','app_id','app_name','release_region_id','os_id')->where('status',1)->orderBy('app_full_name')->get();
        }
        $gameList = Service::data($gameList);
        if ($gameList){
            foreach ($gameList as $all_app_info){
                // 发行区域ID(1,全球;2,国外;3,国内;)
                if ($all_app_info['release_region_id'] == 1){
                    $release_region_id = '全球-';
                }elseif ($all_app_info['release_region_id'] == 2){
                    $release_region_id = '国外-';
                }elseif ($all_app_info['release_region_id'] == 3){
                    $release_region_id = '国内-';
                }else{
                    $release_region_id = '未知区域-';
                }

                // 操作系统(1,ios;2,Android;3,h5;4,Amazon;)
                if ($all_app_info['os_id'] == 1){
                    $os_id = 'ios-';
                }elseif ($all_app_info['os_id'] == 2){
                    $os_id = 'Android-';
                }elseif ($all_app_info['os_id'] == 3){
                    $os_id = 'h5-';
                }elseif ($all_app_info['os_id'] == 4){
                    $os_id = 'Amazon-';
                }else{
                    $os_id = '未知系统-';
                }

                $gameData[] = [
                    'id' => $all_app_info['id'],
                    'value' => $release_region_id.$os_id.$all_app_info['app_name'].'-'.$all_app_info['app_id']
                ];
            }
        }

        ApiResponseFactory::apiResponse($gameData,[]);

    }

    /**
     * 用户权限下应用列表 分页面
     */
    public static function getUserAppNewList($params)
    {
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
        //返回用户下用权限列表
        $app_permission = [];
        if($userInfo[0]['app_permission'] != -2){
//            $app_permission = explode(',', $userInfo[0]['app_permission']);
            $app_permission = $userInfo[0]['app_permission'];
        }

        // 公司ID
        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        $where_sql = '';
        if ($company == 9){
            $where_sql = " and company_id = 9 ";
        }else{
            $where_sql = " and company_id != 9 ";
        }
        // 页面类型 1 推广 2 变现 3 发行
        $type =  isset($params['type']) ? $params['type'] : 1;
        $show_field = '';
        if ($type == 1){
            $show_field = 'promotion_list_on';
        }elseif($type == 2){
            $show_field = 'realization_on';
        }elseif($type == 3){
            $show_field = 'publish_on';
        }


        $gameData = [];
        //当能查询到游戏权限时
        if($app_permission){
//            $gameList = DB::table('c_app')->select('id','app_id','app_name','release_region_id','os_id')->where('status',1)->whereIn("id",$app_permission)->orderBy('app_full_name')->get();

            $game_list_sql = " select id,app_id,app_name,release_region_id,os_id 
                              from c_app 
                              where status = 1 
                              and id in ($app_permission) 
                              {$where_sql}
                              and id not in (select app_key from c_app_show where {$show_field} = 0) 
                              order by CONVERT(app_name USING gbk) ";
            $gameList = DB::select($game_list_sql);

        }else{
//            $gameList = DB::table('c_app')->select('id','app_id','app_name','release_region_id','os_id')->where('status',1)->orderBy('app_full_name')->get();
            $game_list_sql = " select id,app_id,app_name,release_region_id,os_id 
                              from c_app 
                              where status = 1  
                              {$where_sql}
                              and id not in (select app_key from c_app_show where {$show_field} = 0) 
                              order by CONVERT(app_name USING gbk) ";
            $gameList = DB::select($game_list_sql);
        }

        $gameList = Service::data($gameList);
        if ($gameList){
            foreach ($gameList as $all_app_info){
                // 发行区域ID(1,全球;2,国外;3,国内;)
                if ($all_app_info['release_region_id'] == 1){
                    $release_region_id = '全球-';
                }elseif ($all_app_info['release_region_id'] == 2){
                    $release_region_id = '国外-';
                }elseif ($all_app_info['release_region_id'] == 3){
                    $release_region_id = '国内-';
                }else{
                    $release_region_id = '未知区域-';
                }

                // 操作系统(1,ios;2,Android;3,h5;4,Amazon;)
                if ($all_app_info['os_id'] == 1){
                    $os_id = 'ios-';
                }elseif ($all_app_info['os_id'] == 2){
                    $os_id = 'Android-';
                }elseif ($all_app_info['os_id'] == 3){
                    $os_id = 'h5-';
                }elseif ($all_app_info['os_id'] == 4){
                    $os_id = 'Amazon-';
                }else{
                    $os_id = '未知系统-';
                }

                $gameData[] = [
                    'id' => $all_app_info['id'],
                    'value' => $release_region_id.$os_id.$all_app_info['app_name'].'-'.$all_app_info['app_id']
                ];
            }
        }

        ApiResponseFactory::apiResponse($gameData,[]);

    }


    /**
     * 国家
     * @param $params array 请求数据
     */
    public static function getCountryList($params)
    {
        $search = isset($params['search']) ? $params['search'] : ''; // 搜索条件
        $map = []; // 查询条件
        if ($search){
            $map['like'] = ['china_name','like', $search];
        }
        $fields = ["id as id", "china_name as value"]; // 查询字段
        $map['type'] =2; 
        $method_list = CommonLogic::getTgCountryList($map, $fields)->orderby('sort','desc')->get()->toArray();
        if (!$method_list) ApiResponseFactory::apiResponse([],[],1000);
        ApiResponseFactory::apiResponse(['table_list' => $method_list],[]);
    }

    /**
     * 计费平台
     * @param $params array 请求数据
     */
    public static function getAllPlatform($params)
    {
        $search = isset($params['search']) ? $params['search'] : ''; // 搜索条件
        $platform_type_id = isset($params['platform_type_id']) ? $params['platform_type_id'] : ''; // 搜索条件
        $map = []; // 查询条件
        if ($search){
            $map['like'] = ['platform_name','like', $search];
        }
        if ($platform_type_id){
            $map['platform_type_id'] = $platform_type_id;
        }
        $map['status'] = 1;
        $fields = ["platform_id as id", "platform_name as value",'platform_type_id']; // 查询字段
        $platform_list = CommonLogic::getPayPlatformList($map, $fields)->groupBy('platform_id','platform_name','platform_type_id')->get();
        $platform_list = Service::data($platform_list);
        if (!$platform_list) ApiResponseFactory::apiResponse([],[],1000);

        if ($platform_list){
            foreach ($platform_list as $key => $platform){
                if ($platform['platform_type_id'] == 1){
                    $platform["value"] = '统计'.'-'.$platform['value'].'-'.$platform['id'];
                }elseif ($platform['platform_type_id'] == 2){
                    $platform["value"] = '广告'.'-'.$platform['value'].'-'.$platform['id'];
                }elseif ($platform['platform_type_id'] == 3){
                    $platform["value"] = '计费'.'-'.$platform['value'].'-'.$platform['id'];
                }elseif ($platform['platform_type_id'] == 4){
                    $platform["value"] = '推广'.'-'.$platform['value'].'-'.$platform['id'];
                }elseif ($platform['platform_type_id'] == 5){
                    $platform["value"] = '代理'.'-'.$platform['value'].'-'.$platform['id'];
                }
                unset($platform['platform_type_id']);
                $platform_list[$key] = $platform;

            }
        }
        ApiResponseFactory::apiResponse(['table_list' => $platform_list],[]);
    }


    /**
     * 开发者信息列表
     * @param $params array 请求数据
     */
    public static function getAllDeveloper($params)
    {
        $search = isset($params['search']) ? $params['search'] : ''; // 搜索条件
        $map = []; // 查询条件
        if ($search){
            $map['like'] = ['developer_name','like', $search];
        }
        session_write_close();
        $fields = ["id", "developer_id","developer_name"]; // 查询字段
        $developer_list = CommonLogic::getAllDeveloperList($map, $fields)->orderby('developer_name')->get();
        $developer_list = Service::data($developer_list);
        if (!$developer_list) ApiResponseFactory::apiResponse([],[],1000);

        $return_list = [];
        foreach ($developer_list as $dev_key => $developer){
            $return_list[$dev_key]['id'] = $developer['id'];
            $return_list[$dev_key]['value'] = $developer['developer_name']."-".$developer['developer_id'];
        }
        ApiResponseFactory::apiResponse(['table_list' => $return_list],[]);
    }

}
