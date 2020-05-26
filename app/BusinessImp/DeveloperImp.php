<?php

namespace App\BusinessImp;

use App\BusinessLogic\CommonLogic;
use App\BusinessLogic\DeveloperLogic;
use App\BusinessLogic\OperationLogLogic;
use App\BusinessLogic\PlatformLogic;
use App\Common\ApiResponseFactory;
use App\Common\Service;
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

        $fields = ['c_developer.*','c_customer_company.company_name as customer_company_name','c_zplay_company.company_name as zplay_company_name','c_currency_type.currency_name','c_currency_type.currency_en','c_business_manager.manager_name'];

        $map['leftjoin'] = [
            ['c_customer_company','c_customer_company.id', 'c_developer.customer_id'],
            ['c_zplay_company','c_zplay_company.id', 'c_developer.company_id'],
            ['c_currency_type','c_currency_type.id', 'c_developer.currency_type_id'],
            ['c_business_manager','c_business_manager.id', 'c_developer.business_manager_id']
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


        $developer_id_prefix = 'zplay'; // 开发者ID前缀

        self::checkCurrencyType($params);
        self::checkBusManagerType($params);

        $update_time = date('Y-m-d H:i:s');
        $create_time = date('Y-m-d H:i:s');
        unset($params['token']);
        unset($params['sessionid']);
        unset($params['customer_name']);
        unset($params['user_company_id']);

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
