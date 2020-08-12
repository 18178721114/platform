<?php

namespace App\BusinessImp;

use App\BusinessLogic\ChannelLogic;
use App\BusinessLogic\CommonLogic;
use App\BusinessLogic\PlatformLogic;
use App\Common\ApiResponseFactory;
use App\Common\Service;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Overtrue\Pinyin\Pinyin;

class PlatformImp extends ApiBaseImp
{
    /**
     * 平台列表
     * @param $params array 请求数据
     */
    public static function getPlatformList($params)
    {

        $search_name = isset($params['search_name']) ? $params['search_name'] : ''; // 搜索名称
//        $customer_name = isset($params['customer_name']) ? $params['customer_name'] : ''; // 客户名称
        $company_id = isset($params['company_id']) ? $params['company_id'] : ''; // 公司ID
        $platform_type = isset($params['platform_type']) ? $params['platform_type'] : ''; // 平台类型
        $page = isset($params['page']) ? $params['page'] : 1 ;
        $page_size = isset($params['size']) ? $params['size'] : 1000 ;

        $map = []; // 查询条件
        if ($search_name) $map['like'][] = ['c_platform.platform_name','like', $search_name];
        if ($search_name) $map['like'][] = ['c_platform.platform_id','like', $search_name];
        if ($search_name) $map['like'][] = ['c_customer_company.company_name','like', $search_name];
        if ($platform_type) $map['c_platform.platform_type_id'] = $platform_type;
        if ($company_id) $map['c_platform.company_id'] = $company_id;
        $map['c_platform.status'] = 1;

        $fields = ['c_platform.*','c_customer_company.company_name as customer_company_name','c_zplay_company.company_name as zplay_company_name','c_currency_type.currency_name','c_currency_type.currency_en','user.name as manager_name','c_platform_type.platform_type'];

        $map['leftjoin'] = [
            ['c_customer_company','c_customer_company.id', 'c_platform.customer_id'],
            ['c_zplay_company','c_zplay_company.id', 'c_platform.company_id'],
            ['c_currency_type','c_currency_type.id', 'c_platform.currency_type_id'],
            ['user','user.id', 'c_platform.business_manager_id'],
            ['c_platform_type','c_platform_type.id', 'c_platform.platform_type_id'],
        ];
//        // 获取分页数据
        $platform_list = PlatformLogic::getPlatformList($map, $fields)->forPage($page,$page_size)->orderby("c_platform.id","desc")->get();
        $platform_list = Service::data($platform_list);
        if (!$platform_list) ApiResponseFactory::apiResponse([],[],1000);

        // 获取数据总数
        $total = PlatformLogic::getPlatformList($map, $fields)->count();

        if ($platform_list){
            foreach ($platform_list as $key => $platform){

                $currency_name = $platform['currency_name'];
                unset($platform['currency_name']);
                if ($currency_name) {
                    $platform['currency_en'] = $currency_name . "({$platform['currency_en']})";
                }else{
                    $platform['currency_en'] = '';
                }

                $map = [];
                $map['platform_id'] = $platform['platform_id'];
                $map['customer_id'] = $platform['customer_id'];
                $map['company_id'] = $platform['company_id'];

                $fields = ['id','account','data_account','account_pass','account_user_id','account_app_id','account_api_key','account_token'];
                $result_sel = PlatformLogic::getPlatformAccountMapping($map,$fields)->get();
                $result_sel = Service::data($result_sel);
                if ($result_sel){
                    foreach ($result_sel as $k => $value){
                        $platform['platform_account'][$k] = $value;
                    }

                    if ($platform['platform_account']){
                        $platform_account = $platform['platform_account'];
                        foreach ($platform_account as $pak => $pav){
                            $map = [];
                            $map['platform_account_id'] = $pav['id'];
                            unset($pav['id']);
                            $fields = ['id','platform_account_id','agency_platform_id'];
                            $agency_result_sel = PlatformLogic::getPlatformAgenceMapping($map,$fields)->get();
                            $agency_result_sel = Service::data($agency_result_sel);
                            if ($agency_result_sel){
                                foreach ($agency_result_sel as $ark => $arv){
                                    $pav['agency_platform_id'][] = $arv['agency_platform_id'];
                                }
                            }
                            $platform_account[$pak] = $pav;
                        }
                        $platform['platform_account'] = $platform_account;
                    }
                }else{
                    $platform['platform_account']['account'] = '';
                    $platform['platform_account']['data_account'] = '';
                    $platform['platform_account']['account_pass'] = '';
                    $platform['platform_account']['account_user_id'] = '';
                    $platform['platform_account']['account_app_id'] = '';
                    $platform['platform_account']['account_api_key'] = '';
                    $platform['platform_account']['account_token'] = '';
                }

                // 获取渠道计费分成信息
                $divide_map['app_channel_id'] = $platform['id'];
                $divide_map['type'] = 3;
                $plat_divide_list = ChannelLogic::getChannelDivideMapping($divide_map)->orderby("create_time","desc")->first();
                $plat_divide_list = Service::data($plat_divide_list);

                if ($plat_divide_list){
                    if ($platform['platform_type_id'] == 2){
                        $platform['divide_ratio'] = $plat_divide_list['divide_ad'];
                    }else if($platform['platform_type_id'] == 3){
                        $platform['divide_ratio'] = $plat_divide_list['divide_billing'];
                    }

                }

                $platform_list[$key] = $platform;

            }
        }

        $back_data=[
            'table_list'=>$platform_list,
            'total'=> $total,
            'page_total'=> ceil($total / $page_size),
        ];

        ApiResponseFactory::apiResponse($back_data,[]);
    }

    /**
     * 编辑添加平台
     * @param $params array 请求数据
     */
    public static function createPlatform($params)
    {

        // 必填参数判断
        $id = isset($params['id']) ? $params['id'] : ''; // 平台自增ID
        $platform_type_id = isset($params['platform_type_id']) ? $params['platform_type_id'] : ''; // 平台类型
        $platform_name = isset($params['platform_name']) ? trim($params['platform_name']) : ''; // 平台名称
        $customer_name = isset($params['customer_name']) ? trim($params['customer_name']) : ''; // 客户公司
        $company_id = isset($params['company_id']) ? $params['company_id'] : ''; // 掌游旗下公司ID
        if (!$platform_type_id) ApiResponseFactory::apiResponse([],[],538);
        if (!$platform_name) ApiResponseFactory::apiResponse([],[],506);
        if (!$customer_name) ApiResponseFactory::apiResponse([],[],536);
        if (!$company_id) ApiResponseFactory::apiResponse([],[],537);

        // 平台类型区分不同参数
        $platform_account_list = [];
        if ($platform_type_id == 1){ // 统计平台

            $platform_id_prefix = 'ptj'; // 平台ID字符串拼接前缀
            //$platform_account_list = self::checkPlatformAccount($params);

        }else if ($platform_type_id == 2){ // 广告平台

            $platform_id_prefix = 'pad';
            //$platform_account_list = self::checkPlatformAccount($params);
            self::checkCurrencyType($params);
            self::checkBusManagerType($params);
            $divide_ad = self::checkAdDivide($params);

        }else if ($platform_type_id == 3){ // 计费平台

            $platform_id_prefix = 'pff';
            //$platform_account_list = self::checkPlatformAccount($params);
            self::checkCurrencyType($params);
            self::checkBusManagerType($params);
            $divide_billing = self::checkFfDivide($params);
            $bad_account_rate = self::checkRateOfBadAccount($params);

        }else if ($platform_type_id == 4){ // 推广平台

            $platform_id_prefix = 'ptg';
            //$platform_account_list = self::checkPlatformAccount($params);
            self::checkCurrencyType($params);
            self::checkBusManagerType($params);


        }else if ($platform_type_id == 5){ // 代理平台
            $platform_id_prefix = 'pdl';
            self::checkCurrencyType($params);
            self::checkBusManagerType($params);

        }


        $update_time = date('Y-m-d H:i:s');
        $create_time = date('Y-m-d H:i:s');
        unset($params['token']);
        unset($params['sessionid']);
        unset($params['customer_name']);
        unset($params['user_company_id']);
        unset($params['language']);

        $pinyin = new Pinyin(); // 默认

        // 判断 编辑? 还是 创建?
        if ($id){ // 编辑
            $platform_account_list = self::checkPlatformAccount($params);

            // 获取old_data
            $map = [];
            $map['c_platform.id'] = $id;
            $fields = ['c_platform.*','c_customer_company.company_name as customer_name'];

            $map['leftjoin'] = [
                ['c_customer_company','c_customer_company.id', 'c_platform.customer_id']
            ];
            $old_data = PlatformLogic::getPlatformList($map, $fields)->first();
            $old_data = Service::data($old_data);
            $old_customer_company_id = '';
            $old_company_id = '';
            if ($old_data){
                $old_customer_company_id = $old_data['customer_id'];
                $old_company_id = $old_data['company_id'];
                $map = [];
                $map['platform_id'] = $old_data['platform_id'];
                $fields = ['id','account','data_account'];
                $result_sel = PlatformLogic::getPlatformAccountMapping($map,$fields)->get();
                $result_sel = Service::data($result_sel);

                if ($result_sel){
                    foreach ($result_sel as $k => $value){
                        $old_data['platform_account'][$k] = $value;
                    }
                }

                if (isset($old_data['platform_account'])){
                    $platform_account = $old_data['platform_account'];
                    foreach ($platform_account as $pak => $pav){
                        $map = [];
                        $map['platform_account_id'] = $pav['id'];
                        $fields = ['id','platform_account_id','agency_platform_id'];
                        $agency_result_sel = PlatformLogic::getPlatformAgenceMapping($map,$fields)->get();
                        $agency_result_sel = Service::data($agency_result_sel);
                        if ($agency_result_sel){
                            foreach ($agency_result_sel as $ark => $arv){
                                $pav['agency_platform_id'][] = $arv['agency_platform_id'];
                            }
                        }
                        $platform_account[$pak] = $pav;
                    }
                    $old_data['platform_account'] = $platform_account;
                }

            }

            $platform_id = $old_data['platform_id'];

            $params['update_time'] = $update_time;
            unset($params['id']);

            // 开启事物 保存数据
            DB::beginTransaction();

            // 判断客户公司信息是否存在,存在取ID；不存在添加新数据，获取ID
            $map = [];
            $map['company_name'] = $customer_name;
            $customer_info = CommonLogic::getCustomerCompanyList($map)->first();
            $customer_info = Service::data($customer_info);
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


            //校验平台类型、平台名称、客户名称、公司名称不能重复
            $map = [];
            $map['platform_name'] = $platform_name;
            $map['platform_type_id'] = $platform_type_id;
            $map['customer_id'] = $customer_company_id;
            $map['company_id'] = $company_id;
            $map[] = ['id','<>',$id];

            $platform = PlatformLogic::getPlatformList($map)->first();
            if ($platform){ // 平台信息已经重复
                DB::rollBack();
                ApiResponseFactory::apiResponse([],[],526);
            }

            $params['customer_id'] = $customer_company_id;
            unset($params['platform_name']);
            unset($params['platform_type_id']);
            unset($params['platform_id']);
            unset($params['platform_account']);
            unset($params['divide_ratio']);

            $bool = PlatformLogic::updatePlatform($id, $params);
            if (!$bool){
                DB::rollBack();
                ApiResponseFactory::apiResponse([],[],502);
            }

            // 删除推广平台与账号关系
            if ($platform_account_list){
                foreach ($platform_account_list as $key => $platform_account) {
                    $platform_account['platform_id'] = $platform_id;

                    // 先查询推广平台与账号 删除 再添加
                    $map = [];
                    $map['platform_id'] = $platform_id;
                    if ($old_customer_company_id && $old_company_id){
                        $map['customer_id'] = $old_customer_company_id;
                        $map['company_id'] = $old_company_id;
                    }else{
                        $map['customer_id'] = $customer_company_id;
                        $map['company_id'] = $company_id;
                    }

                    $result_sel = PlatformLogic::getPlatformAccountMapping($map)->get();

                    $result_sel = Service::data($result_sel);
                    $platform_account_ids = [];
                    if ($result_sel) {
                        foreach ($result_sel as $r_k => $r_v) {
                            $platform_account_ids[] = $r_v['id'];
                        }
                        $result_del = PlatformLogic::deletePlatformAccountMapping($platform_account_ids);
                        if (!$result_del) {
                            DB::rollBack();
                            ApiResponseFactory::apiResponse([], [], 504);
                        }
                    }
                    // 先查询账号与代理平台 删除 再添加
                    if ($platform_account_ids) {
                        foreach ($platform_account_ids as $p_a_k => $platform_account_id) {
                            $map = [];
                            $map['platform_account_id'] = $platform_account_id;
                            $account_agency = PlatformLogic::getPlatformAgenceMapping($map)->get();
                            $account_agency = Service::data($account_agency);
                            if ($account_agency) {
                                $result_del = PlatformLogic::deletePlatformAgenceMapping($platform_account_id);
                                if (!$result_del) {
                                    DB::rollBack();
                                    ApiResponseFactory::apiResponse([], [], 804);
                                }
                            }
                        }
                    }
                }

            }
//            DB::commit();


            // 开启事物 保存数据
//            DB::beginTransaction();

            // 维护广告平台  与 广告分成关系
            if (isset($divide_ad)){
                $mapping_data = [];
                $mapping_data['app_channel_id'] = $id;
                $mapping_data['divide_ad'] = $divide_ad;
                $mapping_data['type'] = 3;
                $mapping_data['effective_date'] = $create_time = date('Y-m-d H:i:s');;
                $mapping_data['create_time'] = $create_time = date('Y-m-d H:i:s');;
                $result = ChannelLogic::addChannelDivideMapping($mapping_data);
                if (!$result){
                    DB::rollBack();
                    ApiResponseFactory::apiResponse([],[],817);
                }
            }

            // 维护计费平台  与 计费分成关系
            if (isset($divide_billing)){
                $mapping_data = [];
                $mapping_data['app_channel_id'] = $id;
                $mapping_data['divide_billing'] = $divide_billing;
                $mapping_data['type'] = 3;
                $mapping_data['effective_date'] = $create_time = date('Y-m-d H:i:s');;
                $mapping_data['create_time'] = $create_time = date('Y-m-d H:i:s');;
                $result = ChannelLogic::addChannelDivideMapping($mapping_data);
                if (!$result){
                    DB::rollBack();
                    ApiResponseFactory::apiResponse([],[],817);
                }
            }

            // 维护推广平台与账号关系
            if ($platform_account_list){
                foreach ($platform_account_list as $key => $platform_account){
                    $platform_account['platform_id'] = $platform_id;
                    $platform_account['customer_id'] = $customer_company_id;
                    $platform_account['company_id'] = $company_id;
                    $agency_platform_ids = [];
                    if (isset($platform_account['agency_platform_id'])){
                        $agency_platform_ids = $platform_account['agency_platform_id'];
                        unset($platform_account['agency_platform_id']);
                    }

                    if (empty($platform_account['account_pass'])){
                        unset($platform_account['account_pass']);
                    }
                    if (empty($platform_account['account_user_id'])){
                        unset($platform_account['account_user_id']);
                    }
                    if (empty($platform_account['account_app_id'])){
                        unset($platform_account['account_app_id']);
                    }
                    if (empty($platform_account['account_api_key'])){
                        unset($platform_account['account_api_key']);
                    }
                    if (empty($platform_account['account_token'])){
                        unset($platform_account['account_token']);
                    }
                    //校验平台与账号关系信息不能重复
                    $platform = PlatformLogic::getPlatformAccountMapping($platform_account)->first();
                    $platform = Service::data($platform);
                    if ($platform){ // 平台与账号关联信息重复
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([],[],801);
                    }

                    // 平台与账号关系
                    $platform_account_id = PlatformLogic::createPlatformAccountMapping($platform_account);
                    if (!$platform_account_id){
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([],[],503);
                    }

                    // 维护账号与代理平台关系
                    if ($agency_platform_ids){
                        foreach ($agency_platform_ids as $kk => $agency_platform_id) {
                            $account_agency = [];
                            $account_agency['platform_account_id'] = $platform_account_id;
                            $account_agency['agency_platform_id'] = $agency_platform_id;

                            //校验账号与代理平台关系信息不能重复
                            $platform = PlatformLogic::getPlatformAgenceMapping($account_agency)->first();
                            $platform = Service::data($platform);
                            if ($platform) { // 账号与代理平台关联信息重复
                                DB::rollBack();
                                ApiResponseFactory::apiResponse([], [], 802);
                            }

                            // 账号与代理平台
                            $result = PlatformLogic::addPlatformAgenceMapping($account_agency);
                            if (!$result) {
                                DB::rollBack();
                                ApiResponseFactory::apiResponse([], [], 803);
                            }
                        }
                    }
                }
            }

            DB::commit();

            // 保存日志
            $params['customer_id'] = $customer_company_id;
            $params['customer_name'] = $customer_name;
//            $params['platform_account'] = $platform_account_list;

            OperationLogImp::saveOperationLog(2,3, $params, $old_data);

            ApiResponseFactory::apiResponse($id,[]);

        }else{ // 创建

            $map = [];
            // 查询当前平台ID
            $map['platform_name'] = $platform_name;
            $map['platform_type_id'] = $platform_type_id;
            $platform = PlatformLogic::getPlatformList($map)->first();
            $platform = Service::data($platform);
            if ($platform){ // 存在则取此平台的平台ID
                $platform_id = $platform['platform_id'];
            }else{ // 如果不存在，则取最新一条数据的主键ID+1 为 最新平台的平台ID
               $platform = PlatformLogic::getPlatformList()->orderby("id","desc")->first();
               $platform = Service::data($platform);
               if ($platform){
                   $platform_id = $platform['id'] + 1;
                   if ($platform_id < 10){
                       $platform_id = $platform_id_prefix . '00' . strval($platform_id);
                   }else if($platform_id < 100){
                       $platform_id = $platform_id_prefix . '0' . strval($platform_id);
                   }else{
                       $platform_id = $platform_id_prefix . strval($platform_id);
                   }
               }else{
                   $platform_id = $platform_id_prefix . '001';
               }
            }
            if (!$platform_id) ApiResponseFactory::apiResponse([],[],501);

            $params['platform_id'] = $platform_id;
            unset($params['platform_account']);
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

            //校验平台类型、平台名称、客户名称、公司名称不能重复
            $map = [];
            $map['platform_name'] = $platform_name;
            $map['platform_type_id'] = $platform_type_id;
            $map['customer_id'] = $customer_company_id;
            $map['company_id'] = $company_id;

            $platform = PlatformLogic::getPlatformList($map)->first();
            $platform = Service::data($platform);
            if ($platform){ // 平台信息已经重复
                DB::rollBack();
                ApiResponseFactory::apiResponse([],[],526);
            }

            $params['customer_id'] = $customer_company_id;
            $platform_name_alise = $pinyin->sentence($platform_name);
            $params['alise'] = str_replace(' ','',$platform_name_alise);
            // 保存平台数据
            $new_id = PlatformLogic::addPlatform($params);
            if (!$new_id){
                DB::rollBack();
                ApiResponseFactory::apiResponse([],[],502);
            }

            // 维护广告平台  与 广告分成关系
            if (isset($divide_ad)){
                $mapping_data = [];
                $mapping_data['app_channel_id'] = $new_id;
                $mapping_data['divide_ad'] = $divide_ad;
                $mapping_data['type'] = 3;
                $mapping_data['effective_date'] = $create_time = date('Y-m-d H:i:s');;
                $mapping_data['create_time'] = $create_time = date('Y-m-d H:i:s');;
                $result = ChannelLogic::addChannelDivideMapping($mapping_data);
                if (!$result){
                    DB::rollBack();
                    ApiResponseFactory::apiResponse([],[],817);
                }
            }

            // 维护计费平台  与 计费分成关系
            if (isset($divide_billing)){
                $mapping_data = [];
                $mapping_data['app_channel_id'] = $new_id;
                $mapping_data['divide_billing'] = $divide_billing;
                $mapping_data['type'] = 3;
                $mapping_data['effective_date'] = $create_time = date('Y-m-d H:i:s');;
                $mapping_data['create_time'] = $create_time = date('Y-m-d H:i:s');;
                $result = ChannelLogic::addChannelDivideMapping($mapping_data);
                if (!$result){
                    DB::rollBack();
                    ApiResponseFactory::apiResponse([],[],817);
                }
            }

            // 维护推广平台与账号关系
            // if ($platform_account_list){
            //     foreach ($platform_account_list as $key => $platform_account){
            //         $platform_account['platform_id'] = $platform_id;
            //         $agency_platform_ids = [];
            //         if (isset($platform_account['agency_platform_id'])){
            //             $agency_platform_ids = $platform_account['agency_platform_id'];
            //             unset($platform_account['agency_platform_id']);
            //         }
            //         $platform_account['customer_id'] = $customer_company_id;
            //         $platform_account['company_id'] = $company_id;
            //         //校验平台与账号关系信息不能重复
            //         $platform = PlatformLogic::getPlatformAccountMapping($platform_account)->first();
            //         $platform = Service::data($platform);
            //         if ($platform){ // 平台与账号关联信息重复
            //             DB::rollBack();
            //             ApiResponseFactory::apiResponse([],[],801);
            //         }

            //         // 平台与账号关系
            //         $platform_account_id = PlatformLogic::createPlatformAccountMapping($platform_account);
            //         if (!$platform_account_id){
            //             DB::rollBack();
            //             ApiResponseFactory::apiResponse([],[],503);
            //         }

            //         // 维护账号与代理平台关系
            //         if ($agency_platform_ids){
            //             foreach ($agency_platform_ids as $kk => $agency_platform_id) {
            //                 $account_agency = [];
            //                 $account_agency['platform_account_id'] = $platform_account_id;
            //                 $account_agency['agency_platform_id'] = $agency_platform_id;

            //                 //校验账号与代理平台关系信息不能重复
            //                 $platform = PlatformLogic::getPlatformAgenceMapping($account_agency)->first();
            //                 $platform = Service::data($platform);
            //                 if ($platform) { // 账号与代理平台关联信息重复
            //                     DB::rollBack();
            //                     ApiResponseFactory::apiResponse([], [], 802);
            //                 }

            //                 // 账号与代理平台
            //                 $result = PlatformLogic::addPlatformAgenceMapping($account_agency);
            //                 if (!$result) {
            //                     DB::rollBack();
            //                     ApiResponseFactory::apiResponse([], [], 803);
            //                 }
            //             }
            //         }
            //     }
            // }
            DB::commit();

            // 保存日志
            OperationLogImp::saveOperationLog(1,3,$new_id);

            ApiResponseFactory::apiResponse($new_id,[]);

        }

    }

    // 其他平台账号信息验证
    private static function checkPlatformAccount($params){
        $platform_account_list = isset($params['platform_account']) ? $params['platform_account'] : []; // 平台账号信息
        if (!$platform_account_list) ApiResponseFactory::apiResponse([],[],507);

        $platform_account = [];
        foreach ($platform_account_list as $key => $value){

            $account = isset($value['account']) ? $value['account'] : '';
            $data_account = isset($value['data_account']) ? $value['data_account'] : '';
            $account_pass = isset($value['account_pass']) ? $value['account_pass'] : '';
            $account_user_id = isset($value['account_user_id']) ? $value['account_user_id'] : '';
            $account_app_id = isset($value['account_app_id']) ? $value['account_app_id'] : '';
            $account_api_key = isset($value['account_api_key']) ? $value['account_api_key'] : '';
            $account_token = isset($value['account_token']) ? $value['account_token'] : '';
            if ($account){
                if ($data_account){
                    $platform_account[$key]['account'] = $value['account'];
                    $platform_account[$key]['data_account'] = $value['data_account'];
                    $platform_account[$key]['account_pass'] = $account_pass;
                    $platform_account[$key]['account_user_id'] = $account_user_id;
                    $platform_account[$key]['account_app_id'] = $account_app_id;
                    $platform_account[$key]['account_api_key'] = $account_api_key;
                    $platform_account[$key]['account_token'] = $account_token;
                    if (isset($value['agency_platform_id'])) {
                        if ($value['agency_platform_id']){
                            $platform_account[$key]['agency_platform_id'] = $value['agency_platform_id'];
                        }else{
                            $platform_account[$key]['agency_platform_id'] = [0];
                        }
                    }
                }else{
                    ApiResponseFactory::apiResponse([],[],544);
                }
            }else{
                ApiResponseFactory::apiResponse([],[],507);
            }
        }

        return $platform_account;
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


    // 计费分成比例验证
    private static function checkFfDivide($params){
        $divide_billing = isset($params['divide_ratio']) ? $params['divide_ratio'] : ''; // 计费分成
        // 判断计费分成 小数点位数
//        if (!$divide_billing) ApiResponseFactory::apiResponse([],[],610);
        if (!is_numeric($divide_billing)) ApiResponseFactory::apiResponse([],[],694);
        $divide_billing = floatval($divide_billing);
        $divide_billing_arr = explode('.',$divide_billing);

        if (isset($divide_billing_arr[1]) && strlen($divide_billing_arr[1]) > 2){
            ApiResponseFactory::apiResponse([],[],696);
        }
        if ($divide_billing < 0 || $divide_billing > 100) ApiResponseFactory::apiResponse([],[],691);

        return $divide_billing;

    }

    // 广告分成比例验证
    private static function checkAdDivide($params){
        // 判断广告分成 小数点位数
        $divide_ad = isset($params['divide_ratio']) ? $params['divide_ratio'] : ''; // 广告分成
//        if (!$divide_ad ) ApiResponseFactory::apiResponse([],[],611);
        if (!is_numeric($divide_ad) ) ApiResponseFactory::apiResponse([],[],695);
        $divide_ad = floatval($divide_ad);
        $divide_ad_arr = explode('.',$divide_ad);
        if (isset($divide_ad_arr[1]) && strlen($divide_ad_arr[1]) > 2){
            ApiResponseFactory::apiResponse([],[],697);
        }
        if ($divide_ad < 0 || $divide_ad > 100) ApiResponseFactory::apiResponse([],[],692);

        return $divide_ad;
    }


    // 坏账率验证
    private static function checkRateOfBadAccount($params){
        // 判断坏账率 小数点位数
        $bad_account_rate = isset($params['bad_account_rate']) ? $params['bad_account_rate'] : ''; // 广告分成
//        if (!$bad_account_rate ) ApiResponseFactory::apiResponse([],[],813);
        if (!is_numeric($bad_account_rate) ) ApiResponseFactory::apiResponse([],[],814);
        $bad_account_rate = floatval($bad_account_rate);
        $bad_account_rate_arr = explode('.',$bad_account_rate);
        if (isset($bad_account_rate_arr[1]) && strlen($bad_account_rate_arr[1]) > 2){
            ApiResponseFactory::apiResponse([],[],815);
        }
        if ($bad_account_rate < 0 || $bad_account_rate > 100) ApiResponseFactory::apiResponse([],[],816);

        return $bad_account_rate;
    }
    /******************************可视化仪表盘*******************************************************/
    //平台数据状态
    public static function add_platform_status($platform_id,$platform_account,$data_total,$data_time){
        //先查询该平台今天是否已去过数据
        $map =[];
        $map['platform_id']=$platform_id;
        $map['platform_account']=$platform_account;
        $map['data_time']=$data_time;
        $data =  PlatformLogic::getPlatformStatusList($map)->first();
        $data = Service::data($data);
        // // 开启事物 保存数据
        // DB::beginTransaction();
        //删除数据库 数据
        $map_del['id']=$data['id'];
        PlatformLogic::delete_platform_status($map_del);
        $insert_data =[];
        if($data['data_total']==$data_total){
            $insert_data['data_status'] = 2;
        }else{
            $insert_data['data_status'] = 1;
        }
        $insert_data['platform_id'] = $platform_id;
        $insert_data['platform_account'] = $platform_account;
        $insert_data['data_total'] = $data_total;
        $insert_data['data_time'] = $data_time;
        $insert_data['create_time'] = date('Y-m-d H:i:s',time());
        $id = PlatformLogic::createPlatform_status($insert_data);
        return $id;
        // if($id){
        //      DB::commit();
        // }else{
        //      DB::rollBack();
        // }
    }


    /**
     *
     */
    public static function platformDataDetails($params){

        $platform_id = isset($params['platform_id']) ? $params['platform_id'] : '';
        $platform_type = isset($params['platform_type']) ? $params['platform_type'] : '';

        if (!$platform_id){
            ApiResponseFactory::apiResponse([],[],505);
        }

        if (!$platform_type){
            ApiResponseFactory::apiResponse([],[],538);
        }

        // 根据类型区分查询的表
        $table_name = 'zplay_ad_report_daily';
        $sum_field = "sum(earning) as total_num";
        $total_field = [DB::raw($sum_field),DB::raw('count(*) as record_num'),'platform_id','date'];
        $account_field = [DB::raw($sum_field),DB::raw('count(*) as record_num'),'platform_id','date','data_account as account'];
        $account_group_by = "data_account";
        $co_name_a = '新增用户数';
        $co_name_b = '数据量';
        $where = [];
        if ($platform_type == 1){
            // 统计平台
            $table_name = 'zplay_user_tj_report_daily';
            $sum_field = "sum(new_user) as total_num"; // 需要确定
            $total_field = [DB::raw($sum_field),DB::raw('count(*) as record_num'),'platform_id','date'];
            $account_field = [DB::raw($sum_field),DB::raw('count(*) as record_num'),'platform_id','date','account'];
            $account_group_by = 'account';
            $co_name_a = '新增用户数';
            if ($platform_id != 'ptj01'){
                $where = ['type' => 1, 'session_type' => 2];
            }

        }else if ($platform_type == 2){
            // 广告平台
            $table_name = 'zplay_ad_report_daily';
            $sum_field = "sum(earning) as total_num";
            $total_field = [DB::raw($sum_field),DB::raw('count(*) as record_num'),'platform_id','date'];
            $account_field = [DB::raw($sum_field),DB::raw('count(*) as record_num'),'platform_id','date','data_account as account'];
            $account_group_by = "data_account";
            $co_name_a = '广告收入';
            $where = ['statistics' => 0];

        }else if ($platform_type == 3){
            // 计费平台
            $table_name = 'zplay_ff_report_daily';
            $sum_field = "sum(income_fix) as total_num";
            $total_field = [DB::raw($sum_field),DB::raw('count(*) as record_num'),'platform_id','date'];
            $account_field = [DB::raw($sum_field),DB::raw('count(*) as record_num'),'platform_id','date','platform_account as account'];
            $account_group_by = "platform_account";
            $co_name_a = '付费收入';

        }else if ($platform_type == 4){
            // 推广平台
            $table_name = 'zplay_tg_report_daily';
            $sum_field = "sum(cost) as total_num";
            $total_field = [DB::raw($sum_field),DB::raw('count(*) as record_num'),'platform_id','date'];
            $account_field = [DB::raw($sum_field),DB::raw('count(*) as record_num'),'platform_id','date','platform_account as account'];
            $account_group_by = "platform_account";
            $co_name_a = '推广成本';
        }

        $begin_time = date('Y-m-d',strtotime('-7 day'));
        $end_time = date('Y-m-d',strtotime('-1 day'));
        $seven_date = [];
        for ($i = 7; $i >=1 ; $i--){
            $seven_date[] = date('Y-m-d',strtotime("-$i day"));
        }

        // 获取详细数据
        $map = [];
        if ($platform_id == 'ptg02'){
            $map['data_platform_id'] = $platform_id;
        }else{
            $map['platform_id'] = $platform_id;
        }

        $map['between'] = ['date',[ $begin_time, $end_time]];

        $total_list = PlatformLogic::getDailyReportList($table_name,$map)->select($total_field);
        if ($where){
            $total_list = $total_list->where($where);
        }
        $total_list = $total_list->groupBy(['platform_id','date'])->get();
        $total_list = Service::data($total_list);

        $account_list = PlatformLogic::getDailyReportList($table_name,$map)->select($account_field);
        if ($where){
            $account_list = $account_list->where($where);
        }
        $account_list = $account_list->groupBy(['platform_id','date',$account_group_by])->get();
        $account_list = Service::data($account_list);

        $return_list = [];
        if ($total_list){
            $total_account = [];
            $new_total_list = [];
            foreach ($total_list as $key => $value){
                if ($platform_type == 1){
                    $new_total_list[$value['date']]['total_num'] = $value['total_num'];
                }else{
                    $new_total_list[$value['date']]['total_num'] = round($value['total_num'],2);
                }

                $new_total_list[$value['date']]['record_num'] = $value['record_num'];
            }

            foreach ($seven_date as $k => $v) {
                if (!isset($new_total_list[$v])){
                    $new_total_list[$v]['total_num'] = 0;
                    $new_total_list[$v]['record_num'] = 0;
                }
            }

            ksort($new_total_list);
            $total_account['account_name'] = '总数据';
            $total_account['date'] = array_keys($new_total_list);
            $total_account['data_count'][$co_name_a] = array_column($new_total_list,'total_num');
            $total_account['data_count'][$co_name_b] = array_column($new_total_list,'record_num');
            $return_list[] = $total_account;

            if ($account_list){
                $per_account = [];
                $new_account_list = [];
                foreach ($account_list as $key => $value){
                    if ($platform_type == 1){
                        $new_account_list[$value['account']][$value['date']]['total_num'] = $value['total_num'];
                    }else{
                        $new_account_list[$value['account']][$value['date']]['total_num'] =  round($value['total_num'],2);
                    }

                    $new_account_list[$value['account']][$value['date']]['record_num'] = $value['record_num'];
                }

                foreach ($new_account_list as $kk => $vv){
                    foreach ($seven_date as $k => $v) {
                        if (!isset($vv[$v])){
                            $vv[$v]['total_num'] = 0;
                            $vv[$v]['record_num'] = 0;
                        }
                    }
                    ksort($vv);
                    $new_account_list[$kk] = $vv;
                }

                if (count($new_account_list) != 1){
                    foreach ($new_account_list as $n_k => $n_v){
                        $per_account['account_name'] = $n_k;
                        $per_account['date'] = array_keys($n_v);;
                        $per_account['data_count'][$co_name_a] = array_column($n_v,'total_num');
                        $per_account['data_count'][$co_name_b] = array_column($n_v,'record_num');
                        $return_list[] = $per_account;
                    }
                }else{
                    foreach ($new_account_list as $n_k => $n_v){
                        $return_list[0]['account_name'] = $n_k;
                    }
                }

            }
        }

        ApiResponseFactory::apiResponse(['table_list' => $return_list ],[]);
    }


    public static function platformDataStatus($params){

        $begin_date = date('Y-m-d',strtotime('-2 day'));
//        $end_date = date('Y-m-d',strtotime('-1 day'));
        $end_date = date('Y-m-d');

        $sql = "SELECT
                c_platform_data_status.data_status, 
                c_platform_data_status.data_time, 
                sum(
                  c_platform_data_status.data_total
                ) AS data_total,
                c_platform.platform_id,
                c_platform.platform_type_id,
                c_platform.platform_name
                FROM
                c_platform_data_status
                left join (select distinct platform_name,platform_id,platform_type_id from c_platform) as c_platform on  c_platform.platform_id = c_platform_data_status.platform_id
                WHERE
                c_platform_data_status.data_time >= '$begin_date'
                AND c_platform_data_status.data_time <= '$end_date'
                GROUP BY
                c_platform_data_status.data_time,c_platform_data_status.platform_id
                ORDER BY
                c_platform.platform_type_id,
                c_platform_data_status.data_total,
                c_platform_data_status.platform_id";

        $info = DB::select($sql);
        $info = Service::data($info);

        // 货币类型
        $currency_sql = "select distinct platform_name,platform_id,platform_type_id,currency_type_id,c_currency_type.currency_en from c_platform left join c_currency_type on c_platform.currency_type_id =c_currency_type.id";
        $currency_list = DB::select($currency_sql);
        $currency_list = Service::data($currency_list);


        $seven_date = [];
        for ($i = 2; $i >=0 ; $i--){
            $seven_date[] = date('Y-m-d',strtotime("-$i day"));
        }

        $data =[];
        $data_info = [];
        //先处理数据
        if ($info) {
            foreach ($info as $key => $value) {
                $data[$value['platform_id']]['platform_type_id'] = $value['platform_type_id'];
                $data[$value['platform_id']]['platform_id'] = $value['platform_id'];
                $currency_en = [];
                if ($currency_list){
                    foreach ($currency_list as $culk => $culv){
                        if (($value['platform_type_id'] == $culv['platform_type_id']) && ($value['platform_id'] == $culv['platform_id'])){
                            $currency_en[] = $culv['currency_en'];
                        }
                    }
                }
                $data[$value['platform_id']]['currency'] = implode(',',$currency_en);
                $data[$value['platform_id']]['platform_name'] = $value['platform_name'];
                $data[$value['platform_id']]['data'][$value['data_time']]['date'] = $value['data_time'];
                if ($value['platform_type_id'] == 1){
                    $data[$value['platform_id']]['data'][$value['data_time']]['data_count'] = $value['data_total'];
                }else{
                    $data[$value['platform_id']]['data'][$value['data_time']]['data_count'] = round($value['data_total'],2);
                }
                $data[$value['platform_id']]['data'][$value['data_time']]['status'] = $value['data_status'];
            }

            foreach ($data as $data_key => $data_value) {
                foreach ($seven_date as $v) {
                    if (!isset($data_value['data'][$v])) {
                        $data_value['data'][$v]['date'] = $v;
                        $data_value['data'][$v]['data_count'] = 0;
                        $data_value['data'][$v]['status'] = 3;
                    }
                }
                ksort($data_value['data']);
                $data_value['data'] = array_values($data_value['data']);
                $data[$data_key] = $data_value;
            }

            foreach ($data as $data_platform_id => $v) {
                $per_data_info = [];
                // //显示以稳定的平台状态
                if ($v['platform_type_id'] == 1) {
                    $per_data_info['plat_name'] = $v['platform_name'];
                    $per_data_info['plat_id'] = $v['platform_id'];
                    $per_data_info['currency'] = $v['currency'];
                    $per_data_info['data'] = $v['data'];

                    if (isset($v['data'])) {
                        foreach ($v['data'] as $dck => $data_count_info) {
                            if ($data_count_info['status'] == 1) {
                                $d_k = date('Y-m-d', strtotime($data_count_info['date']));
                                $data_info['tj']['stable_list'][$v['platform_id']] = ["$d_k" => $v['platform_name']];
                            }
                        }
                    }
                    $data_info['tj']['stable_list'] = isset($data_info['tj']['stable_list']) ? array_values($data_info['tj']['stable_list']) : [];
                    $data_info['tj']['plat_type'] = 1;
                    $data_info['tj']['plat_list'][] = $per_data_info;


                } else if ($v['platform_type_id'] == 2) {
                    $per_data_info['plat_name'] = $v['platform_name'];
                    $per_data_info['plat_id'] = $v['platform_id'];
                    $per_data_info['currency'] = $v['currency'];
                    $per_data_info['data'] = $v['data'];

                    if (isset($v['data'])) {
                        foreach ($v['data'] as $dck => $data_count_info) {
                            if ($data_count_info['status'] == 1) {
                                $d_k = date('Y-m-d', strtotime($data_count_info['date']));
                                $data_info['ad']['stable_list'][$v['platform_id']] = ["$d_k" => $v['platform_name']];
                            }
                        }
                    }

                    $data_info['ad']['stable_list'] = isset($data_info['ad']['stable_list']) ? array_values($data_info['ad']['stable_list']) : [];

                    $data_info['ad']['plat_type'] = 2;
                    $data_info['ad']['plat_list'][] = $per_data_info;

                } else if ($v['platform_type_id'] == 3) {
                    $per_data_info['plat_name'] = $v['platform_name'];
                    $per_data_info['plat_id'] = $v['platform_id'];
                    $per_data_info['currency'] = 'CNY';
                    $per_data_info['data'] = $v['data'];

                    if (isset($v['data'])) {
                        foreach ($v['data'] as $dck => $data_count_info) {
                            if ($data_count_info['status'] == 1) {
                                $d_k = date('Y-m-d', strtotime($data_count_info['date']));
                                $data_info['jf']['stable_list'][$v['platform_id']] = ["$d_k" => $v['platform_name']];
                            }
                        }
                    }
                    $data_info['jf']['stable_list'] = isset($data_info['jf']['stable_list']) ? array_values($data_info['jf']['stable_list']) : [];
                    $data_info['jf']['plat_type'] = 3;
                    $data_info['jf']['plat_list'][] = $per_data_info;
                } else {

                    $per_data_info['plat_name'] = $v['platform_name'];
                    $per_data_info['plat_id'] = $v['platform_id'];
                    $per_data_info['currency'] = $v['currency'];
                    $per_data_info['data'] = $v['data'];

                    if (isset($v['data'])) {
                        foreach ($v['data'] as $dck => $data_count_info) {
                            if ($data_count_info['status'] == 1) {
                                $d_k = date('Y-m-d', strtotime($data_count_info['date']));
                                $data_info['tg']['stable_list'][$v['platform_id']] = ["$d_k" => $v['platform_name']];
                            }
                        }
                    }

                    $data_info['tg']['stable_list'] = isset($data_info['tg']['stable_list']) ? array_values($data_info['tg']['stable_list']) : [];
                    $data_info['tg']['plat_type'] = 4;
                    $data_info['tg']['plat_list'][] = $per_data_info;
                }
            }
        }

        if ($data_info){
            foreach ($data_info as $dik => $plat_data_list){
                if (isset($plat_data_list['stable_list']) && isset($plat_data_list['stable_list'])){
                    $tg_stable_list = $plat_data_list['stable_list'];
                    $new_tg_stable_list = [];
                    if ($tg_stable_list){
                        foreach ($tg_stable_list as $slk => $slv){
                            foreach($slv as $kk => $vv){
                                $new_tg_stable_list[$kk][] = $vv;
                            }
                        }
                    }
                    $new_tg_stable_str_list = [];
                    if ($new_tg_stable_list){
                        foreach ($new_tg_stable_list as $ntslk => $ntslv){
                            $new_tg_stable_str_list[] = $ntslk . '数据增长中:'.implode(',',$ntslv);
                        }
                    }
                    $plat_data_list['stable_list'] = $new_tg_stable_str_list;
                }
                $data_info[$dik] = $plat_data_list;
            }

            // 自定义排序
            $sort_platform_list = [
                'tj' => ['ptj01','ptj02'],
                'ad' => ['pad01','pad23','pad05','pad24','pad36','pad09','pad33','pad02','pad11','pad10','pad03','pad50','pad52','pad29','pad21','pad12','pad44','pad69','pad262','pad271','pad272','pad63','pad54','pad35','pad34','pad16','pad261','pad42','pad40','pad65','pad64','pad62','pad56'],
                'jf' => ['pff02','pff03','pff05','pff04','pcf06','pcf25','pcf23','pcf11','pcf19','pcf13','pcf07'],
                'tg' => ['ptg40','ptg33','ptg36','ptg21','ptg03','ptg25','ptg75','ptg37','ptg72','ptg66','ptg67','ptg76','ptg68','ptg282','ptg80','ptg78','ptg63','ptg74','ptg06','ptg07','ptg05'],
            ];

            foreach ($data_info as $disk => $plat_data_sort_list){
                if (isset($plat_data_sort_list['plat_list']) && isset($plat_data_sort_list['plat_list'])){
                    $sort_plat_list = $plat_data_sort_list['plat_list'];
                    $new_sort_plat_list = [];
                    foreach ($sort_platform_list[$disk] as  $platform_id){
                        foreach ($sort_plat_list as $sspk => $sspv){
                            if ($sspv['plat_id'] == $platform_id){
                                $new_sort_plat_list[] = $sspv;
                            }
                        }
                    }
                    $data_info[$disk]['plat_list'] = $new_sort_plat_list;
                }
            }
        }
        ApiResponseFactory::apiResponse(['table_list' => $data_info ],[]);

    }

}
