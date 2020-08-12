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

class ChannelImp extends ApiBaseImp
{
    /**
     * 渠道列表
     * @param $params array 请求数据
     */
    public static function getChannelList($params)
    {

        $search_name = isset($params['search_name']) ? $params['search_name'] : ''; // 搜索名称
//        $customer_name = isset($params['customer_name']) ? $params['customer_name'] : ''; // 客户名称
        $company_id = isset($params['company_id']) ? $params['company_id'] : ''; // 公司ID
        $channel_type = isset($params['channel_type']) ? $params['channel_type'] : ''; // 渠道类型
        $channel_region = isset($params['channel_region']) ? $params['channel_region'] : ''; // 渠道区域
        $page = isset($params['page']) ? $params['page'] : 1 ;
        $page_size = isset($params['size']) ? $params['size'] : 1000 ;

        $map = []; // 查询条件
        if ($search_name) $map['like'][] = ['c_channel.channel_name','like', $search_name];
        if ($search_name) $map['like'][] = ['c_customer_company.company_name','like', $search_name];
        if ($search_name) $map['like'][] = ['c_channel.channel_id','like', $search_name];
        if ($channel_type) $map['c_channel.channel_type'] = $channel_type;
        if ($company_id) $map['c_channel.company_id'] = $company_id;
        if ($channel_region) $map['c_channel.channel_region'] = $channel_region;

        $fields = ['c_channel.*','c_customer_company.company_name as customer_company_name','c_zplay_company.company_name as zplay_company_name','c_currency_type.currency_name','c_currency_type.currency_en','user.name as manager_name'];

        $map['leftjoin'] = [
            ['c_customer_company','c_customer_company.id', 'c_channel.customer_id'],
            ['c_zplay_company','c_zplay_company.id', 'c_channel.company_id'],
            ['c_currency_type','c_currency_type.id', 'c_channel.currency_type_id'],
            ['user','user.id', 'c_channel.business_manager_id']
        ];
        // 获取分页数据
        $channel_list = ChannelLogic::getChannelList($map, $fields)->forPage($page,$page_size)->orderby("c_channel.sort","desc")->get();
        $channel_list = Service::data($channel_list);
        if (!$channel_list) ApiResponseFactory::apiResponse([],[],1000);

        // 获取数据总数
        $total = ChannelLogic::getChannelList($map)->count();

        // 获取渠道关联计费平台信息
        $fields = ['c_channel_payment_mapping.*','c_platform.platform_name'];

        $pay_map['leftjoin'] = [
            ['c_platform','c_platform.platform_id', 'c_channel_payment_mapping.pay_platform_id']
        ];
        $channel_payment_list = ChannelLogic::getChannelPaymentMapping($pay_map, $fields)->get();
        $channel_payment_list = Service::data($channel_payment_list);

        // 获取渠道关联广告平台信息
        $fields = ['c_channel_ad_platform_mapping.*','c_platform.platform_name'];

        $ad_map['leftjoin'] = [
            ['c_platform','c_platform.platform_id', 'c_channel_ad_platform_mapping.ad_platform_id']
        ];
        $channel_adplatform_list = ChannelLogic::getChannelAdPlatformMapping($ad_map, $fields)->get();
        $channel_adplatform_list = Service::data($channel_adplatform_list);


        // 获取渠道关联推广平台信息
        $fields = ['c_channel_generalize_mapping.*','c_platform.platform_name'];
        $gen_map['leftjoin'] = [
            ['c_platform','c_platform.platform_id', 'c_channel_generalize_mapping.generalize_platform_id']
        ];
        $channel_generalize_list = ChannelLogic::getChannelGeneralizeMapping($gen_map, $fields)->get();
        $channel_generalize_list = Service::data($channel_generalize_list);

        if ($channel_list){
            foreach ($channel_list as $key => $channel){
                $currency_name = $channel['currency_name'];
                unset($channel['currency_name']);
                if ($currency_name){
                    $channel['currency_en'] = $currency_name."({$channel['currency_en']})";
                }else{
                    $channel['currency_en'] = '';
                }

                if ($channel_payment_list){
                    foreach ($channel_payment_list as $kk => $channel_payment_list_value) {
                        if ($channel['id'] == $channel_payment_list_value['channel_id']) {
                            $channel['pay_platform'][] = $channel_payment_list_value['pay_platform_id'];
                        }
                    }
                    if (isset($channel['pay_platform'])) $channel['pay_platform'] = implode(',',$channel['pay_platform']);
                }
                if ($channel_adplatform_list){
                    foreach ($channel_adplatform_list as $kk => $channel_adplatform_list_value) {
                        if ($channel['id'] == $channel_adplatform_list_value['channel_id']) {
                            $channel['ad_platform'] = $channel_adplatform_list_value['ad_platform_id'];
                        }
                    }
                }
                if ($channel_generalize_list){
                    foreach ($channel_generalize_list as $kk => $channel_generalize_list_value) {
                        if ($channel['id'] == $channel_generalize_list_value['channel_id']) {
                            $channel['generalize_platform'] = $channel_generalize_list_value['generalize_platform_id'];
                        }
                    }
                }

                // 获取渠道计费分成信息
                $divide_map['app_channel_id'] = $channel['id'];
                $divide_map['type'] = 2;
                $channel_divide_list = ChannelLogic::getChannelDivideMapping($divide_map)->orderby("create_time","desc")->first();
                $channel_divide_list = Service::data($channel_divide_list);

                if ($channel_divide_list){
                    $channel['divide_billing'] = $channel_divide_list['divide_billing'];
                    $channel['divide_ad'] = $channel_divide_list['divide_ad'];
                }

                unset($channel['create_time']);
                unset($channel['update_time']);
                $channel_list[$key] = $channel;
            }
        }

        $back_data=[
            'table_list'=>$channel_list,
            'total'=> $total,
            'page_total'=> ceil($total / $page_size),
        ];

        ApiResponseFactory::apiResponse($back_data,[]);
    }

    /**
     * 编辑添加渠道
     * @param $params array 请求数据
     */
    public static function createChannel($params)
    {

        // 必填参数判断
        $id = isset($params['id']) ? $params['id'] : ''; // 渠道自增ID
        $channel_name = isset($params['channel_name']) ? trim($params['channel_name']) : ''; // 渠道名称
        $customer_name = isset($params['customer_name']) ? trim($params['customer_name']) : ''; // 客户公司名称
        $company_id = isset($params['company_id']) ? $params['company_id'] : ''; // 掌游旗下公司ID
        $channel_type = isset($params['channel_type']) ? $params['channel_type'] : ''; // 渠道类型
        $channel_region = isset($params['channel_region']) ? $params['channel_region'] : ''; // 渠道区域
        $business_manager_id = isset($params['business_manager_id']) ? $params['business_manager_id'] : ''; // 渠道负责人

        if (!$channel_name) ApiResponseFactory::apiResponse([],[],511);
        if (!$customer_name) ApiResponseFactory::apiResponse([],[],540);
        if (!$company_id) ApiResponseFactory::apiResponse([],[],541);
        if (!$channel_type) ApiResponseFactory::apiResponse([],[],542);
        if (!$channel_region) ApiResponseFactory::apiResponse([],[],543);
        if (!$business_manager_id) ApiResponseFactory::apiResponse([],[],510);

        $divide_billing = isset($params['divide_billing']) ? $params['divide_billing'] : ''; // 计费分成
        $divide_ad = isset($params['divide_ad']) ? $params['divide_ad'] : ''; // 广告分成

        // 判断计费分成 小数点位数
//        if (!$divide_billing) ApiResponseFactory::apiResponse([],[],610);
        if (!is_numeric($divide_billing)) ApiResponseFactory::apiResponse([],[],694);
        $divide_billing = floatval($divide_billing);
        $divide_billing_arr = explode('.',$divide_billing);

        if (isset($divide_billing_arr[1]) && strlen($divide_billing_arr[1]) > 2){
            ApiResponseFactory::apiResponse([],[],696);
        }
        if ($divide_billing < 0 || $divide_billing > 100) ApiResponseFactory::apiResponse([],[],691);

        // 判断广告分成 小数点位数
//        if (!$divide_ad ) ApiResponseFactory::apiResponse([],[],611);
        if (!is_numeric($divide_ad) ) ApiResponseFactory::apiResponse([],[],695);
        $divide_ad = floatval($divide_ad);
        $divide_ad_arr = explode('.',$divide_ad);
        if (isset($divide_ad_arr[1]) && strlen($divide_ad_arr[1]) > 2){
            ApiResponseFactory::apiResponse([],[],697);
        }
        if ($divide_ad < 0 || $divide_ad > 100) ApiResponseFactory::apiResponse([],[],692);

        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : ''; // 货币类型ID
        if (!$currency_type_id) ApiResponseFactory::apiResponse([],[],512);

        $pay_platform_id = isset($params['pay_platform_id']) ? $params['pay_platform_id'] : ''; // 计费平台ID
        $ad_platform_id = isset($params['ad_platform_id']) ? $params['ad_platform_id'] : ''; // 广告平台ID
        $generalize_platform_id = isset($params['generalize_platform_id']) ? $params['generalize_platform_id'] : ''; // 推广平台ID

        $is_jailbreak = isset($params['is_jailbreak']) ? $params['is_jailbreak'] : 0; // 是否为越狱

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
            // 获取当前ID的old数据
            $map = [];
            $map['c_channel.id'] = $id;
            $fields = ['c_channel.*','c_customer_company.company_name as customer_name'];

            $map['leftjoin'] = [
                ['c_customer_company','c_customer_company.id', 'c_channel.customer_id']
            ];

            $old_data = ChannelLogic::getChannelList($map,$fields)->first();
            $old_data = Service::data($old_data);

            // 获取渠道关联计费平台信息
            $fields = ['c_channel_payment_mapping.*','c_platform.platform_name'];

            $pay_map['channel_id'] = $id;
            $pay_map['leftjoin'] = [
                ['c_platform','c_platform.platform_id', 'c_channel_payment_mapping.pay_platform_id']
            ];
            $channel_payment_list = ChannelLogic::getChannelPaymentMapping($pay_map, $fields)->first();
            $channel_payment_list = Service::data($channel_payment_list);
            if ($channel_payment_list){
                $old_data['pay_platform_id'] = $channel_payment_list['pay_platform_id'];
            }else{
                $old_data['pay_platform_id'] = '';
            }
            // 获取渠道关联广告平台信息
            $fields = ['c_channel_ad_platform_mapping.*','c_platform.platform_name'];
            $ad_map['channel_id'] = $id;
            $ad_map['leftjoin'] = [
                ['c_platform','c_platform.platform_id', 'c_channel_ad_platform_mapping.ad_platform_id']
            ];
            $channel_adplatform_list = ChannelLogic::getChannelAdPlatformMapping($ad_map, $fields)->first();
            $channel_adplatform_list = Service::data($channel_adplatform_list);
            if ($channel_adplatform_list){
                $old_data['ad_platform_id'] = $channel_adplatform_list['ad_platform_id'];
            }else{
                $old_data['ad_platform_id'] = '';
            }

            // 获取渠道关联推广平台信息
            $gen_map['channel_id'] = $id;
            $fields = ['c_channel_generalize_mapping.*','c_platform.platform_name'];
            $gen_map['leftjoin'] = [
                ['c_platform','c_platform.platform_id', 'c_channel_generalize_mapping.generalize_platform_id']
            ];
            $channel_generalize_list = ChannelLogic::getChannelGeneralizeMapping($gen_map, $fields)->first();
            $channel_generalize_list = Service::data($channel_generalize_list);
            if ($channel_generalize_list){
                $old_data['generalize_platform_id'] = $channel_generalize_list['generalize_platform_id'];
            }else{
                $old_data['generalize_platform_id'] = '';
            }

            // 获取渠道计费分成信息
            $divide_map['app_channel_id'] = $id;
            $divide_map['type'] = 2;
            $channel_divide_list = ChannelLogic::getChannelDivideMapping($divide_map)->orderby("create_time","desc")->first();
            $channel_divide_list = Service::data($channel_divide_list);
            if ($channel_divide_list){
                $old_data['divide_billing'] = $channel_divide_list['divide_billing'];
                $old_data['divide_ad'] = $channel_divide_list['divide_ad'];
            }else{
                $old_data['divide_billing'] = '';
                $old_data['divide_ad'] = '';
            }

            // 保存修改数据
            $channel_id = isset($params['channel_id']) ? $params['channel_id'] : ''; // 渠道ID
            if (!$channel_id) ApiResponseFactory::apiResponse([],[],521);

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
            }
            if (!$customer_company_id) ApiResponseFactory::apiResponse([],[],688);
//            else{
//                $cus_data['company_name'] = $customer_name;
//                $cus_data['create_time'] = $create_time;
//                $cus_data['update_time'] = $update_time;
//                $customer_company_id = PlatformLogic::createCustomerCompany($cus_data);
//                if (!$customer_company_id){
//                    DB::rollBack();
//                    ApiResponseFactory::apiResponse([],[],527);
//                }
//            }

            //校验渠道名称、客户名称、公司名称、渠道类型、渠道区域不可重复
            $map = [];
            $map['channel_name'] = $channel_name;
            $map['customer_id'] = $customer_company_id;
            $map['company_id'] = $company_id;
            $map['channel_type'] = $channel_type;
            $map['channel_region'] = $channel_region;
            $map[] = ['id','<>',$id];

            $platform = ChannelLogic::getChannelList($map)->first();
            $platform = Service::data($platform);
            if ($platform){ // 渠道信息已经重复
                DB::rollBack();
                ApiResponseFactory::apiResponse([],[],528);
            }

            // 拼接渠道数据
//            $add_data['channel_id'] = $channel_id;
//            $add_data['channel_name'] = $channel_name;
//            $add_data['customer_id'] = $customer_company_id;
//            $add_data['company_id'] = $company_id;
            $add_data['channel_type'] = $channel_type;
            $add_data['channel_region'] = $channel_region;
            $add_data['currency_type_id'] = $currency_type_id;
            $add_data['business_manager_id'] = $business_manager_id;
            $add_data['update_time'] = $update_time;

            $bool = ChannelLogic::updateChannel($id, $add_data);
            if (!$bool){
                DB::rollBack();
                ApiResponseFactory::apiResponse([],[],516);
            }

            // 维护渠道 计费平台关系
            // 先查询 删除 再添加
            $map = [];
            $map['channel_id'] = $id;
            $result_sel = ChannelLogic::getChannelPaymentMapping($map)->get();
            $result_sel = Service::data($result_sel);
            if ($result_sel){
                $result_del = ChannelLogic::deleteChannelPaymentMapping($id);
                if (!$result_del){
                    DB::rollBack();
                    ApiResponseFactory::apiResponse([],[],522);
                }
            }

            if ($pay_platform_id){
                $pay_platform_id = explode(',',$pay_platform_id);
                foreach ($pay_platform_id as $ppk => $pay_platform_id_value){
                    $mapping_data = [];
                    $mapping_data['channel_id'] = $id;
                    $mapping_data['pay_platform_id'] = $pay_platform_id_value;
                    $result = ChannelLogic::addChannelPaymentMapping($mapping_data);
                    if (!$result){
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([],[],517);
                    }
                }
            }

            // 维护渠道 广告平台关系
            // 先查询 删除 再添加
            $map = [];
            $map['channel_id'] = $id;
            $result_sel = ChannelLogic::getChannelAdPlatformMapping($map)->get();
            $result_sel = Service::data($result_sel);
            if ($result_sel){
                $result_del = ChannelLogic::deleteChannelAdPlatformMapping($id);
                if (!$result_del){
                    DB::rollBack();
                    ApiResponseFactory::apiResponse([],[],522);
                }
            }
            if ($ad_platform_id){
                $mapping_data = [];
                $mapping_data['channel_id'] = $id;
                $mapping_data['ad_platform_id'] = $ad_platform_id;
                $result = ChannelLogic::addChannelAdPlatformMapping($mapping_data);
                if (!$result){
                    DB::rollBack();
                    ApiResponseFactory::apiResponse([],[],518);
                }
            }
            // 维护渠道 推广平台关系
            // 先查询 删除 再添加
            $map = [];
            $map['channel_id'] = $id;
            $result_sel = ChannelLogic::getChannelGeneralizeMapping($map)->get();
            $result_sel = Service::data($result_sel);
            if ($result_sel){
                $result_del = ChannelLogic::deleteChannelGeneralizeMapping($id);
                if (!$result_del){
                    DB::rollBack();
                    ApiResponseFactory::apiResponse([],[],522);
                }
            }
            if ($generalize_platform_id){

                $mapping_data = [];
                $mapping_data['channel_id'] = $id;
                $mapping_data['generalize_platform_id'] = $generalize_platform_id;
                $result = ChannelLogic::addChannelGeneralizeMapping($mapping_data);
                if (!$result){
                    DB::rollBack();
                    ApiResponseFactory::apiResponse([],[],519);
                }
            }
            // 维护渠道分成关系
            if (isset($divide_billing) && isset($divide_ad)){
                $mapping_data = [];
                $mapping_data['app_channel_id'] = $id;
                $mapping_data['divide_billing'] = $divide_billing;
                $mapping_data['divide_ad'] = $divide_ad;
                $mapping_data['type'] = 2;
                $mapping_data['create_time'] = $create_time = date('Y-m-d H:i:s');;
                $result = ChannelLogic::addChannelDivideMapping($mapping_data);
                if (!$result){
                    DB::rollBack();
                    ApiResponseFactory::apiResponse([],[],520);
                }
            }

            DB::commit();

            // 保存日志
            $params['customer_id'] = $customer_company_id;
            $params['customer_name'] = $customer_name;
            if (!isset($params['pay_platform_id'])) $params['pay_platform_id'] = '';
            if (!isset($params['ad_platform_id'])) $params['ad_platform_id'] = '';
            if (!isset($params['generalize_platform_id'])) $params['generalize_platform_id'] = '';

            OperationLogImp::saveOperationLog(2,4, $params, $old_data);

            ApiResponseFactory::apiResponse($id,[]);

        }else{ // 创建

            // 渠道ID 命名规则
            $channel_id = 'c';
            // 渠道区域(1,全球;2,国外;3,国内)
            // 渠道类型(1,ios流量;2,Android流量;3,h5;4,Amazon;)
            if ($channel_type == 3){
                // h5
                $channel_id .= 'h';

            }elseif ($channel_type == 4){
                // Amazon
                $channel_id .= 'z';
            }elseif ($channel_type == 1){
                // ios流量
                if ($is_jailbreak){
                    // 越狱
                    $channel_id .= 'o';
                }else{
                    // 非越狱
                    $channel_id .= 'i';
                }
            }elseif ($channel_type == 2){
                // Android流量
                if ($channel_region == 1){
                    // 国外
                    $channel_id .= 'g';
                }elseif ($channel_region == 3){
                    // 国内
                    $channel_id .= 'a';
                }
            }

            $map = [];
            $map['channel_name'] = $channel_name;
            $channel = ChannelLogic::getChannelList($map)->first();
            $channel = Service::data($channel);
            if ($channel){ // 存在则取此渠道的渠道ID
                $channel_id = $channel['channel_id'];
            }else{ // 如果不存在，则取最新一条数据的主键ID+1 为 最新渠道的渠道ID
                $channel = ChannelLogic::getChannelList()->orderby("id","desc")->first();
                $channel = Service::data($channel);
               if ($channel){
                   $auto_channel_id = $channel['id'] + 1;
                   if ($auto_channel_id < 10){
                       $channel_id .= '00' . strval($auto_channel_id);
                   }else if($auto_channel_id < 100){
                       $channel_id .= '0' . strval($auto_channel_id);
                   }else{
                       $channel_id .= strval($auto_channel_id);
                   }

               }else{
                   $channel_id .= '001';
               }
            }
            if (!$channel_id) ApiResponseFactory::apiResponse([],[],515);

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

            //校验渠道名称、客户名称、公司名称、渠道类型、渠道区域不可重复
            $map = [];
            $map['channel_name'] = $channel_name;
            $map['customer_id'] = $customer_company_id;
            $map['company_id'] = $company_id;
            $map['channel_type'] = $channel_type;
            $map['channel_region'] = $channel_region;

            $platform = ChannelLogic::getChannelList($map)->first();
            $platform = Service::data($platform);
            if ($platform){ // 渠道信息已经重复
                DB::rollBack();
                ApiResponseFactory::apiResponse([],[],528);
            }

            // 拼接渠道数据
            $add_data['channel_id'] = $channel_id;
            $add_data['td_channel_id'] = $channel_id;
            $add_data['channel_name'] = $channel_name;
            $add_data['customer_id'] = $customer_company_id;
            $add_data['company_id'] = $company_id;
            $add_data['channel_type'] = $channel_type;
            $add_data['channel_region'] = $channel_region;
            $add_data['is_jailbreak'] = $is_jailbreak;
            $add_data['currency_type_id'] = $currency_type_id;
            $add_data['business_manager_id'] = $business_manager_id;
            $add_data['create_time'] = $create_time;
            $add_data['update_time'] = $update_time;
            $channel_name_alise = $pinyin->sentence($channel_name);
            $add_data['alise'] = str_replace(' ','',$channel_name_alise);

            $new_id = ChannelLogic::addChannel($add_data);
            if (!$new_id){
                DB::rollBack();
                ApiResponseFactory::apiResponse([],[],516);
            }
            // 维护渠道 计费平台关系
            if ($pay_platform_id){
                $pay_platform_id = explode(',',$pay_platform_id);
                foreach ($pay_platform_id as $kk => $pay_platform_id_value) {
                    $mapping_data = [];
                    $mapping_data['channel_id'] = $new_id;
                    $mapping_data['pay_platform_id'] = $pay_platform_id_value;
                    $result = ChannelLogic::addChannelPaymentMapping($mapping_data);
                    if (!$result) {
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([], [], 517);
                    }
                }
            }

            // 维护渠道 广告平台关系
            if ($ad_platform_id){
                $mapping_data = [];
                $mapping_data['channel_id'] = $new_id;
                $mapping_data['ad_platform_id'] = $ad_platform_id;
                $result = ChannelLogic::addChannelAdPlatformMapping($mapping_data);
                if (!$result){
                    DB::rollBack();
                    ApiResponseFactory::apiResponse([],[],518);
                }
            }
            // 维护渠道 推广平台关系
            if ($generalize_platform_id){
                $mapping_data = [];
                $mapping_data['channel_id'] = $new_id;
                $mapping_data['generalize_platform_id'] = $generalize_platform_id;
                $result = ChannelLogic::addChannelGeneralizeMapping($mapping_data);
                if (!$result){
                    DB::rollBack();
                    ApiResponseFactory::apiResponse([],[],519);
                }
            }
            // 维护渠道分成关系
            if (isset($divide_billing) && isset($divide_ad)){
                $mapping_data = [];
                $mapping_data['app_channel_id'] = $new_id;
                $mapping_data['divide_billing'] = $divide_billing;
                $mapping_data['divide_ad'] = $divide_ad;
                $mapping_data['type'] = 2;
                $mapping_data['create_time'] = $create_time;
                $result = ChannelLogic::addChannelDivideMapping($mapping_data);
                if (!$result){
                    DB::rollBack();
                    ApiResponseFactory::apiResponse([],[],520);
                }
            }

            DB::commit();

            // 保存日志
            OperationLogImp::saveOperationLog(1,4,$new_id);

            ApiResponseFactory::apiResponse($new_id,[]);

        }

    }


}
