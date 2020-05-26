<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2018/7/6
 * Time: 下午2:18
 */
namespace App\BusinessLogic;

use Illuminate\Support\Facades\DB;

class CommonLogic
{
    /*
     * @desc 默认字段定义
     * @access static
     * */
    static $defaultValve=[];


    //  $map['between'] = ['day',[ $start_time, $end_time]];
//        if (isset($map["between"])) {
//            $com_obj->whereBetween($map["between"][0], $map["between"][1]);
//            unset($map["between"]);
//        }

//        $com_obj->leftJoin('user_rooms', function ($join) {
//            $join->on('user_rooms.user_uuid', '=', 'chat_info.user_uuid')
//                ->on('user_rooms.room_uuid', '=', 'chat_info.room_uuid');
//        })

    /**
     *  获取货币列表接口
     */
    public static function getCurrencyList($map = [], $fields = '*'){

        $com_obj = DB::table("c_currency_type");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
            unset($map["like"]);
        }

        if ($map) {
            $com_obj->where($map);
        }
        if ($fields) {
            $com_obj->select($fields);
        }
        return $com_obj;
    }

    /**
     *  获取掌游旗下公司列表接口
     */
    public static function getCompanyList($map = [], $fields = '*'){

        $com_obj = DB::table("c_zplay_company");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
            unset($map["like"]);
        }

        if ($map) {
            $com_obj->where($map);
        }
        if ($fields) {
            $com_obj->select($fields);
        }
        return $com_obj;
    }

    /**
     *  获取应用大类列表接口
     */
    public static function getAppCategoryList($map = [], $fields = '*'){

        $com_obj = DB::table("c_app_category");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
            unset($map["like"]);
        }

        if ($map) {
            $com_obj->where($map);
        }
        if ($fields) {
            $com_obj->select($fields);
        }
        return $com_obj;
    }

    /**
     *  获取广告类型 广告形式
     */
    public static function getAdTypeList($map = [], $fields = '*'){

        $com_obj = DB::table("c_ad_type");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
            unset($map["like"]);
        }

        if ($map) {
            $com_obj->where($map);
        }
        if ($fields) {
            $com_obj->select($fields);
        }
        return $com_obj;
    }

    /**
     *  获取负责人信息
     */
    public static function getBusinessManagerList($map = [], $fields = '*'){

        $com_obj = DB::table("c_business_manager");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
            unset($map["like"]);
        }

        if ($map) {
            $com_obj->where($map);
        }
        if ($fields) {
            $com_obj->select($fields);
        }
        return $com_obj;
    }

    /**
     *  获取平台类型
     */
    public static function getPlatformTypeList($map = [], $fields = '*'){

        $com_obj = DB::table("c_platform_type");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
            unset($map["like"]);
        }

        if ($map) {
            $com_obj->where($map);
        }
        if ($fields) {
            $com_obj->select($fields);
        }
        return $com_obj;
    }

    /**
     *  获取客户公司
     */
    public static function getCustomerCompanyList($map = [], $fields = '*'){

        $com_obj = DB::table("c_customer_company");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
            unset($map["like"]);
        }

        if ($map) {
            $com_obj->where($map);
        }
        if ($fields) {
            $com_obj->select($fields);
        }
        return $com_obj;
    }

    /**
     *  获取支付方式
     */
    public static function getPaymentMethodList($map = [], $fields = '*'){

        $com_obj = DB::table("c_payment_method");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
            unset($map["like"]);
        }

        if ($map) {
            $com_obj->where($map);
        }
        if ($fields) {
            $com_obj->select($fields);
        }
        return $com_obj;
    }

    /**
     *  获取计费平台 支付平台
     */
    public static function getPayPlatformList($map = [], $fields = '*'){

        $com_obj = DB::table("c_platform");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
            unset($map["like"]);
        }

        if ($map) {
            $com_obj->where($map);
        }
        if ($fields) {
            $com_obj->select($fields);
        }
        return $com_obj;
    }
        /**
     *  部门
     */
    public static function getDepartmentList($map = [], $fields = '*'){

        $com_obj = DB::table("department");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
            unset($map["like"]);
        }

        if ($map) {
            $com_obj->where($map);
        }
        if ($fields) {
            $com_obj->select($fields);
        }
        return $com_obj;
    }
       /**
     *  部门
     */
    public static function getCountryList($map = [], $fields = '*'){

        $com_obj = DB::table("c_country_corresponding");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
            unset($map["like"]);
        }

        if ($map) {
            $com_obj->where($map);
        }
        if ($fields) {
            $com_obj->select($fields);
        }
        return $com_obj;
    }

           /**
     *  部门
     */
    public static function getAdTypeCorrespondingList($map = [], $fields = '*'){

        $com_obj = DB::table("c_ad_type_corresponding");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
            unset($map["like"]);
        }

        if ($map) {
            $com_obj->where($map);
        }
        if ($fields) {
            $com_obj->select($fields);
        }
        return $com_obj;
    }
            /**
     *  部门
     */
    public static function getCurrencyEXList($map = [], $fields = '*'){

        $com_obj = DB::table("c_currency_ex");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
            unset($map["like"]);
        }

        if ($map) {
            $com_obj->where($map);
        }
        if ($fields) {
            $com_obj->select($fields);
        }
        return $com_obj;
    }

    /**
     *  部门
     */
    public static function getCurrencyType($map = [], $fields = '*'){

        $com_obj = DB::table("c_currency_type");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
            unset($map["like"]);
        }

        if ($map) {
            $com_obj->where($map);
        }
        if ($fields) {
            $com_obj->select($fields);
        }
        return $com_obj;
    }

    /**
     *  平均启动时长
     */
    public static function getChannelAvgSessionLength($map = [], $fields = '*'){

        $com_obj = DB::table("talkingdata_china_session");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
            unset($map["like"]);
        }

        if ($map) {
            $com_obj->where($map);
        }
        if ($fields) {
            $com_obj->select($fields);
        }
        return $com_obj;
    }


        /**
     *  获取国家
     */
    public static function getTgCountryList($map = [], $fields = '*'){

        $com_obj = DB::table("c_country_standard");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
            unset($map["like"]);
        }

        if ($map) {
            $com_obj->where($map);
        }
        if ($fields) {
            $com_obj->select($fields);
        }
        return $com_obj;
    }

    /**
     *  获取国家
     */
    public static function getAllDeveloperList($map = [], $fields = '*'){

        $com_obj = DB::table("c_developer");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
            unset($map["like"]);
        }

        if ($map) {
            $com_obj->where($map);
        }
        if ($fields) {
            $com_obj->select($fields);
        }
        return $com_obj;
    }
}