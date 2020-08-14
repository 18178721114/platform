<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2018/7/6
 * Time: 下午2:18
 */
namespace App\BusinessLogic;

use App\Models\CApp;
use App\Models\CAppAdPlatform;
use App\Models\CAppAdSlot;
use App\Models\CAppStatistic;
use App\Models\CAppStatisticVersion;
use App\Models\CCustomerCompany;
use App\Models\CDeveloper;
use App\Models\CGeneralize;
use App\Models\CGeneralizeAdApp;
use App\Models\CPlatform;
use App\Models\CPlatformAgenceMapping;
use Illuminate\Support\Facades\DB;
use App\Models\CAppBilling;
use App\Models\CAppBillingPoint;

class ApplicationLogic
{
    /*
     * @desc 默认字段定义
     * @access static
     * */
    static $defaultValve=[];

    // 表格字段名称
    static $tableFieldName = [
        'app_name' => 601,
        'app_full_name' => 602,
        'developer_id' => 603,
        'app_type_id' => 604,
        'company_id' => 605,
        'release_region_id' => 606,
        'os_id' => 607,
        'app_category_id' => 608,
        'online_time' => 609,
        'divide_ad' => 610,
        'divide_billing' => 611,
        'divide_cost' => 612,
        'api_key' => 622,
        'statistic_app_name' => 623,
        'channel_id' => 624,
        'app_version' => 625,
        'statistic_version' => 626,
        'version_release_time' => 627,
        'ad_status' => 628,
        'td_app_id' => 629,
        'app_id' => 617,
        'access_key' => 630,
        'billing_point_name' => 703,
        'billing_point_id' => 704,
        'billing_point_price_usd' => 705,
        'billing_point_price_cny' => 706,
        'app_package_name' => 702,
        'billing_app_name' => 711,
        'billing_app_id' => 712,
        'pay_platform_id' => 713,
    ];

    // 表格字段名称
    static $appAdFieldName = [
        'platform_app_id' =>638,
        'platform_account' =>639,
        'api_key' =>640,
        'currency' =>641,
        'publisher_id' =>642,
        'sdk_key' =>643,
        'app_key' =>644,
        'bundle_id' =>645,
        'access_key' =>646,
        'privkey_pem' =>647,
        'app_signature' =>648,
        'user_id' =>649,
        'user_signature' =>650,
        'system_user_token' =>651,
        'agid' =>652,
        'gdt_app_id' =>653,
        'instance_id' =>654,
        'secret_key' =>655,
        'reward_id' =>656,
        'skey' =>657,
        'account_id' =>658,
        'zone' =>659,
        'ad_slot_id' =>660,
        'video_placement_id' =>661,
        'interstitial_placement_id' =>662,
        'dynamic_id' =>664,
        'app_ad_platform_id' =>664,
        'ad_type' =>818,
        'ad_slot_name' =>819,
    ];

    // 表格字段名称
    static $appGeneralizeFieldName = [
        'application_id' =>671,
        'token' =>672,
        'account_id' =>673,
        'campaign_id' =>674,
        'campaign_name' =>675,
        'internal_suffix' =>676,
        'generalize_time' =>677,
        'campaign_package_name' =>678,
        'generalize_price' =>679,
        'channel_id' => 686,
        'channel_name' => 687,
        'api_key' => 820,
        'user_id' => 821,
        'user_signature' => 822,
        'secret_key' => 823,
        'organization_id' => 824,
    ];

    /**
     *  获取应用信息列表
     */
    public static function getApplicationList($map = [], $fields = '*'){

        $com_obj = DB::table("c_app");
        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }
        if (isset($map["notin"])) {
            $com_obj->whereNotIn($map["notin"][0],$map["notin"][1]);
            unset($map["notin"]);
        }

        if (isset($map["like"])) {
            $like_arr = $map["like"];
            $com_obj->where(function ($query) use($like_arr){
                foreach($like_arr as $like){
                    $query -> orWhere($like[0], $like[1], '%'.$like[2].'%');
                }
            });
            unset($map["like"]);
        }

        if (isset($map["leftjoin"])) {
            foreach ($map["leftjoin"] as $leftjoin){
                $com_obj->leftjoin($leftjoin[0],$leftjoin[1],'=',$leftjoin[2]);
            }
            unset($map["leftjoin"]);
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
     *  创建应用信息
     */
    public static function addApplication($data){

        $com_obj = CApp::create($data);
        return $com_obj->id;
    }

    /**
     *  修改应用信息
     */
    public static function updateApplication($id, $update_data){

        $bool = DB::table("c_app")->where('id',$id)->update($update_data);
        return $bool;
    }


    /**
     *  查询应用二级大类信息
     */
    public static function selectAppGenera($map){

        $info = DB::table("c_app_genera")->where($map)->get();
        return $info;
    }

    /**
     *  创建应用二级大类信息
     */
    public static function insertAppGenera($insert_data){

        $bool = DB::table("c_app_genera")->insert($insert_data);
        return $bool;
    }

    /**
     *  修改应用二级大类信息
     */
    public static function updateAppGenera($map, $update_data){

        $bool = DB::table("c_app_genera")->where($map)->update($update_data);
        return $bool;
    }

    /**
     *  创建统计信息配置
     */
    public static function addStatistic($data){

        $com_obj = CAppStatistic::create($data);
        return $com_obj->id;
    }

    /**
     *  获取应用信息列表
     */
    public static function getStatistic($map = [], $fields = '*'){

        $com_obj = DB::table("c_app_statistic");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            foreach ($map["like"] as $likefilter){
                $com_obj->where($likefilter[0],$likefilter[1],'%'.$likefilter[2].'%');
            }

            unset($map["like"]);
        }

        if (isset($map["leftjoin"])) {
            foreach ($map["leftjoin"] as $leftjoin){
                $com_obj->leftjoin($leftjoin[0],$leftjoin[1],'=',$leftjoin[2]);
            }
            unset($map["leftjoin"]);
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
     *  创建统计信息配置
     */
    public static function addStatisticVersion($data){

        $com_obj = CAppStatisticVersion::create($data);
        return $com_obj->id;
    }

    /**
     *  创建广告信息配置下不变参数信息
     */
    public static function addAppAdPlatform($data){

        $com_obj = CAppAdPlatform::create($data);
        return $com_obj->id;
    }

    /**
     *  创建广告信息配置下可变参数信息
     */
    public static function addAppAdSlot($data){

        $com_obj = CAppAdSlot::create($data);
        return $com_obj->id;
    }

    /**
     *  获取统计信息配置下应用版本信息状态
     */
    public static function getStatisticVersion($map = [], $fields = '*'){

        $com_obj = DB::table("c_app_statistic_version");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            foreach ($map["like"] as $likefilter){
                $com_obj->where($likefilter[0],$likefilter[1],'%'.$likefilter[2].'%');
            }

            unset($map["like"]);
        }

        if (isset($map["leftjoin"])) {
            foreach ($map["leftjoin"] as $leftjoin){
                $com_obj->leftjoin($leftjoin[0],$leftjoin[1],'=',$leftjoin[2]);
            }
            unset($map["leftjoin"]);
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
     *  获取广告信息配置下不可变参数信息
     */
    public static function getAppAdList($map = [], $fields = '*'){

        $com_obj = DB::table("c_app_ad_platform");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            foreach ($map["like"] as $likefilter){
                $com_obj->where($likefilter[0],$likefilter[1],'%'.$likefilter[2].'%');
            }

            unset($map["like"]);
        }

        if (isset($map["leftjoin"])) {
            foreach ($map["leftjoin"] as $leftjoin){
                $com_obj->leftjoin($leftjoin[0],$leftjoin[1],'=',$leftjoin[2]);
            }
            unset($map["leftjoin"]);
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
     *  获取广告信息配置下可变参数信息
     */
    public static function getAppAdSlot($map = [], $fields = '*'){

        $com_obj = DB::table("c_app_ad_slot");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            foreach ($map["like"] as $likefilter){
                $com_obj->where($likefilter[0],$likefilter[1],'%'.$likefilter[2].'%');
            }

            unset($map["like"]);
        }

        if (isset($map["leftjoin"])) {
            foreach ($map["leftjoin"] as $leftjoin){
                $com_obj->leftjoin($leftjoin[0],$leftjoin[1],'=',$leftjoin[2]);
            }
            unset($map["leftjoin"]);
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
     *  创建广告信息配置下不变参数信息
     */
    public static function addAppGeneralize($data){

        $com_obj = CGeneralize::create($data);
        return $com_obj->id;
    }

    /**
     *  创建广告信息配置下可变参数信息
     */
    public static function addAppGeneralizeAd($data){

        $com_obj = CGeneralizeAdApp::create($data);
        return $com_obj->id;
    }


    /**
     *  获取推广信息配置下不可变参数信息
     */
    public static function getAppGeneralizeList($map = [], $fields = '*'){

        $com_obj = DB::table("c_generalize");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            foreach ($map["like"] as $likefilter){
                $com_obj->where($likefilter[0],$likefilter[1],'%'.$likefilter[2].'%');
            }

            unset($map["like"]);
        }

        if (isset($map["leftjoin"])) {
            foreach ($map["leftjoin"] as $leftjoin){
                $com_obj->leftjoin($leftjoin[0],$leftjoin[1],'=',$leftjoin[2]);
            }
            unset($map["leftjoin"]);
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
     *  获取推广信息配置下可变参数信息
     */
    public static function getAppGeneralizeAdList($map = [], $fields = '*'){

        $com_obj = DB::table("c_generalize_ad_app");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            foreach ($map["like"] as $likefilter){
                $com_obj->where($likefilter[0],$likefilter[1],'%'.$likefilter[2].'%');
            }

            unset($map["like"]);
        }

        if (isset($map["leftjoin"])) {
            foreach ($map["leftjoin"] as $leftjoin){
                $com_obj->leftjoin($leftjoin[0],$leftjoin[1],'=',$leftjoin[2]);
            }
            unset($map["leftjoin"]);
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
     *  修改推广信息配置不可变参数信息
     */
    public static function updateAppGeneralize($id, $update_data){

        $bool = DB::table("c_generalize")->where('id',$id)->update($update_data);
        return $bool;
    }

    /**
     *  修改推广信息配置下可变参数信息
     */
    public static function updateAppGeneralizeApp($map = [], $update_data){

        $com_obj = DB::table("c_generalize_ad_app");

        if ($map) {
            $com_obj->where($map);
        }
        return $com_obj->update($update_data);
    }

    /**
     *  修改统计信息配置下应用版本信息状态
     */
    public static function changeStatisticVersionStatus($id, $update_data){

        $bool = DB::table("c_app_statistic_version")->where('id',$id)->update($update_data);
        return $bool;
    }
    /**
     * 获取计费信息列表
     */
    public static function changeStatisticBillingList($map = [], $fields = '*'){
        $com_obj = DB::table("c_billing");

        if (isset($map["leftjoin"])) {
            foreach ($map["leftjoin"] as $leftjoin){
                $com_obj->leftjoin($leftjoin[0],$leftjoin[1],'=',$leftjoin[2]);
            }
            unset($map["leftjoin"]);
        }

        $com_obj = $com_obj->where($map)->select($fields);
        return $com_obj;
    }

    /**
     * 获取计费信息列表
     */
    public static function changeStatisticBillingPoinList($map = [], $fields = '*'){

        $com_obj = DB::table("c_billing_point");

        if (isset($map["orWhere"])) {
            $orWhere_arr = $map["orWhere"];
            $com_obj->where(function ($query) use($orWhere_arr){
                foreach($orWhere_arr as $orWhere){
                    $query -> orWhere($orWhere[0], $orWhere[1]);
                }
            });
            unset($map["orWhere"]);
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
     * 添加计费基础信息
     */
    public static function addBill($data){
        $com_obj = CAppBilling::create($data);
        return $com_obj->id;

    }
        /**
     * 获取计费信息列表
     */
    public static function addBillPoint($data){
        $com_obj = CAppBillingPoint::create($data);
        return $com_obj->id;
    }

    /**
     *  删除计费信息国内基础信息
     */
    public static function changeBillingStatus($id, $update_data){

        $bool = DB::table("c_billing")->where('id',$id)->update($update_data);
        return $bool;
    }

        /**
     *  删除计费信息计费点信息
     */
    public static function changeBillingPointStatus($id, $update_data){

        $bool = DB::table("c_billing_point")->where('id',$id)->update($update_data);
        return $bool;
    }
    /**
     *  删除计费信息计费点信息
     */
    public static function changeAppGeneralizeStatus($id, $update_data)
    {

        $bool = DB::table("c_generalize_ad_app")->where('id', $id)->update($update_data);
        return $bool;
    }

    /**
     *  修改广告信息配置不可变参数信息
     */
    public static function updateAppAdPlatform($id, $update_data){

        $bool = DB::table("c_app_ad_platform")->where('id',$id)->update($update_data);
        return $bool;
    }

    /**
     *  修改广告信息配置下可变参数信息
     */
    public static function updateAppAdSlot($map = [], $update_data){

        $com_obj = DB::table("c_app_ad_slot");

        if ($map) {
            $com_obj->where($map);
        }
        return $com_obj->update($update_data);
    }

}