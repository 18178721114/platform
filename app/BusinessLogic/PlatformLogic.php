<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2018/7/6
 * Time: 下午2:18
 */
namespace App\BusinessLogic;

use App\Models\CCustomerCompany;
use App\Models\CPlatform;
use App\Models\CPlatformAccountMapping;
use App\Models\CPlatformAgenceMapping;
use Illuminate\Support\Facades\DB;
use App\Models\VPlatformDataStatus;

class PlatformLogic
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



    /**
     *  获取平台信息列表
     */
    public static function getPlatformList($map = [], $fields = '*'){

        $com_obj = DB::table("c_platform");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
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
     *  获取平台代理信息列表
     */
    public static function getPlatformAccountMapping($map = [], $fields = '*'){

        $com_obj = DB::table("c_platform_account_mapping");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
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
     *  获取平台代理信息列表
     */
    public static function getPlatformAgenceMapping($map = [], $fields = '*'){

        $com_obj = DB::table("c_platform_agency_mapping");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
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
     *  获取平台代理信息列表
     */
    public static function getDistinctPlatformAgenceMapping($map = [], $fields = '*'){

        $com_obj = DB::table("c_platform_agency_mapping");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
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
            $com_obj->distinct()->select($fields);
        }
        return $com_obj;
    }

    /**
     *  获取平台动态字段列表
     */
    public static function getPlatformKeys($map = [], $fields = '*'){

        $com_obj = DB::table("c_dictionary_keys");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
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
     *  获取c_app_ad_platform
     */
    public static function getAppAdPlatform($map = [], $fields = '*'){

        $com_obj = DB::table("c_app_ad_platform");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
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
     *  获取c_app_ad_slot
     */
    public static function getAppAdSlot($map = [], $fields = '*'){

        $com_obj = DB::table("c_app_ad_slot");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
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
     *  创建平台信息
     */
    public static function addPlatform($data){

        $com_obj = CPlatform::create($data);
        return $com_obj->id;
    }

    /**
     *  创建客户公司信息
     */
    public static function createCustomerCompany($data){

        $com_obj = CCustomerCompany::create($data);
        return $com_obj->id;
    }

    /**
     *  创建推广 账号 mapping信息
     */
    public static function createPlatformAccountMapping($data){

        $com_obj = CPlatformAccountMapping::create($data);
        return $com_obj->id;
    }

    /**
     *  创建推广 代理 mapping信息
     */
    public static function addPlatformAgenceMapping($data){

        $com_obj = DB::table('c_platform_agency_mapping')->insert($data);
        return $com_obj;
    }

    /**
     *  删除推广 账号 mapping信息
     */
    public static function deletePlatformAccountMapping($platform_account_ids){

        $com_obj = DB::table("c_platform_account_mapping")->whereIn('id',$platform_account_ids)->delete();;
        return $com_obj;
    }

    /**
     *  删除账号 代理 mapping信息
     */
    public static function deletePlatformAgenceMapping($id){

        $com_obj = DB::table("c_platform_agency_mapping")->where('platform_account_id',$id)->delete();;
        return $com_obj;
    }

    /**
     *  修改平台信息
     */
    public static function updatePlatform($id, $update_data){

        $bool = DB::table("c_platform")->where('id',$id)->update($update_data);
        return $bool;
    }
    /*********************可视化仪表盘**********************************/
    /**
     * 查询平台状态
     */
    public static function getPlatformStatusList($map = [], $fields = '*'){
        $com_obj = DB::table("c_platform_data_status");

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
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
     *  创建平台状态
     */
    public static function createPlatform_status($data){

        $com_obj = VPlatformDataStatus::create($data);
        return $com_obj->id;
    }

    /**
     *  删除平台状态
     */
    public static function delete_platform_status($id){

         $com_obj = DB::table("c_platform_data_status")->where('id',$id)->delete();;
        return $com_obj;
    }

    /**
     * 获取广告report
     *
     */
    public static function getAdReportSum($table_name,$map = [], $fields = '*'){
        $com_obj = DB::table($table_name);

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["like"])) {
            $com_obj->where($map["like"][0],$map["like"][1],'%'.$map["like"][2].'%');
            unset($map["like"]);
        }

        if (isset($map["leftjoin"])) {
            foreach ($map["leftjoin"] as $leftjoin){
                $com_obj->leftjoin($leftjoin[0],$leftjoin[1],'=',$leftjoin[2]);
            }
            unset($map["leftjoin"]);
        }

        if(isset($map["between"])) {
            $com_obj->whereBetween($map["between"][0],$map["between"][1]);
            unset($map["between"]);
        }

        if ($map) {
            $com_obj->where($map);
        }
        if ($fields) {
            $com_obj->select($fields);
        }
        return $com_obj;
    }


    public static function getDailyReportList($table_name,$map = [], $fields = '*'){
        $com_obj = DB::table($table_name);

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

        if(isset($map["between"])) {
            $com_obj->whereBetween($map["between"][0],$map["between"][1]);
            unset($map["between"]);
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