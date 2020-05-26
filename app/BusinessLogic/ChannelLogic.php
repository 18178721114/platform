<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2018/7/6
 * Time: 下午2:18
 */
namespace App\BusinessLogic;

use App\Models\CChannel;
use App\Models\CPlatform;
use App\Models\CPlatformAgenceMapping;
use Illuminate\Support\Facades\DB;

class ChannelLogic
{
    /*
     * @desc 默认字段定义
     * @access static
     * */
    static $defaultValve=[];

    /**
     *  获取渠道信息列表
     */
    public static function getChannelList($map = [], $fields = '*'){

        $com_obj = DB::table("c_channel");

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
     *  获取渠 支付平台mapping信息
     */
    public static function getChannelPaymentMapping($map = [], $fields = '*'){

        $com_obj = DB::table("c_channel_payment_mapping");

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
     *  获取渠 广告平台mapping信息
     */
    public static function getChannelAdPlatformMapping($map = [], $fields = '*'){

        $com_obj = DB::table("c_channel_ad_platform_mapping");

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
     *  获取渠 推广平台mapping信息
     */
    public static function getChannelGeneralizeMapping($map = [], $fields = '*'){

        $com_obj = DB::table("c_channel_generalize_mapping");

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
     *  获取渠 分成信息
     */
    public static function getChannelDivideMapping($map = [], $fields = '*'){

        $com_obj = DB::table("c_divide");

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
     *  创建渠道信息
     */
    public static function addChannel($data){

        $com_obj = CChannel::create($data);
        return $com_obj->id;
    }

    /**
     *  创建渠道 计费平台 mapping信息
     */
    public static function addChannelPaymentMapping($data){

        $com_obj = DB::table('c_channel_payment_mapping')->insert($data);
        return $com_obj;
    }

    /**
     *  创建渠道 广告平台 mapping信息
     */
    public static function addChannelAdPlatformMapping($data){

        $com_obj = DB::table('c_channel_ad_platform_mapping')->insert($data);
        return $com_obj;
    }

    /**
     *  创建渠道 推广平台 mapping信息
     */
    public static function addChannelGeneralizeMapping($data){

        $com_obj = DB::table('c_channel_generalize_mapping')->insert($data);
        return $com_obj;
    }

    /**
     *  创建渠道 分成 mapping信息
     */
    public static function addChannelDivideMapping($data){

        $com_obj = DB::table('c_divide')->insert($data);
        return $com_obj;
    }


    /**
     *  删除渠道 计费平台 mapping信息
     */
    public static function deleteChannelPaymentMapping($id){

        $com_obj = DB::table("c_channel_payment_mapping")->where('channel_id',$id)->delete();;
        return $com_obj;
    }

    /**
     *  删除渠道 广告平台 mapping信息
     */
    public static function deleteChannelAdPlatformMapping($id){

        $com_obj = DB::table("c_channel_ad_platform_mapping")->where('channel_id',$id)->delete();;
        return $com_obj;
    }

    /**
     *  删除渠道 推广 mapping信息
     */
    public static function deleteChannelGeneralizeMapping($id){

        $com_obj = DB::table("c_channel_generalize_mapping")->where('channel_id',$id)->delete();;
        return $com_obj;
    }


    /**
     *  修改渠道信息
     */
    public static function updateChannel($id, $update_data){

        $bool = DB::table("c_channel")->where('id',$id)->update($update_data);
        return $bool;
    }



}