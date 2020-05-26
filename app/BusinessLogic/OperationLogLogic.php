<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2018/7/6
 * Time: 下午2:18
 */
namespace App\BusinessLogic;

use App\Models\CCustomerCompany;
use App\Models\COperationLog;
use App\Models\CPlatform;
use App\Models\CPlatformAgenceMapping;
use Illuminate\Support\Facades\DB;

class OperationLogLogic
{
    /*
     * @desc 默认字段定义
     * @access static
     * */
    static $defaultValve=[];

    // 表字段名称
    static $tableFieldName = [
        'customer_id' => '客户公司ID',
        'customer_name' => '客户公司名称',
        'business_manager_id' => '负责人ID',
        'company_id' => '掌游旗下公司ID',
        'developer_email' => '开发者邮箱',
        'pay_platform_id' => '支付平台ID',
        'ad_platform_id' => '广告平台ID',
        'generalize_platform_id' => '推广平台ID',
        'currency_type_id' => '货币类型ID',
        'divide_cost' => '成本分成',
        'divide_ad' => '广告分成',
        'divide_billing' => '计费分成',
        'channel_region' => '渠道区域',
        'channel_type' => '渠道类型',
        'platform_account' => '平台账号信息',
        'agency_platform_id' => '代理平台ID',
        'developer_id' => '开发者ID',
        'app_type_id' => '应用类型ID',
        'release_region_id' => '发行区域ID',
        'os_id' => '操作系统ID',
        'app_category_id' => '应用大类ID',
        'online_time' => '上线时间',
        'effective_date' => '开发者分成生效时间',
        'release_group' => '发行小组',
        'app_full_name' => '应用英文全称',
        'ad_status' => '统计信息配置应用广告状态',
        'platform_type_id' => '平台类型ID',
        'channel_name' => '渠道名称',
    ];

    /**
     *  获取操作日志信息列表
     */
    public static function getOperationLogList($map = [], $fields = '*'){

        $com_obj = DB::table("c_operation_log");

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


    /**
     *  创建操作日志信息
     */
    public static function addOperationLog($data){

        $com_obj = COperationLog::create($data);
        return $com_obj->id;
    }

    /**
     * 获取操作位置 模块名称 菜单名称
     */
    public static function navMenuInfo($map = [], $fields = '*'){
        $com_obj = DB::table("nav_menu_list");

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


}