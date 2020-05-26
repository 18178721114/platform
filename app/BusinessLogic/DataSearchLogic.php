<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2018/7/6
 * Time: 下午2:18
 */
namespace App\BusinessLogic;

use App\Common\Service;
use App\Models\AppTest;
use Illuminate\Support\Facades\App;
use Illuminate\Support\Facades\DB;
use App\Models\User;
use App\Models\AD;

class DataSearchLogic
{
    /*
     * @desc 默认字段定义
     * @access static
     * */
    static $defaultValve=[

    ];

     // 表格字段名称
    static $tableFieldName = [

    ];

    /**
     *  获取数据报错日志列表
     */
    public static function getDataErrorList($schema,$table_name,$map = [], $fields = '*'){

        $com_obj = DB::connection('pgsql')->table($schema.'.'.$table_name);

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["or_in"])) {
            $com_obj->orWhereIn($map["or_in"][0],$map["or_in"][1]);
            unset($map["or_in"]);
        }

        if(isset($map["between"])) {
            $com_obj->whereBetween($map["between"][0],$map["between"][1]);
            unset($map["between"]);
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

    // 默认
    public static function get_default_select($type,$currency_type_id){
        $default_selectid_info =  DB::table('s_cfg_select_dim')->select(['dim_id','dim_name','dim_type','dim_table','dim_order','dim_value','dim_table_id','dim_cfg'])->where(['dim_default' => 1 ,'dim_type' =>$type,'currency_type'=>$currency_type_id])->get();
        return $default_selectid_info;
    }

    /**
     * @desc 用户查询权限
     * @param int $user_id
     * @param  $dimension 权限字段
     * @return multitype:Ambigous <unknown>
     */
    public static function searchPower($user_id,$dimension=''){
        if(!$dimension){
            $powerData = DB::table('s_cfg_user_select')->select(['dim','dim_value'])->where('user_id',$user_id)->get();
        }else {
            $powerData = DB::table('s_cfg_user_select')->select(['dim','dim_value'])->where(['user_id'=>$user_id,  'dim'=>$dimension])->get();
        }
        $powerList = array();
        foreach($powerData as $power){
            $powerList[] = $power['dim_value'];
        }
        return $powerList;
    }

    /**
     * @desc 根据公司名字返回sql拼接字符串
     * @param string $company
     */
    public static function getCompanyPowerSql($company,$tableName){
        $sqlStr = '';
        if($company){
            if($company!='-2'){//-2全部
                if($company == 9)
                    $sqlStr .= "  and {$tableName}.game_creator = 9 ";
                if($company == 1 )
                    $sqlStr .="  and {$tableName}.game_creator != 9 ";
            }
        }
        return $sqlStr;
    }

    /**
     * @desc 获取对应关系
     *
     */
    public static function getRelation($relationId){
        $relationList = DB::table('s_cfg_select_compute')->select(['compute_name','compute_code','compute_id','compute_connect'])->where('compute_id',$relationId)->first();
        return $relationList;
    }
    /*
     * @desc 获取某一维度信息
     */
    public static function getDimension($dimensionId,$currency_type_id){
        $dimensionList = DB::table('s_cfg_select_dim')->select(['dim_id','dim_table_id','dim_table','dim_value','dim_type'])->where(['dim_id'=>$dimensionId,'currency_type' => $currency_type_id])->first();
        return $dimensionList;
    }

    //获得所有的维度信息 根据维度类型
    public static function getDimensionList($type,$currency_type_id){
        $dimensionList = DB::table('s_cfg_select_dim')->select(['dim_id','dim_table_id','dim_table','dim_value'])->where(['dim_type'=>$type,'currency_type' => $currency_type_id])->orderBy('dim_order')->get();
        $dimensionList = Service::data($dimensionList);
        $dim = array();
        foreach($dimensionList as $dimension){
            $dim[] = "{$dimension['dim_value']} as {$dimension['dim_table_id']}";
        }
        $dimList = implode(',',$dim);
        return array('dimList'=>$dimList,'dimArr'=>$dimensionList);
    }

    /*
	 * @desc 获取某一维度信息
	*/
    public static function getDimensionByTableId($table_id,$currency_type_id){
        $dimensionList = DB::table('s_cfg_select_dim')->select(['dim_id','dim_table_id','dim_table','dim_value','dim_type'])->where(['dim_table_id'=>$table_id,'currency_type' => $currency_type_id])->first();
        return $dimensionList;
    }

    public static function searchCondition($uid,$company){
        if($company){
            $where = self::getCompanyPowerSql($company, 's_search_custom');
        }else $where = '';
        $sql = "select search_name,id,user_id,search_condition from s_search_custom where user_id = {$uid} {$where}";
        $data =DB::select($sql);
        return $data;
    }

    // 获取相关信息
    public static function getBasicListInfo($table_name,$map = [], $fields = ''){
        $com_obj = DB::table($table_name);

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["or_in"])) {
            $com_obj->orWhereIn($map["or_in"][0],$map["or_in"][1]);
            unset($map["or_in"]);
        }

        if(isset($map["between"])) {
            $com_obj->whereBetween($map["between"][0],$map["between"][1]);
            unset($map["between"]);
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

    // 获取首页查询应用相关信息
    public static function getIndexAppList($currency_type_id,$map = [], $fields = ''){
        $com_obj = DB::table('s_basic_data_homepage');
        if ($currency_type_id == 60){
            $com_obj = DB::table('s_basic_data_homepage_usd');
        }

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["not_in"])) {
            $com_obj->whereNotIn($map["not_in"][0],$map["not_in"][1]);
            unset($map["not_in"]);
        }

        if(isset($map["between"])) {
            $com_obj->whereBetween($map["between"][0],$map["between"][1]);
            unset($map["between"]);
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

    public static function getCustomSortInfo($dim_id,$currency_type_id){
        $customSortInfo = DB::table('s_cfg_select_dim')->select('dim_id','dim_decimals')->where(['dim_id' => $dim_id,'currency_type'=>$currency_type_id])->first();
        return $customSortInfo;
    }

    //指标定制数据
    public static function getTargetCustom($user_id,$type){
        $customInfo = DB::table('s_homepage_custom')->select("{$type}","user_id")->where('user_id',$user_id)->first();
        $customInfo = Service::data($customInfo);
        if(!$customInfo[$type])
            $customInfo = DB::table('s_homepage_custom')->select("{$type}","user_id")->where('user_id',$user_id)->first();
        $customInfo = Service::data($customInfo);
        if ($customInfo){
            return $customInfo[$type] ? explode(',',$customInfo[$type]) : [];
        }else{
            return [];
        }
    }
}