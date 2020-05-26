<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2018/7/6
 * Time: 下午2:18
 */
namespace App\BusinessLogic;

use App\Models\AppTest;
use Illuminate\Support\Facades\App;
use Illuminate\Support\Facades\DB;
use App\Models\User;
use App\Models\AD;

class DataUploadLogic
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
     *  删除mysql历史数据
     */
    public static function deleteMysqlData($table_name, $map = []){

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

        if ($map) {
            $com_obj->where($map);
        }


        return $com_obj->delete();
    }
    /**
     *  插入mysql数据报错日志信息
     */
    public static function insertMysqlData($table_name,$insert_data){

        $bool = DB::table($table_name)->insert($insert_data);

        return $bool;
    }
        // 获取相关信息
    public static function getMysqlData($table_name,$map = [], $fields = ''){
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

}