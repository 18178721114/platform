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

class UserLogic
{
    /*
     * @desc 默认字段定义
     * @access static
     * */
    static $defaultValve=[
        "remark" => '', // 其他
        "create_time" => 0, // 创建时间
        "update_time" => 0, // 更新时间
    ];

     // 表格字段名称
    static $tableFieldName = [
        'name' => 722,
        'user_account' => 720,
        'type' => 728,
        'company_id' => 729,
    ];

    /**
     *  检查账号
     */
    public static function userAdd($adgroup_info){
        $adgroup = User::create($adgroup_info);
        return $adgroup->id;
    }
    /**
     *  检查账号是否唯一
     */
    public static function checkUserAccount($map = [], $fields = '*'){
        $com_obj = DB::table("user");
        if ($map) {
            $com_obj->where($map);
        }
        if ($fields) {
            $com_obj->select($fields);
        }
        return $com_obj;
    }
     /**
     *  修改用户信息
     */
    public static function userEdit($id, $update_data){

        $bool = DB::table("user")->where('id',$id)->update($update_data);
        return $bool;
    }
    //获取权限list
    public static function appList($map = [], $fields = '*'){
        $com_obj = DB::table("c_app");
        if ($map) {
            $com_obj->where($map);
        }
        if ($fields) {
            $com_obj->select($fields);
        }
        return $com_obj;

    }
    /**
     *  检查账号
     */
    public static function Userlist($map = [], $fields = '*'){
               $com_obj = DB::table("user");

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

        if (isset($map["orlike"])) {
            foreach ($map["orlike"] as $likefilter){
                $com_obj->orwhere($likefilter[0],$likefilter[1],'%'.$likefilter[2].'%');
            }

            unset($map["orlike"]);
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