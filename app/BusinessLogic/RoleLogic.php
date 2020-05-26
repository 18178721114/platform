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
use App\Models\Role;

class RoleLogic
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
        'role_name' => 730,
        'department_id' => 731,
        'permission' => 732,
        
    ];
     /**
     *  检查账号
     */
    public static function Rolelist($map = [], $fields = '*'){
               $com_obj = DB::table("role");

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

    /**
     *  检查账号
     */
    public static function RoleAdd($adgroup_info){
        $adgroup = Role::create($adgroup_info);
        return $adgroup->id;
    }
    /**
     *  检查账号是否唯一
     */
    public static function checkRoleAccount($map = [], $fields = '*'){
        $com_obj = DB::table("role");
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
    public static function RoleEdit($id, $update_data){
        $bool = DB::table("role")->where('id',$id)->update($update_data);
        return $bool;
    }
    //应用权限list
    public static function permissionList($map = [], $fields = '*'){
        $com_obj = DB::table("nav_menu_list");
        if ($map) {
            $com_obj->where($map);
        }
        if ($fields) {
            $com_obj->select($fields);
        }
        return $com_obj;

    }


}