<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2018/7/6
 * Time: 下午2:18
 */
namespace App\BusinessLogic;

use App\Models\CCustomerCompany;
use App\Models\CDeveloper;
use App\Models\CPlatform;
use App\Models\CPlatformAgenceMapping;
use Illuminate\Support\Facades\DB;

class DeveloperLogic
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
     *  获取开发者信息列表
     */
    public static function getDeveloperList($map = [], $fields = '*'){

        $com_obj = DB::table("c_developer");

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
     *  创建开发者信息
     */
    public static function addDeveloper($data){

        $com_obj = CDeveloper::create($data);
        return $com_obj->id;
    }

    /**
     *  修改开发者信息
     */
    public static function updateDeveloper($id, $update_data){

        $bool = DB::table("c_developer")->where('id',$id)->update($update_data);
        return $bool;
    }



}