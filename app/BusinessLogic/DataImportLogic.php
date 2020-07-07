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

class DataImportLogic
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
     *  检查历史数据
     */
    public static function getChannelData($schema,$table_name,$map = [], $fields = '*'){

        $com_obj = DB::connection('pgsql')->table($schema.'.'.$table_name);

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["or_in"])) {
            $com_obj->orWhereIn($map["or_in"][0],$map["or_in"][1]);
            unset($map["or_in"]);
        }

        if (isset($map["like"])) {
            foreach ($map["like"] as $likefilter){
                $com_obj->where($likefilter[0],$likefilter[1],'%'.$likefilter[2].'%');
            }

            unset($map["like"]);
        }
        if (isset($map["notlike"])) {
            foreach ($map["notlike"] as $likefilter){
                $com_obj->where($likefilter[0],$likefilter[1],'%'.$likefilter[2].'%');
            }

            unset($map["notlike"]);
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


    /**
     *  删除pgsql历史数据
     */
    public static function deleteHistoryData($schema,$table_name,$map = []){

        $com_obj = DB::connection('pgsql')->table($schema.'.'.$table_name);

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["or_in"])) {
            $com_obj->orWhereIn($map["or_in"][0],$map["or_in"][1]);
            unset($map["or_in"]);
        }

        if ($map) {
            $com_obj->where($map);
        }


        return $com_obj->delete();
    }

    /**
     *  删除mysql历史数据
     */
    public static function deleteMysqlHistoryData($table_name, $map = []){

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
     *  插入pgsq数据报错日志信息
     */
    public static function insertDataErrorLog($schema,$table_name,$insert_data){

        $bool = DB::connection('pgsql')->table($schema.'.'.$table_name)->insert($insert_data);
          
        return $bool;
    }

    /**
     *  插入mysql数据报错日志信息
     */
    public static function insertMysqlDataErrorLog($insert_data){

        $bool = DB::table('api_data_error')->insert($insert_data);

        return $bool;
    }

    /**
     *  插入渠道数据
     */
    public static function insertChannelData($schema,$table_name,$insert_data){

        $bool = DB::connection('pgsql')->table($schema.'.'.$table_name)->insert($insert_data);

        return $bool;
    }

    /**
     *  插入渠道数据
     */
    public static function insertMysqlChannelData($table_name,$insert_data){

        $bool = DB::table($table_name)->insert($insert_data);

        return $bool;
    }

    /**
     *  删除msyql历史数据
     */
    public static function deleteGdtMysqlData($map = []){

        // 上线需打开
        $com_obj = DB::connection('mysql_179')->table('gdt_tg');
//        $com_obj = DB::table('gdt_tg');

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["or_in"])) {
            $com_obj->orWhereIn($map["or_in"][0],$map["or_in"][1]);
            unset($map["or_in"]);
        }

        if ($map) {
            $com_obj->where($map);
        }

        return $com_obj->delete();
    }

    /**
     *  插入mysql gdt数据
     */
    public static function insertGdtMysqlData($insert_data){

        // 上线需打开
        $bool = DB::connection('mysql_179')->table('gdt_tg')->insert($insert_data);
//        $bool = DB::table('gdt_tg')->insert($insert_data);
        return $bool;
    }

    /**
     *  删除kuaishou msyql历史数据
     */
    public static function deleteKsMysqlData($map = []){

        // 上线需打开
        $com_obj = DB::connection('mysql_179')->table('kuaishou_tg');
//        $com_obj = DB::table('gdt_tg');

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["or_in"])) {
            $com_obj->orWhereIn($map["or_in"][0],$map["or_in"][1]);
            unset($map["or_in"]);
        }

        if ($map) {
            $com_obj->where($map);
        }

        return $com_obj->delete();
    }

    /**
     *  修改错误信息状态
     */
    public static function updateErrorStatus($table_name,$map,$updata){

        $com_obj = DB::connection('pgsql')->table($table_name);

        if (isset($map["in"])) {
            $com_obj->whereIn($map["in"][0],$map["in"][1]);
            unset($map["in"]);
        }

        if (isset($map["or_in"])) {
            $com_obj->orWhereIn($map["or_in"][0],$map["or_in"][1]);
            unset($map["or_in"]);
        }

        if ($map) {
            $com_obj->where($map);
        }


        return $com_obj->update($updata);
    }

    /**
     *  插入kuaishou mysql gdt数据
     */
    public static function insertKuaishouMysqlData($insert_data){

        // 上线需打开
        $bool = DB::connection('mysql_179')->table('kuaishou_tg')->insert($insert_data);
//        $bool = DB::table('gdt_tg')->insert($insert_data);
        return $bool;
    }

    // 获取配置信息
    public static function getConf($platformId){
        if($platformId=='10010'||$platformId=='10051') {
            $sql = "select distinct company_account,company_username,company_pass,key_list,secret_key from platform_conf  where platform_id={$platformId}  GROUP BY company_account,company_username,company_pass,key_list,secret_key";
        } elseif ($platformId=='10064') {
            $sql = "SELECT company_account, company_username, company_pass, '' AS key_list, '' AS secret_key, param_key FROM platform_conf WHERE platform_id = {$platformId} GROUP BY company_account, company_username, company_pass, param_key";
        }else if($platformId == '10007'){
            $sql = "select distinct application_id,company_account,secret_key from platform_conf where platform_id={$platformId} and application_id!='' and secret_key!='' ";
        }else {
            $sql = "select platform_id,company_account,company_username,company_pass,application_name,application_id,adspace_id,adspace_name,param_key,key_list,secret_key from platform_conf  where platform_id={$platformId}";
        }

        return DB::select($sql);
    }


    public static function ad_creat($data){
        $com_obj = AD::create($data);
        return $com_obj->id;
    }

    // 保存处理过程最终数据结果
    public static function insertAdReportInfo($table_name, $insert_data){
        $bool = DB::table($table_name)->insert($insert_data);
        return $bool;
    }
    public static function insertAdReportInfoDatabase($database,$table_name, $insert_data){
        $bool = DB::connection($database)->table($table_name)->insert($insert_data);
        return $bool;
    }

    // 删除 移动基地 日付数据
    public static function deleteOperatorsData($table_name, $map = []){

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

    // 插入 移动基地 日付数据
    public static function insertOperatorsData($table_name,$insert_data){

        $bool = DB::table($table_name)->insert($insert_data);
        return $bool;
    }




}