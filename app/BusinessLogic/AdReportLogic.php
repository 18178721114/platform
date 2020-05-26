<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2018/7/6
 * Time: 下午2:18
 */
namespace App\BusinessLogic;

use App\Models\AdReportData;
use Illuminate\Support\Facades\App;
use Illuminate\Support\Facades\DB;

class AdReportLogic
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

    /**
     *  zplayads 数据
     */
    public static function insertAdStats($ad_stats_list){
        $result = AdReportData::insert($ad_stats_list);
        return $result;
    }

}