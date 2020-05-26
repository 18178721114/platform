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

class TestLogic
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
     *  测试数据库操作
     */
    public static function appAdd($adgroup_info){
        $adgroup = AppTest::create($adgroup_info);
        return $adgroup->id;
    }

}