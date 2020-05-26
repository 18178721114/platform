<?php

namespace App\InterfaceDefine;


interface IStatistic
{
    /**
     * 统计信息初始化
     * @param $params array 请求数据
     */
    public static function getStatisticList($params);

    /**
     * 添加编辑版本信息
     * @param $params array 请求数据
     */
    public static function createStatisticConf($params);

    /**
     * 删除版本信息
     * @param $params array 请求数据
     */
    public static function deleteVersionConf($params);

}
