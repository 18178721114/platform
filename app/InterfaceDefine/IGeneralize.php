<?php

namespace App\InterfaceDefine;


interface IGeneralize
{
    /**
     * 推广平台信息初始化
     * @param $params array 请求数据
     */
    public static function getGeneralizeList($params);

    /**
     * 添加推广信息配置
     * @param $params array 请求数据
     */
    public static function addGeneralizeConf($params);

    /**
     * 删除推广信息配置
     * @param $params array 请求数据
     */
    public static function deleteGeneralizeConf($params);

}
