<?php

namespace App\InterfaceDefine;


interface IDataImport
{
    /**
     * 获取渠道数据最后时间
     * @param $params array 请求数据
     */
    public static function getChannelDataTime($params);


    /**
     * 渠道数据导入
     * @param $params array 请求数据
     */
    public static function importChannelData($params);

    /**
     * 数据日志列表
     * @param $params array 请求数据
     */
    public static function getDateErrorLog($params);


}
