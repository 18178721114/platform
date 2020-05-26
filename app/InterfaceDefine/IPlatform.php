<?php

namespace App\InterfaceDefine;


interface IPlatform
{
    /**
     * 平台列表
     * @param $params array 请求数据
     */
    public static function getPlatformList($params);

    /**
     * 编辑添加平台
     * @param $params array 请求数据
     */
    public static function createPlatform($params);



}
