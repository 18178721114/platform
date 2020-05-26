<?php

namespace App\InterfaceDefine;


interface IDeveloper
{
    /**
     * 开发者列表
     * @param $params array 请求数据
     */
    public static function getDeveloperList($params);

    /**
     * 编辑添加开发者
     * @param $params array 请求数据
     */
    public static function createDeveloper($params);



}
