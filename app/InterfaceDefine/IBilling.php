<?php

namespace App\InterfaceDefine;


interface IBilling
{
    /**
     * 计费点列表
     * @param $params array 请求数据
     */
    public static function getBillingList($params);

    /**
     * 添加编辑计费点
     * @param $params array 请求数据
     */
    public static function createBillingConf($params);

    /**
     * 删除计费点
     * @param $params array 请求数据
     */
    public static function deleteBillingConf($params);

}
