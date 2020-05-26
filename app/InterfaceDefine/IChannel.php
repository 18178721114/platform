<?php

namespace App\InterfaceDefine;


interface IChannel
{
    /**
     * 渠道列表
     * @param $params array 请求数据
     */
    public static function getChannelList($params);

    /**
     * 编辑添加渠道
     * @param $params array 请求数据
     */
    public static function createChannel($params);



}
