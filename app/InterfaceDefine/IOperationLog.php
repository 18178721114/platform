<?php

namespace App\InterfaceDefine;


interface IOperationLog
{
    /**
     * 操作日志列表
     * @param $params array 请求数据
     */
    public static function getOperationLogList($params);

}
