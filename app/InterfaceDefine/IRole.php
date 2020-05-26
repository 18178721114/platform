<?php

namespace App\InterfaceDefine;


interface IRole
{
    
    /**
     * 角色列表
     * @param $params array 请求数据
     */
    public static function RoleList($params);
    /**
     * 角色
     * @param $params array 请求数据
     */
    public static function RoleCreate($params);
   

}
