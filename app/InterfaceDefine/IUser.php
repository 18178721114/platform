<?php

namespace App\InterfaceDefine;


interface IUser
{
    /**
     * 用户登录
     * @param $params array 请求数据
     */
    public static function login($params);
    /**
     * 用户列表
     * @param $params array 请求数据
     */
    public static function list($params);
    /**
     * 添加用户
     * @param $params array 请求数据
     */
    public static function create($params);
    /**
     * 修改用户状态
     * @param $params array 请求数据
     */
    public static function editStatus($params);
    /**
     * 修改用户权限
     * @param $params array 请求数据
     */
    public static function editRole($params);
        /**
     * 获取所有用户
     * @param $params array 请求数据
     */
    public static function wholeUserList($params);
    

}
