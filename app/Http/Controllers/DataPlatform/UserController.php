<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * 
 */
namespace App\Http\Controllers\DataPlatform;
use App\BusinessImp\CommonImp;
use App\BusinessImp\SysInItImp;
use App\BusinessImp\UserImp;
use App\Http\Controllers\Controller as Controller;
use Illuminate\Http\Request;
class UserController extends Controller
{
    /**
     * 用户登录
     */
    public  function login()
    {
        UserImp::login($this->params);
    }
    /**
     * 用户列表
     */
    public  function list()
    {
        UserImp::list($this->params);
    }
    /**
     * 用户创建和修改
     */
    public  function create()
    {
        UserImp::create($this->params);
    }
    /**
     * 用户修改状态
     */
    public  function editStatus()
    {
        UserImp::editStatus($this->params);
    }
    /**
     * 修改用户权限
     */
    public  function editRole()
    {
        UserImp::editRole($this->params);
    }

        /**
     * 获取所有用户
     */
    public  function wholeUserList()
    {
        UserImp::wholeUserList($this->params);
    }


}