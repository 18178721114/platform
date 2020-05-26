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
use App\BusinessImp\RoleImp;
use App\Http\Controllers\Controller as Controller;
use Illuminate\Http\Request;
class RoleController extends Controller
{

    /**
     * 角色列表
     */
    public  function RoleList()
    {
        RoleImp::RoleList($this->params);
    }
    /**
     * 角色创建和修改
     */
    public  function RoleCreate()
    {
        RoleImp::RoleCreate($this->params);
    }
    

}