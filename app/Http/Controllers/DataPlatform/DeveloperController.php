<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: 下午4:18
 */
namespace App\Http\Controllers\DataPlatform;

use App\BusinessImp\CommonImp;
use App\BusinessImp\DeveloperImp;
use App\BusinessImp\PlatformImp;
use App\Http\Controllers\Controller as Controller;
use Illuminate\Http\Request;

class DeveloperController extends Controller
{
    /**
     * 开发者列表
     * @param $params array 请求数据
     */
    public function getDeveloperList(){
        DeveloperImp::getDeveloperList($this->params);
    }


    /**
     * 编辑添加开发者
     * @param $params array 请求数据
     */
    public function createDeveloper(){
        DeveloperImp::createDeveloper($this->params);
    }

    /**
     * 开发者平台应用控制列表接口
     * @param $params array 请求数据
     */
    public function developAppList(){
        DeveloperImp::developAppList($this->params);
    }

    /**
     * 开发者平台应用控制状态修改接口
     * @param $params array 请求数据
     */
    public function developAppStatus(){
        DeveloperImp::developAppStatus($this->params);
    }

    /**
     * 开发者平台用户注册接口
     * @param $params array 请求数据
     */
    public function developUserRegister(){
        DeveloperImp::developUserRegister($this->params);
    }


}