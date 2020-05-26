<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: 脧脗脦莽4:18
 */
namespace App\Http\Controllers\DataPlatform;

use App\BusinessImp\CommonImp;
use App\BusinessImp\PlatformImp;
use App\Http\Controllers\Controller as Controller;
use Illuminate\Http\Request;

class PlatformController extends Controller
{
    /**
     * 获取平台列表
     * @param $params array
     */
    public function getPlatformList(){
        PlatformImp::getPlatformList($this->params);
    }

    /**
     * 创建平台
     * @param $params array
     */
    public function createPlatform(){
        PlatformImp::createPlatform($this->params);
    }

    /**
     * 平台取数状态
     * @param $params array 请求数据
     */
    public function platformDataStatus(){
        PlatformImp::platformDataStatus($this->params);
    }

    /**
     * 平台取数信息详情
     * @param $params array 请求数据
     */
    public function platformDataDetails(){
        PlatformImp::platformDataDetails($this->params);
    }


}