<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: 下午4:18
 */
namespace App\Http\Controllers\DataPlatform;

use App\BusinessImp\AppsFlyerImp;
use App\Http\Controllers\Controller as Controller;
use Illuminate\Http\Request;

class AppsFlyerController extends Controller
{
    /**
     * 应用列表
     */
    public function getAppsFlyerDeviceList(){
        AppsFlyerImp::getAppsFlyerDeviceList($this->params);
    }

    /**
     * 获取 appsflyer push 数据接口
     */
    public function getAppsflyerPushData(){
        AppsFlyerImp::getAppsflyerPushData($this->params);
    }

}