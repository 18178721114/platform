<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * 
 */
namespace App\Http\Controllers\DataPlatform;
use App\BusinessImp\CommonImp;
use App\BusinessImp\DataImportImp;
use App\BusinessImp\SysInItImp;
use App\BusinessImp\UserImp;
use App\Http\Controllers\Controller as Controller;
use Illuminate\Http\Request;
class DataImportController extends Controller
{

    /**
     * 获取渠道数据最后时间
     * @param $params array 请求数据
     */
    public function getChannelDataTime()
    {
        DataImportImp::getChannelDataTime($this->params);
    }

    /**
     * 渠道数据导入
     * @param $params array 请求数据
     */
    public function importChannelData()
    {
        DataImportImp::importChannelData($this->params);
    }

    /**
     * 数据报错日志列表
     * @param $params array 请求数据
     */
    public function getDateErrorLog(){
        DataImportImp::getDateErrorLog($this->params);
    }

    /**
     * 数据报错日志处理状态修改
     * @param $params array 请求数据
     */
    public function changeErrorStatus(){
        DataImportImp::changeErrorStatus($this->params);
    }

    /**
     * 配置报错
     * @param $params array 请求数据
     */
    public function getConfigErrorLog(){
        DataImportImp::getConfigErrorLog($this->params);
    }

}