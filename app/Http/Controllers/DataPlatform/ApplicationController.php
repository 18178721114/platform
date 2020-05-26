<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: 下午4:18
 */
namespace App\Http\Controllers\DataPlatform;

use App\BusinessImp\ApplicationImp;
use App\Http\Controllers\Controller as Controller;
use Illuminate\Http\Request;

class ApplicationController extends Controller
{
    /**
     * 应用列表
     */
    public function getAppList(){
        ApplicationImp::getAppList($this->params);
    }


    /**
     * 编辑添加应用
     */
    public function createApp(){
        ApplicationImp::addApp($this->params);
    }

    /**
     * 统计信息配置列表
     * @param $params array 请求数据
     */
    public function appStatisticList()
    {
        ApplicationImp::appStatisticList($this->params);
    }

    /**
     * 统计信息配置增加 修改
     * @param $params array 请求数据
     */
    public function createAppStatistic()
    {
        ApplicationImp::createAppStatistic($this->params);
    }

    /**
     * 统计信息配置删除
     * @param $params array 请求数据
     */
    public function changeAppStatisticStatus()
    {
        ApplicationImp::changeAppStatisticStatus($this->params);
    }

    /**
     * 广告信息配置列表
     */
    public function appAdList()
    {
        ApplicationImp::appAdList($this->params);
    }

    /**
     * 广告信息配置账号数据
     */
    public function appAdAccountList()
    {
        ApplicationImp::appAdAccountList($this->params);
    }

    /**
     * 广告信息配置增加 修改
     */
    public function createAppAd()
    {
        ApplicationImp::createAppAd($this->params);
    }

    /**
     * 广告信息配置删除
     */
    public function changeAppAdStatus()
    {
        ApplicationImp::changeAppAdStatus($this->params);
    }

    /**
     * 计费信息配置列表
     */
    public function appBillingList()
    {
        ApplicationImp::appBillingList($this->params);
    }

    /**
     * 计费信息配置增加 修改
     */
    public function createAppBilling()
    {
        ApplicationImp::createAppBilling($this->params);
    }

    /**
     * 计费信息配置删除
     */
    public function changeAppBillingStatus()
    {
        ApplicationImp::changeAppBillingStatus($this->params);
    }

        /**
     * 计费信息配置删除
     */
    public function changeAppBillingRebate()
    {
        ApplicationImp::changeAppBillingRebate($this->params);
    }

    /**
     * 推广信息配置列表
     */
    public function appGeneralizeList()
    {
        ApplicationImp::appGeneralizeList($this->params);
    }

    /**
     * 推广信息配置代理平台数据
     */
    public function appGeneralizeAgencyList()
    {
        ApplicationImp::appGeneralizeAgencyList($this->params);
    }

    /**
     * 推广信息配置增加 修改
     */
    public function createAppGeneralize()
    {
        ApplicationImp::createAppGeneralize($this->params);
    }

    /**
     * 推广信息配置删除
     */
    public function changeAppGeneralizeStatus()
    {
        ApplicationImp::changeAppGeneralizeStatus($this->params);
    }

    /**
     * 平台动态参数公共接口
     */
    public function getPlatformConfiglist()
    {
        ApplicationImp::getPlatformConfiglist($this->params);
    }


}