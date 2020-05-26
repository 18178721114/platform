<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: ����4:18
 */
namespace App\Http\Controllers\DataPlatform;

use App\BusinessImp\CommonImp;
use App\Http\Controllers\Controller as Controller;
use Illuminate\Http\Request;

class CommonController extends Controller
{

    /**
     * 货币列表
     */
    public  function getCurrencyList()
    {
        CommonImp::getCurrencyList($this->params);
    }

    /**
     * 掌游公司列表
     */
    public function getZplayCompanyList()
    {
        CommonImp::getZplayCompanyList($this->params);
    }

    /**
     * 游戏大类
     */
    public function getAppCategoryList()
    {
        CommonImp::getAppCategoryList($this->params);
    }

    /**
     *  广告类型
     */
    public function getAdTypeList()
    {
        CommonImp::getAdTypeList($this->params);
    }

    /**
     * 负责人
     */
    public function getBusinessManagerList()
    {
        CommonImp::getBusinessManagerList($this->params);
    }

    /**
     * 平台类型
     */
    public function getPlatformTypeList()
    {
        CommonImp::getPlatformTypeList($this->params);
    }

    /**
     * 客户佛你公司
     */
    public function getCustomerCompanyList()
    {
        CommonImp::getCustomerCompanyList($this->params);
    }

    /**
     * ֧支付方式
     */
    public function getPaymentMethodList()
    {
        CommonImp::getPaymentMethodList($this->params);
    }

    /**
     * 广告
     */
    public function getAdMethodList()
    {
        CommonImp::getAdMethodList($this->params);
    }

    /**
     * 统计
     */
    public function getTjMethodList()
    {
        CommonImp::getTjMethodList($this->params);
    }

    /**
     * 推广
     */
    public function getTgMethodList()
    {
        CommonImp::getTgMethodList($this->params);
    }

    /**
     * 支付方式
     */
    public function getDlMethodList()
    {
        CommonImp::getDlMethodList($this->params);
    }

    /**
     * 部门
     */
    public function getDepartmentList()
    {
        CommonImp::getDepartmentList($this->params);
    }

    /**
     * 权限
     */
    public function getfunctionPermissionList()
    {
        CommonImp::getfunctionPermissionList($this->params);
    }

    /**
     * 菜单
     */
    public function getMenuList()
    {
        CommonImp::getMenuList($this->params);
    }

    /**
     * 用户权限下应用列表
     */
    public function getUserAppList()
    {
        CommonImp::getUserAppList($this->params);
    }

     /**
     * 国家
     */
    public function getCountryList()
    {
        CommonImp::getCountryList($this->params);
    }

    /**
     * 平台
     */
    public function getAllPlatform()
    {
        CommonImp::getAllPlatform($this->params);
    }

    /**
     * 开发者
     */
    public function getAllDeveloper()
    {
        CommonImp::getAllDeveloper($this->params);
    }

    /**
     * 用户权限下应用列表 分页面
     */
    public function getUserAppNewList()
    {
        CommonImp::getUserAppNewList($this->params);
    }

    



    
}