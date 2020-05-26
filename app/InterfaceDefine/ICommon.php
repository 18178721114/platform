<?php

namespace App\InterfaceDefine;


interface ICommon
{
    /**
     * 货币接口
     * @param $params array 请求数据
     */
    public static function getCurrencyList($params);

    /**
     * 发行公司(掌游旗下公司列表)
     * @param $params array 请求数据
     */
    public static function getZplayCompanyList($params);

    /**
     * 应用大类
     * @param $params array 请求数据
     */
    public static function getAppCategoryList($params);

    /**
     * 广告形式接口
     * @param $params array 请求数据
     */
    public static function getAdTypeList($params);

    /**
     * 负责人
     * @param $params array 请求数据
     */
    public static function getBusinessManagerList($params);

    /**
     * 平台类型
     * @param $params array 请求数据
     */
    public static function getPlatformTypeList($params);

    /**
     * 负责人
     * @param $params array 请求数据
     */
    public static function getCustomerCompanyList($params);

    /**
     * 平台类型
     * @param $params array 请求数据
     */
    public static function getPaymentMethodList($params);
        /**
     * 部门类型
     * @param $params array 请求数据
     */
    public static function getDepartmentList($params);
        /**
     * 功能权限类型
     * @param $params array 请求数据
     */
    public static function getfunctionPermissionList($params);
        /**
     * 用户左侧列表
     * @param $params array 请求数据
     */
    public static function getMenuList($params);
    

}
