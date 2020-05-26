<?php

namespace App\InterfaceDefine;


interface IApplication
{
    /**
     * 应用列表
     * @param $params array 请求数据
     */
    public static function getAppList($params);

    /**
     * 创建应用
     * @param $params array 请求数据
     */
    public static function addApp($params);

    /**
     * 统计信息配置列表
     * @param $params array 请求数据
     */
    public static function appStatisticList($params);

    /**
     * 统计信息配置增加 修改
     * @param $params array 请求数据
     */
    public static function createAppStatistic($params);

    /**
     * 统计信息配置删除
     * @param $params array 请求数据
     */
    public static function changeAppStatisticStatus($params);

    /**
     * 广告信息配置列表
     * @param $params array 请求数据
     */
    public static function appAdList($params);

    /**
     * 广告信息配置增加 修改
     * @param $params array 请求数据
     */
    public static function createAppAd($params);

    /**
     * 广告信息配置删除
     * @param $params array 请求数据
     */
    public static function changeAppAdStatus($params);

    /**
     * 计费信息配置列表
     * @param $params array 请求数据
     */
    public static function appBillingList($params);

    /**
     * 广告信息配置账号数据
     * @param $params array 请求数据
     */
    public static function appAdAccountList($params);

    /**
     * 计费信息配置增加 修改
     * @param $params array 请求数据
     */
    public static function createAppBilling($params);

    /**
     * 计费信息配置删除
     * @param $params array 请求数据
     */
    public static function changeAppBillingStatus($params);

    /**
     * 推广信息配置代理平台数据
     * @param $params array 请求数据
     */
    public static function appGeneralizeAgencyList($params);

    /**
     * 推广信息配置列表
     * @param $params array 请求数据
     */
    public static function appGeneralizeList($params);

    /**
     * 推广信息配置增加 修改
     * @param $params array 请求数据
     */
    public static function createAppGeneralize($params);

    /**
     * 推广信息配置删除
     * @param $params array 请求数据
     */
    public static function changeAppGeneralizeStatus($params);


}
