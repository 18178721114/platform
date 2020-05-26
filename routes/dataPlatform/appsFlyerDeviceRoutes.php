<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2020/3/10
 * Time: 下午3:42
 */
// 应用列表信息
Route::match(['post'], 'api/app/appsflyer/device', 'AppsFlyerController@getAppsFlyerDeviceList');