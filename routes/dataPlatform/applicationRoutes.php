<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: 下午4:11
 */

// 应用列表信息
Route::match(['get'], 'api/app/list', 'ApplicationController@getAppList');

// 添加 修改应用信息
Route::match(['post'], 'api/create/app', 'ApplicationController@createApp');

// 统计信息配置列表
Route::match(['get'], 'api/app/statistic/list', 'ApplicationController@appStatisticList');

// 统计信息配置增加 修改
Route::match(['post'], 'api/create/app/statistic', 'ApplicationController@createAppStatistic');

// 统计信息配置删除
Route::match(['post'], 'api/app/statistic/status', 'ApplicationController@changeAppStatisticStatus');

// 广告信息配置列表[废弃]
Route::match(['get'], 'api/app/ad/list', 'ApplicationController@appAdList');

// 广告信息配置账号数据
Route::match(['get'], 'api/app/account/list', 'ApplicationController@appAdAccountList');

// 广告信息配置增加 修改
Route::match(['post'], 'api/create/app/ad', 'ApplicationController@createAppAd');

// 广告信息配置删除
Route::match(['post'], 'api/app/ad/status', 'ApplicationController@changeAppAdStatus');

// 计费信息配置列表
Route::match(['get'], 'api/app/billing/list', 'ApplicationController@appBillingList');

// 计费信息配置增加 修改
Route::match(['post'], 'api/create/app/billing', 'ApplicationController@createAppBilling');

// 计费信息配置删除
Route::match(['post'], 'api/app/billing/status', 'ApplicationController@changeAppBillingStatus');
// 计费信息折扣修改
Route::match(['post'], 'api/app/billing/rebate_billing', 'ApplicationController@changeAppBillingRebate');

// 推广信息配置列表[废弃]
Route::match(['get'], 'api/app/generalize/list', 'ApplicationController@appGeneralizeList');

// 推广信息配置代理平台数据
Route::match(['get'], 'api/app/agency/list', 'ApplicationController@appGeneralizeAgencyList');

// 推广信息配置增加 修改
Route::match(['post'], 'api/create/app/generalize', 'ApplicationController@createAppGeneralize');

// 推广信息配置删除
Route::match(['post'], 'api/app/generalize/status', 'ApplicationController@changeAppGeneralizeStatus');

// 平台动态参数公共接口
Route::match(['get'], 'api/platform/config/list', 'ApplicationController@getPlatformConfiglist');

