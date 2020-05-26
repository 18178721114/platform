<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: 下午4:11
 */

// 平台列表
Route::match(['get'], 'api/platform/list', 'PlatformController@getPlatformList');

// 平台创建
Route::match(['post'], 'api/create/platform', 'PlatformController@createPlatform');

// 平台取数状态
Route::match(['get'], 'api/platform/data/status', 'PlatformController@platformDataStatus');

// 平台取数信息详情
Route::match(['get'], 'api/platform/data/details', 'PlatformController@platformDataDetails');
