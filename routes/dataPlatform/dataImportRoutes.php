<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: 下午4:11
 */

// 获取日期时间
Route::match(['get'], 'api/channel/data/time', 'DataImportController@getChannelDataTime');

// 导入渠道数据
Route::match(['post'], 'api/channel/data/import', 'DataImportController@importChannelData');

// 数据报错日志信息
Route::match(['post'], 'api/data/error/log', 'DataImportController@getDateErrorLog');

// 数据报错日志处理状态修改
Route::match(['post'], 'api/data/error/status', 'DataImportController@changeErrorStatus');

// 配置报错
Route::match(['post'], 'api/data/config/log', 'DataImportController@getConfigErrorLog');