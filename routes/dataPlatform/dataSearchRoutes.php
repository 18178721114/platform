<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/7/22
 * Time: 上午9:59
 */

// 初始化
Route::match(['get'], 'api/data/integrated/query', 'DataSearchController@dataIntegratedQuery');

// 获取邮件发送列表
Route::match(['get'], 'api/data/mail/list', 'DataSearchController@dataMailList');

// 保存邮件发送信息
Route::match(['post'], 'api/data/mail/save', 'DataSearchController@dataMailSave');

// 定制条件列表查询
Route::match(['get'], 'api/data/custom/query', 'DataSearchController@dataCustomQuery');

// 获取搜索数据
Route::match(['post','get'], 'api/get/search/data', 'DataSearchController@getSearchData');

// 定制保存
Route::match(['post'], 'api/data/custom/save', 'DataSearchController@dataCustomSave');

// 三方平台及数据平台初始化
Route::match(['get'], 'api/check/init', 'DataSearchController@getCheckInit');

// 三方平台及数据平台页面
Route::match(['post','get'], 'api/check/date', 'DataSearchController@getCheckData');

// 三方平台及数据平台页面
Route::match(['post','get'], 'api/update/data', 'DataSearchController@updateCheckData');

// 开发者数据
Route::match(['post','get'], 'api/get/developer/data', 'DataSearchController@getDeveloperData');

// 开发者数据 折线图
Route::match(['post','get'], 'api/developer/line/data', 'DataSearchController@getDeveloperLine');
