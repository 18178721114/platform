<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: 下午4:11
 */

// 开发者列表路由
Route::match(['get'], 'api/developer/list', 'DeveloperController@getDeveloperList');

// 添加 修改开发者信息
Route::match(['post'], 'api/create/developer', 'DeveloperController@createDeveloper');

// 开发者平台应用控制列表接口
Route::match(['post'], 'api/develop/app/list', 'DeveloperController@developAppList');

// 开发者平台应用控制状态修改接口
Route::match(['post'], 'api/develop/app/status', 'DeveloperController@developAppStatus');

// 开发者平台用户注册接口
Route::match(['post'], 'api/develop/user/register', 'DeveloperController@developUserRegister');
