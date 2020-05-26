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
