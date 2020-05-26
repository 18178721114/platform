<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: 下午4:11
 */

// 用户列表
Route::match(['get'], 'api/user/list', 'UserController@list');
// 创建用户
Route::match(['post'], 'api/user/create', 'UserController@create');
// 修改状态
Route::match(['post'], 'api/user/editStatus', 'UserController@editStatus');
// 修改用户权限
Route::match(['post'], 'api/user/editRole', 'UserController@editRole');

// 修改所有用户权限
Route::match(['get'], 'api/user/wholeUserList', 'UserController@wholeUserList');
