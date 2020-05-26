<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: 下午4:11
 */

// 角色列表
Route::match(['get'], 'api/role/list', 'RoleController@RoleList');
// 创建角色
Route::match(['post'], 'api/role/create', 'RoleController@RoleCreate');
