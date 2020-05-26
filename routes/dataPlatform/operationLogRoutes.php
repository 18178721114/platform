<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: 下午4:11
 */

// 平台列表路由
Route::match(['get'], 'api/operation/log', 'OperationLogController@getOperationLogList');
