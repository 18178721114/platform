<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/7/22
 * Time: 上午9:59
 */

// 商业化运营数据报表 变现数据
Route::match(['post'], 'api/realization/data', 'DataOperationController@getRealizationData');

// 游戏发行运营数据报表 发行数据
Route::match(['post'], 'api/publish/data', 'DataOperationController@getPublishData');
