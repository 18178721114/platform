<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: 下午4:11
 */

// 平台列表路由
Route::match(['get'], 'api/channel/list', 'ChannelController@getChannelList');

// 添加 修改平台信息
Route::match(['post'], 'api/create/channel', 'ChannelController@createChannel');
