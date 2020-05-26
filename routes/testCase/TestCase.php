<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/4/26
 * Time: 上午10:16
 */
// 测试路由设置
Route::match(['post'], 'api/test/testcase', 'TestController@testCase');