<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/4/26
 * Time: ����10:16
 */
// ����·������
Route::match(['post'], 'api/test/testcase', 'TestController@testCase');