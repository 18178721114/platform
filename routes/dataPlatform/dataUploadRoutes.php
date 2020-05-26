<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: 下午4:11
 */

// 数据核对报表导入
Route::match(['post'], 'api/data/upload', 'DataUploadController@postOriginalDataUpload');

// 三方平台报表数据导入
Route::match(['post'], 'api/platform/data/upload', 'DataUploadController@postPlatformDataUpload');