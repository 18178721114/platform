<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/7/22
 * Time: 上午9:59
 */

// 首页初始化
Route::match(['get'], 'api/data/index/init', 'DataSearchController@dataIndexInit');

// 首页初始化
Route::match(['post'], 'api/data/app/list', 'DataSearchController@dataAppList');

// 应用概览数据
Route::match(['post'], 'api/data/general/list', 'DataSearchController@dataGeneralList');

// 趋势分析
Route::match(['post'], 'api/data/app/line', 'DataSearchController@dataAppLine');

// 地域分析
Route::match(['post'], 'api/data/app/country', 'DataSearchController@dataAppCountry');

// 构成分析
Route::match(['post'], 'api/data/app/form', 'DataSearchController@dataAppForm');

// 构成分析
Route::match(['post'], 'api/data/custom/target', 'DataSearchController@dataCustomTarget');
