<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/7/22
 * Time: 上午9:59
 */

// 推广初始化
Route::match(['post'], 'api/data/promotion/init', 'DataSearchController@dataPromotionInit');

// 推广初始化 总体
Route::match(['post'], 'api/data/promotion/population', 'DataSearchController@dataPromotionPopulation');

// 推广 cpi detail
Route::match(['post'], 'api/data/promotion/population/details', 'DataSearchController@dataPromotionPopulationDetails');

// 推广 arpu detail
Route::match(['post'], 'api/data/promotion/arpu/details', 'DataSearchController@dataPromotionArpuDetails');

//设备表格数据
Route::match(['post'], 'api/data/promotion/device', 'DataSearchController@dataPromotionDevice');

//平台表格数据
Route::match(['post'], 'api/data/promotion/platform', 'DataSearchController@dataPromotionPlatform');

//国家表格数据
Route::match(['post'], 'api/data/promotion/country', 'DataSearchController@dataPromotionCountry');

// 总体情况增长量图
Route::match(['post'], 'api/data/promotion/growth', 'DataSearchController@dataPromotionGrowth');

// 总体情况成本收入图
Route::match(['post'], 'api/data/promotion/income', 'DataSearchController@dataPromotionIncome');

// 总体情况成本收入图
Route::match(['post'], 'api/data/promotion/income', 'DataSearchController@dataPromotionIncome');

// 国家或地区排名
Route::match(['post'], 'api/data/promotion/ranking', 'DataSearchController@dataPromotionRranking');

// 总体情况CPI
Route::match(['post'], 'api/data/promotion/cpi', 'DataSearchController@dataPromotionCpi');

// 总体情况ARPU
Route::match(['post'], 'api/data/promotion/arpu', 'DataSearchController@dataPromotionArpu');

// 总体情况 新增活跃 柱状图
Route::match(['post'], 'api/data/promotion/user', 'DataSearchController@dataPromotionUser');