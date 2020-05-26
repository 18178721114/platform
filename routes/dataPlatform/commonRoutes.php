<?php

// 货币接口路由
Route::match(['get'], 'api/get/currency', 'CommonController@getCurrencyList');

// 发行公司(掌游旗下公司列表)
Route::match(['get'], 'api/get/company', 'CommonController@getZplayCompanyList');

// 应用大类
Route::match(['get'], 'api/get/appCategory', 'CommonController@getAppCategoryList');

// 广告形式接口
Route::match(['get'], 'api/get/adType', 'CommonController@getAdTypeList');

// 负责人
Route::match(['get'], 'api/get/busManager', 'CommonController@getBusinessManagerList');

// 平台类型
Route::match(['get'], 'api/get/platType', 'CommonController@getPlatformTypeList');

// 客户公司信息
Route::match(['get'], 'api/customer/company', 'CommonController@getCustomerCompanyList');

// 支付方式
Route::match(['get'], 'api/payment/platform', 'CommonController@getPaymentMethodList');

// 部门
Route::match(['get'], 'api/get/department', 'CommonController@getDepartmentList');
// 功能权限
Route::match(['get'], 'api/get/functionPermission', 'CommonController@getfunctionPermissionList');

// 用户列表
Route::match(['get','post'], 'api/get/menuList', 'CommonController@getMenuList');


// 广告方式
Route::match(['get'], 'api/ad/platform', 'CommonController@getAdMethodList');

// 统计方式
Route::match(['get'], 'api/tj/platform', 'CommonController@getTjMethodList');

// 推广方式
Route::match(['get'], 'api/tg/platform', 'CommonController@getTgMethodList');

// 代理方式
Route::match(['get'], 'api/dl/platform', 'CommonController@getDlMethodList');

// 平台公共列表
Route::match(['get'], 'api/all/platform', 'CommonController@getAllPlatform');

// 用户权限下应用列表
Route::match(['get'], 'api/user/app/list', 'CommonController@getUserAppList');

// 用户权限下应用列表 分页面
Route::match(['get'], 'api/user/app/newlist', 'CommonController@getUserAppNewList');

// 用户权限下应用列表
Route::match(['get'], 'api/country/list', 'CommonController@getCountryList');

// 开发者列表
Route::match(['get'], 'api/all/developer', 'CommonController@getAllDeveloper');



