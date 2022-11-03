<?php

/*
|--------------------------------------------------------------------------
| Web Routes
|--------------------------------------------------------------------------
|
| Here is where you can register web routes for your application. These
| routes are loaded by the RouteServiceProvider within a group which
| contains the "web" middleware group. Now create something great!
|
*/

Route::get('/', function () {
//    $laravel = app();
//    return "Your Laravel version is ".$laravel::VERSION;
    return view('404');
});

Route::match(['get'], 'api/user/add', 'Platform\UserController@zhibo_add');
// Route::match(['post'], 'api/user/login', 'DataPlatform\UserController@login');

// Route::match(['post'], 'api/develop/platform/login', 'DataPlatform\UserController@devlogin');
// // Oauth2.0
// Route::match(['get'], 'api/oauth/authorize', 'OauthController@authorizecode');
// Route::match(['post'], 'api/oauth/token', 'OauthController@token');
// Route::match(['post'], 'api/oauth/tokenRefresh', 'OauthController@tokenRefresh');

// // 获取广点通code
// Route::match(['get'], 'api/gdt/code', 'OauthController@getGdtCode');
// // 获取快手code
// Route::match(['get'], 'api/kuaishou/code', 'OauthController@getKuaishouCode');
// // 获取快手code
// Route::match(['get'], 'api/tiktok/code', 'OauthController@getTiktokCode');
// // 获取今日头条code
// Route::match(['get'], 'api/toutiao/code', 'OauthController@getToutiaoCode');
// // 获取snapchatcode
// Route::match(['get'], 'api/snapchat/code', 'OauthController@getSnapchatCode');
// // 获取twitter
// Route::match(['get'], 'api/twitter/code', 'OauthController@getTwitterCode');
// // 获取appsflyer push data
// Route::namespace('DataPlatform')->group(function () {
//     Route::match(['get', 'post'], 'api/appsflyer/push', 'AppsFlyerController@getAppsflyerPushData');
// });


// // 数据报错日志信息
// //Route::match(['post'], 'api/data/upload', 'DataUploadController@postOriginalDataUpload');
// //测试接口
// Route::match(['get'], 'api/test', 'OauthController@test');

// //数据链接
// Route::match(['get'], 'api/data/commond', 'OauthController@dataCommond');

// Route::middleware(['CheckUser'])->group(function () {


//     Route::namespace('DataPlatform')->group(function () {
//         // Controllers Within The "App\Http\Controllers\DataPlatform" Namespace
//         //用户
//         require_once "dataPlatform/userRoutes.php";
//         //
//         require_once "dataPlatform/commonRoutes.php";
//         //
//         require_once "dataPlatform/platformRoutes.php";
//         // 
//         require_once "dataPlatform/channelRoutes.php";
//         //
//         require_once "dataPlatform/developerRoutes.php";
//         // 
//         require_once "dataPlatform/operationLogRoutes.php";
//         // 
//         require_once "dataPlatform/applicationRoutes.php";
//          // 
//         require_once "dataPlatform/roleRoutes.php";
//         //
//         require_once "dataPlatform/dataImportRoutes.php";
//         //
//         require_once "dataPlatform/dataSearchRoutes.php";
//         //
//         require_once "dataPlatform/dataIndexRoutes.php";
//         //
//         require_once "dataPlatform/dataPromotionRoutes.php";
//         //
//         require_once "dataPlatform/dataUploadRoutes.php";
//         //
//         require_once "dataPlatform/appsFlyerDeviceRoutes.php";
//         //
//         require_once "dataPlatform/dataOperationRoutes.php";

//     });

// });

// Route::namespace('DataPlatform')->group(function () {
//     require_once "dataPlatform/dataProcessRoutes.php";
// });

