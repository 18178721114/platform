<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: 下午4:11
 */

//admob 处理过程
Route::match(['get'], 'api/admob/process', 'DataProcessController@admobProcess');

//adwords 处理过程
Route::match(['get'], 'api/adwords/tg/process', 'DataProcessController@adwordsTgProcess');

//facebook 处理过程
Route::match(['get'], 'api/facebook/tg/process', 'DataProcessController@facebookTgProcess');

//mintegral 处理过程
Route::match(['get'], 'api/mintegral/tg/process', 'DataProcessController@mintegralTgProcess');

//百度推广 处理过程
Route::match(['get'], 'api/baidu/tg/process', 'DataProcessController@baiduTgProcess');

//百度广告 处理过程
Route::match(['get'], 'api/baidu/ad/process', 'DataProcessController@baiduAdProcess');

//添加平台取数状态
Route::match(['get'], 'api/platform/add/status', 'DataProcessController@platformAddStatus');

//unity广告 处理过程
Route::match(['get'], 'api/unity/ad/process', 'DataProcessController@UnityAdProcess');

//snapchat推广 处理过程
Route::match(['get'], 'api/snapchat/tg/process', 'DataProcessController@snapchatTgProcess');

//vungle 处理过程
Route::match(['get'], 'api/vungle/ad/process', 'DataProcessController@vungleAdProcess');

//GoolePlay 处理过程
Route::match(['get'], 'api/GoolePlay/ff/process', 'DataProcessController@GoolePlayFfProcess');

//AppStore 处理过程
Route::match(['get'], 'api/AppStore/ff/process', 'DataProcessController@AppStoreFfProcess');

//tiktok 保存数据
Route::match(['post'], 'api/tiktok/tg/data', 'DataProcessController@tiktokTgData');

// todo tiktok 处理过程
Route::match(['get'], 'api/tiktok/tg/process', 'DataProcessController@tiktokTgProcess');

//今日头条 保存数据
Route::match(['post'], 'api/toutiao/tg/data', 'DataProcessController@toutiaoTgData');

//todo 今日头条 处理过程
Route::match(['get'], 'api/toutiao/tg/process', 'DataProcessController@toutiaoTgProcess');

//推啊 处理过程
Route::match(['get'], 'api/tuia/ad/process', 'DataProcessController@tuiaAdProcess');


//todo 渠道处理过程手动测试
Route::match(['get'], 'api/process/channel', 'DataProcessController@channelProcess');


//todo 渠道处理过程手动测试
Route::match(['get'], 'api/flurry/tj/process', 'DataProcessController@flurryTjProcess');

//todo talkingdata 国外数据处理过程
Route::match(['get'], 'api/talkdata/foreign/process', 'DataProcessController@tdForeignProcess');

//todo talkingdata 国内数据处理过程
Route::match(['get'], 'api/talkdata/china/process', 'DataProcessController@tdChinaProcess');

//todo talkingdata 留存数据处理过程
Route::match(['get'], 'api/talkdata/keepuser/process', 'DataProcessController@tdKeepUserProcess');

//todo talkingdata 月活数据处理过程
Route::match(['get'], 'api/talkdata/month/process', 'DataProcessController@tdMonthUserProcess');

//todo talkingdata 留存数据处理过程
Route::match(['get'], 'api/applovin/ad/process', 'DataProcessController@applovinAdProcess');

// 金立计费
Route::match(['get'], 'api/jinli/ff/process', 'DataProcessController@jinliFfProcess');

// appsflyer  pull 数据处理过程
Route::match(['get'], 'api/appsflyer/pull/process', 'DataProcessController@appsflyerPullProcess');

// IronSource
Route::match(['get'], 'api/ironSource/ad/process', 'DataProcessController@ironSourceAdProcess');

//twitter 处理过程
Route::match(['get'], 'api/twitter/tg/process', 'DataProcessController@twitterTgProcess');