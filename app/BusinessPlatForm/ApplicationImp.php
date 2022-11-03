<?php

namespace App\BusinessPlatForm;

use App\BusinessLogic\ApplicationLogic;
use App\BusinessLogic\ChannelLogic;
use App\BusinessLogic\CommonLogic;
use App\BusinessLogic\DeveloperLogic;
use App\BusinessLogic\OperationLogLogic;
use App\BusinessLogic\PlatformLogic;
use App\Common\ApiResponseFactory;
use App\Common\Service;
use function GuzzleHttp\Psr7\str;
use Illuminate\Support\Facades\App;
use Illuminate\Support\Facades\DB;
use App\BusinessLogic\UserLogic;
use Illuminate\Support\Facades\Redis;
use App\Models\Zhibo_user;

class ApplicationImp
{
    public static function zhibo_user_add($params)
    {
        // 必填参数判断
        if (!$params) ApiResponseFactory::apiResponse([], [], 300, '');
        if (!$params['name']) ApiResponseFactory::apiResponse([], [], 2000, '用户姓名不能为空');
        if (!$params['phone']) ApiResponseFactory::apiResponse([], [], 20001, '用户手机号不能为空');

        $preg_phone = '/^1[3456789]\d{9}$/ims';
        if (!preg_match($preg_phone, $params['phone'])) {
            ApiResponseFactory::apiResponse([], [], 20002, '用户手机号合法');
        }
        $arr = [];
        $arr['name'] = $params['name'];
        $arr['phone'] = $params['phone'];
        $pre = 'zhibo_';
        $key = $pre . $arr['phone'];

        Redis::select(0);

        $get_num = Redis::get($key);
        if ($get_num >= 10) {
            ApiResponseFactory::apiResponse([], [], 20003, '一分钟只能填10次');
        }

        $com_obj = Zhibo_user::create($arr);
        $com_obj = Service::data($com_obj);
        Redis::incr($key);
        Redis::expire($key, '60');
        if (!$com_obj) {
            ApiResponseFactory::apiResponse([], [], 20004, '添加失败');
        }
        ApiResponseFactory::apiResponse([], []);
    }
}
