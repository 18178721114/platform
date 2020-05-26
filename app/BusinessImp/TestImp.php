<?php

namespace App\BusinessImp;

use App\BusinessLogic\TestLogic;
use App\Common\ApiResponseFactory;
use Illuminate\Support\Facades\Log;

class TestImp extends ApiBaseImp
{
    /**
     * 测试接口定义
     * @param array $params
     */
    public static function testCase($params){

        // 返回测试结果
        Log::channel('daily')->info('12341234');
        $app_info['id'] = 123;
        $app_info['app_name'] = 'zhangsan';
        ApiResponseFactory::apiResponse($app_info,[],303);
    }

}
