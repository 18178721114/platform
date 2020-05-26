<?php

namespace App\Http\Controllers\Test;

use App\BusinessImp\TestImp;
use App\Http\Controllers\Controller as Controller;
use Illuminate\Http\Request;

class TestController extends Controller
{

    /**
     * 测试接口定义
     * @param $params array 请求数据
     */
    public function testCase()
    {
        TestImp::testCase($this->params);
    }
}