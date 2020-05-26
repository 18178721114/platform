<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2018/5/2
 * Time: 16:07
 */
namespace App\Common;

/**
 *
 * 接口响应结果工厂类 ApiResponseFactory
 * @category   ApiResponseFactory
 *
 */
class ApiResponseFactory
{
    /**
     *
     * 接口响应数据处理
     *
     * @param $data
     * @param $log
     * @param int $code
     * @param string $msg
     *
     */
    public static function apiResponse( $data, $log, $code=200, $msg = ''){
        $api_response = new ApiResponse();
        $api_response->setCode($code);
        if($msg){
            $api_response->setErrMsg($msg);
        }
        $api_response->send($data, $log);
    }

}