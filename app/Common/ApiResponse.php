<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2018/5/2
 * Time: 16:07
 */
namespace App\Common;
use Illuminate\Http\Request;

/**
 *
 * 接口响应结果处理类 ApiResponse
 * @category   ApiResponse
 *
 */
class ApiResponse{
    private $status   = TRUE;
    private $err_code = 200;
    private $err_msg  = null;
    private $data     = [];

    /*
     *
     * @Description 设置状态码
     *
     * @Params       int
     * @Default      200
     *
     * */
    public function setCode ($code) {
        $this->err_code = $code;
    }


    /*
     *
     * @Description 设置提示信息
     *
     * @Params       String
     * @Default       NULL
     *
     * */
    public function setErrMsg ($msg) {
        $this->err_msg = $msg;
    }

    /*
     *
     * @Description   数据发送
     *
     * @Params        Array
     * return         json
     *
     * */
    public function send ($data = array(), $log_msg = array(), $probe = false) {

        if (!$probe) {
            if ($this->err_code == 200) {
                $this->data = $data;
                $this->err_msg = 'success';
            }else if ($this->err_code == 1000) {
                $this->data = $data;
                $this->err_msg = '暂无数据';
            } else if ($this->err_code == 1002) {
//                header('content-type:application:json;charset=utf8');
//                header('Access-Control-Allow-Origin:'.env("ORIGIN_URL"));
//                header('Access-Control-Allow-Methods:GET, POST, OPTIONS, PUT, DELETE');
//                header('Access-Control-Allow-Headers:DNT,X-CustomHeader,Keep-Alive,User- Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,token');
////                header('Access-Control-Allow-Credentials:true');
                $origin = isset($_SERVER['HTTP_ORIGIN']) ? $_SERVER['HTTP_ORIGIN'] : '';
                $allow_origin = array('http://wwcm.goldphp.cn:8080','http://analysis-test.zplay.cn','http://erm-older-test.zplay.cn','https://analysis.zplay.cn','http://analysis-debug.zplay.cn:8080');
                if(in_array($origin, $allow_origin)){
                    header("Access-Control-Allow-Origin:".$origin);
                    //header("Access-Control-Allow-Origin: *");
                    header('content-type:application:json;charset=utf8');
                    header('Access-Control-Allow-Methods:GET, POST, OPTIONS, PUT, DELETE');
                    header('Access-Control-Allow-Headers:DNT,X-CustomHeader,Keep-Alive,User- Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,token');
                    header('Access-Control-Allow-Credentials:true');
                    header("HTTP/1.0 401 Unauthorized");die;
                } 
            }else {
                if (!$this->err_msg) {
                    $this->err_msg = SetError::getErrorInfo($this->err_code);
                }
                $this->status = false;
            }
        }

        $response_array             = [];
        $response_array['data_type'] = $this->status;
        $response_array['error']  = $this->err_msg;
        $response_array['data']     = $this->data;

        // 存储日志
        $log_msg = $_REQUEST;

    //    if($log_msg){
    //        $this->saveLog($log_msg,$response_array);
    //    }

        $format = isset($_REQUEST['format']) ? $_REQUEST['format'] : NULL;

        switch ($format) {
            case 'json':
                $result = json_encode($response_array);
                break;
            case 'xml':
                $result = $this->xml_response($response_array);
                break;
            default:
                $result = json_encode($response_array);
                break;
        }

        if (!$probe){
//             $origin = isset($_SERVER['HTTP_ORIGIN']) ? $_SERVER['HTTP_ORIGIN'] : '';
// //            $allow_origin = array('http://erm-test.zplay.cn','http://erm-older-test.zplay.cn','https://erm.zplay.cn','https://analysis.zplay.cn','http://erm-debug.zplay.cn','http://erm-debug.zplay.cn:8080');
//             $allow_origin = array('http://wwcm.goldphp.cn:8080','http://analysis-test.zplay.cn','http://erm-older-test.zplay.cn','https://analysis.zplay.cn','http://analysis-debug.zplay.cn:8080');
//             if(in_array($origin, $allow_origin)){
//                 header("Access-Control-Allow-Origin:".$origin);
//                 ///header("Access-Control-Allow-Origin: *");
//                 header('content-type:application:json;charset=utf8');
//                 header('Access-Control-Allow-Methods:GET, POST, OPTIONS, PUT, DELETE');
//                 header('Access-Control-Allow-Headers:DNT,X-CustomHeader,Keep-Alive,User- Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,token');
//                 header('Access-Control-Allow-Credentials:true');
//     //            header("Access-Control-Allow-Origin:".env("ORIGIN_URL"));
//     //            header('content-type:application:json;charset=utf8');
//     //            header('Access-Control-Allow-Methods:GET, POST, OPTIONS, PUT, DELETE');
//     //            header('Access-Control-Allow-Headers:DNT,X-CustomHeader,Keep-Alive,User- Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,token');
//     //            header('Access-Control-Allow-Credentials:true');
//             }
             header("Access-Control-Allow-Origin: *");
             header('content-type:application:json;charset=utf8');
             header('Access-Control-Allow-Methods:GET, POST, OPTIONS, PUT, DELETE');
             header('Access-Control-Allow-Headers:DNT,X-CustomHeader,Keep-Alive,User- Agent,X-Requested-With,If-Modified-Since,Cache-Control,Content-Type,token');
             header('Access-Control-Allow-Credentials:true');
                
            echo $result;
            exit;
        }
    }

    /**
     *
     * 保存日志
     *
     * @param $params 请求参数
     * @param $response 执行结果
     *
     */
    private function saveLog ($params, $response) {
        $fileName = date('Y-m-d',time());
        $dir = '../storage/logs';

        $request = Request::createFromGlobals();

        $startTime = $request->server('REQUEST_TIME_FLOAT');
        $time           = microtime(true);
        $consumeTime    = $time-$startTime;
        $arr1 = [
            'IP' => Service::getIP(),
            '请求地址' => $request->server('HTTP_HOST'),
            'action地址' => $request->server('REQUEST_URI'),
            '执行时间' => $consumeTime
        ];
        $arr1['请求参数'] = $params;
        $arr2['执行结果'] = $response;

        if (!is_dir($dir)) {
            mkdir($dir,0777,true);
        }
        $logFilename      = $dir.'/'.$fileName.'.log';

        //生成日志
        $string = date('Y-m-d H:i:s', time()) . "\n" .json_encode($arr1, JSON_UNESCAPED_UNICODE) ."\n" .json_encode($arr2, JSON_UNESCAPED_UNICODE) . "\n\n";
        file_put_contents( $logFilename,$string,FILE_APPEND);

    }

    /**
     *
     * xml数据格式化
     *
     * @param $response_array
     *
     */
    public function xml_response ($response_array) {
        $result = $this->grant_array($response_array);
        header("Content-Type:text/xml");
        $xml = "<?xml version='1.0' encoding='UTF-8'?>\n";
        $xml .= "<root>\n";
        $xml .= self::xml_encode($result);
        $xml .= "</root>";
        echo $xml;
        exit();
    }

    /*
    * 按照接口格式生成原数据数组
    *
    * @param integer $code 状态码
    * @param string $msg 状态信息
    * @param array $data 数据
    * return array
    */
    public function grant_array ($response_array) {
        if(empty($response_array['data'])){
            $response_array['data'] = NULL;
        }
        return array(
            'code' => (int)$response_array['err_code'],
            'msg' => $response_array['err_msg'],
            'data' => $response_array['data']
        );
    }

    /*
    * 将数组转换为XML格式
    *
    * @param array $array 数组
    * return string
    */
    private function xml_encode ($array = array()) {
        $xml = $attr = "";
        if(!empty($array)){

            if(is_object($array)){
                $array = get_object_vars($array);
            }

            foreach ($array as $key => $value) {

                if (is_numeric($key)) {
                    $attr = " id='{$key}'";
                    $key = "item";
                }

                $xml .= "<{$key}{$attr}>";
                $xml .= (is_array($value) || is_object($value) )? $this->xml_encode($value) : $value;
                $xml .= "</{$key}>\n";

            }
        }
        return $xml;
    }


}