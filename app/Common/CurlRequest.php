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
 * 公共服务类 Service
 * @category   Service
 *
 */
class CurlRequest
{

    /**
     * curl post header
     * @param $url
     * @param $params
     * @param $header
     * @return string
     */
    public static function curl_header_Post($url,$params,$header){

        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_HTTPHEADER,$header);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_POST, 1);
        curl_setopt($ch, CURLOPT_POSTFIELDS, http_build_query($params));//所需传的数组用http_bulid_query()函数处理一下，就ok了
        curl_setopt($ch, CURLOPT_TIMEOUT, 120); // 设置超时限制防止死循环
        $response = curl_exec($ch);
        curl_close($ch);
        return trim($response);
    }

    /**
     * curl post header
     * @param $url
     * @param $params
     * @param $header
     * @return string
     */
    public static function curl_header_json_Post($url,$params,$header){

        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, 0); // 对认证证书来源的检查
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0); // 从证书中检查SSL加密算法是否存在
        curl_setopt($ch, CURLOPT_HTTPHEADER,$header);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_POST, 1);
        curl_setopt($ch, CURLOPT_POSTFIELDS, $params);//所需传的数组用http_bulid_query()函数处理一下，就ok了
        curl_setopt($ch, CURLOPT_TIMEOUT, 120); // 设置超时限制防止死循环
        $response = curl_exec($ch);
        curl_close($ch);
        return trim($response);
    }

    /**
     * curl get
     * @param $url
     * @param $params
     * @param $header
     * @return string
     */
    public static function get_response($url)
    {
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);

        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_HEADER, 0);
        curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
        curl_setopt($ch, CURLOPT_TIMEOUT,120); //瓒堕  绉
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, FALSE);
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, FALSE);
        $output = curl_exec($ch);
        curl_close($ch);
        return $output;
    }

    public static function get_response_header($url,$header)
    {
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);

        curl_setopt($ch, CURLOPT_HTTPHEADER,$header);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
        curl_setopt($ch, CURLOPT_TIMEOUT,120); //瓒堕  绉
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, FALSE);
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, FALSE);
        $output = curl_exec($ch);
        curl_close($ch);
        return $output;
    }
    public static function post_response_header($url,$header)
    {
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_POST, 1);
        curl_setopt($ch, CURLOPT_HTTPHEADER,$header);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
        curl_setopt($ch, CURLOPT_TIMEOUT,120); //瓒堕  绉
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, FALSE);
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, FALSE);
        $output = curl_exec($ch);
        curl_close($ch);
        return $output;
    }

    /**
     * php模拟curl请求
     *
     * @param string $url 请求的url
     * @param string $method 请求的方法, 默认POST
     * @param array $header 请求设置的头信息
     * @param array $data post请求传递的数据
     * @param integer $head 是否打印头信息
     * @param integer $body 是否打印body信息
     * @param integer $timeout 设置超时时间
     *
     * @return array
     */
    function curlRequest($url, $method = "POST", $header = array(), $data = array(), $head = 0, $body = 0, $timeout = 30, $httpCode = false)
    {
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);

        if (strpos($url, "https") !== false ) {
            curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, 0);
            curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0);
            if (isset($_SERVER['HTTP_USER_AGENT'])) {
                curl_setopt($ch, CURLOPT_USERAGENT, $_SERVER['HTTP_USER_AGENT']);
            }
        }

        if (!empty($header)) {
            curl_setopt($ch, CURLOPT_HTTPHEADER, $header);
        }

        switch ($method) {
            case 'POST':
                curl_setopt($ch, CURLOPT_POST, 1);
                curl_setopt($ch, CURLOPT_POSTFIELDS,$data);
                break;
            case 'GET':
                break;
            case 'PUT':
                curl_setopt($ch, CURLOPT_PUT, 1);
                curl_setopt($ch, CURLOPT_INFILE , '');
                curl_setopt($ch, CURLOPT_INFILESIZE ,10 );
                break;
            case 'DELETE':
                curl_setopt($ch, CURLOPT_CUSTOMERREQUEST, "DELETE");
                break;
            default:
                break;
        }

        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_HEADER, $head);
        curl_setopt($ch, CURLOPT_NOBODY, $body);
        curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, $timeout);

        if ($httpCode){
            $curl_res = curl_getinfo($ch, CURLINFO_HTTP_CODE); // 获取返回状态码
        }else{
            $curl_res = curl_exec($ch); //获得返回数据
        }
        curl_close($ch);
        return $curl_res;
    }

    /**
     *
     * 重试机制 获取数据内容
     * @param $url
     * @return bool|string
     */
    public static function getContent($url)
    {
        static $degree = 0;
        $content = self::get_response ( $url );
        if (! $content) {
            if ($degree > 1) {
                return false;
            }
            $degree ++;
            sleep ( 2 );
            return self::getContent ( $url );
        }

        return $content;
    }


    /**
     * csv数据 解析为 arr数组数据
     * @param string $content
     * @return array|string
     */
    public static function csvToArr($content = ''){

        if(!$content) return '';

        $data = explode ( "\n", trim ( $content, "\n" ) );
        $data = array_map ( 'str_getcsv', $data );

        if (isset ($data[1])) {
            // 0作为字段名称
            $filed = array_map ( function ($value) {
                return strtolower ( preg_replace ( '/\s+/', '_', $value ) );
            }, $data [0] );

            unset ( $data [0] );
            foreach ( $data as &$value ) {
                $value = array_combine ( $filed, $value );
            }
            unset ( $value );

            return $data;
        }
        return '';
    }

    /**
     * 创建文件夹
     * @param $dir
     */
    public static function mkdir($dir){
        $pDir = dirname($dir);
        if (!is_dir($pDir)) {
            self::mkdir($pDir);
        }
        if (!is_dir($dir)) {
            mkdir($dir);
        }
    }

    /**
     * @desc   模拟get请求
     * @access public
     * @param  String  $url 请求地址
     * @return mixed
     */

    public static function curl_gdt_get($url) {
        $curl = curl_init(); // 启动一个CURL会话
        curl_setopt($curl, CURLOPT_URL, $url); // 要访问的地址
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($curl, CURLOPT_SSL_VERIFYPEER, false); // 对认证证书来源的检查
        curl_setopt($curl, CURLOPT_SSL_VERIFYHOST, false); // 从证书中检查SSL加密算法是否存在
        $tmpInfo = curl_exec($curl); // 执行操作
        curl_close($curl); // 关闭CURL会话
        return trim($tmpInfo);
    }


    /* PHP CURL HTTPS POST */
    public static function curl_post_https($url,$post_data){ // 模拟提交数据函数
        $curl = curl_init(); // 启动一个CURL会话
        curl_setopt($curl, CURLOPT_URL, $url); // 要访问的地址
        curl_setopt($curl, CURLOPT_SSL_VERIFYPEER, 0); // 对认证证书来源的检查
        curl_setopt($curl, CURLOPT_SSL_VERIFYHOST, 0); // 从证书中检查SSL加密算法是否存在
//        curl_setopt($curl, CURLOPT_USERAGENT, $_SERVER['HTTP_USER_AGENT']); // 模拟用户使用的浏览器
        curl_setopt($curl, CURLOPT_FOLLOWLOCATION, 1); // 使用自动跳转
        curl_setopt($curl, CURLOPT_AUTOREFERER, 1); // 自动设置Referer
        curl_setopt($curl, CURLOPT_POST, 1); // 发送一个常规的Post请求
        curl_setopt($curl, CURLOPT_POSTFIELDS, $post_data); // Post提交的数据包
        curl_setopt($curl, CURLOPT_TIMEOUT, 30); // 设置超时限制防止死循环
        curl_setopt($curl, CURLOPT_HEADER, 0); // 显示返回的Header区域内容
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1); // 获取的信息以文件流的形式返回
        $tmpInfo = curl_exec($curl); // 执行操作
        if (curl_errno($curl)) {
            echo 'Errno'.curl_error($curl);//捕抓异常
        }
        curl_close($curl); // 关闭CURL会话
        return $tmpInfo; // 返回数据，json格式
    }

}
