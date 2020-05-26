<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/6/5
 * Time: 上午11:59
 */
/**
 *
 * 系统调用方式：cli/fpm-fcgi
 * @param $sapi_type
 * @return array
 */

namespace App\Common;
use function foo\func;

class ParseDayid
{

    protected static $date_format = "yyyy-mm-dd";

    public function __construct()
    {
        // 初始化
        define("BR", "<br/>");
    }


    public static function get_dayid($argv){
        $dayid = self::get_date_time($argv);
        echo "dayid: $dayid" . PHP_EOL;
        return $dayid;
    }


    protected static function usage()
    {
        global $argv;
        global $date_format;
        echo "Usage: php $argv[0] dayid($date_format)" . PHP_EOL;
        exit;
    }


    /**
     * 对于cli/cgi两种调用方法，得到日期。
     * 如果没有输入日期，取昨天日期。
     */
    private static function get_date_time($argv)
    {
        $sapi_type = php_sapi_name();
        if (substr($sapi_type, 0, 3) == 'cli') {
            echo "You are using $sapi_type PHP" . PHP_EOL;

            if (isset($argv)) {
                $dayid = $argv;
            }

        } else {
            $debug = filter_var(trim($_REQUEST['debug']), FILTER_VALIDATE_INT);
            $dayid = trim($_REQUEST['date']);

            if ($debug) {
                echo nl2br("You are not using $sapi_type PHP" . BR);
                echo $dayid . BR;
            }
        }

        //如果没有输入日期，取昨天
        if (empty($dayid)) {
            $dayid = date('Y-m-d', time() - 3600 * 24);
        }

        $dayid_str = strval($dayid);
        if (strlen($dayid_str) != strlen(self::$date_format)) {
            echo "invalid dayid length. should be ".self::$date_format." format" . PHP_EOL;
            exit;
        }

        $year = intval(substr($dayid_str, 0, 4));
        $month = (substr($dayid_str, 5, 2));
        $day = (substr($dayid_str, 8, 2));
        if (isset($debug) && $debug) {
            var_dump($year, $month, $day);
        }

        if (!checkdate($month, $day, $year)) {
            echo 'not a valid dayid. should be yyyymmdd format';
            exit;
        }
        $day_iso = sprintf('%s-%s-%s', $year, $month, $day);

        return $day_iso;
    }

    /**
     * 增强版的echo函数
     * $sapi_type: cli|fast-fcgi
     * @param $msg
     * @param $sapi_type
     */
    private static function iecho($msg, $sapi_type = 'cgi')
    {
        if ($sapi_type == "cgi") {
            echo $msg . PHP_EOL;
        } else {
            echo nl2br($msg . PHP_EOL);
        }
    }


}
