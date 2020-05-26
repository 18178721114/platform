<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2018/5/2
 * Time: 16:07
 */
namespace App\Common;
use function foo\func;

/**
 *
 * 公共服务类 Service
 * @category   Service
 *
 */
class Service
{
    const PI = 3.1415926535898;
    /*
     *
     * 验证必填字段
     *
     * @params $arr array 必填字段
     * @params $condition array 传递的参数
     * return  array 必填数据赋值
     *
     * */
    public static function checkField ($arr, $condition, $tableFieldName) {

        $data = [];
        foreach ($condition as $key => $value){
            if ($value != 'ad_type' && $value != 'app_version' &&  $value != 'statistic_version' &&  $value != 'divide_billing' && $value != 'divide_ad' && $value != 'divide_cost' && $value != 'ad_status'){
                if (!isset($value,$arr) || !$arr[$value]){
                    if (isset($tableFieldName[$value]) && $tableFieldName[$value]){
                        ApiResponseFactory::apiResponse( [], $condition, $tableFieldName[$value]);
                    }else{
                        ApiResponseFactory::apiResponse( [], $condition, 300);
                    }
                }else{
                    $data[$value] = $arr[$value];
                }
            }else{
                $data[$value] = $arr[$value];
            }

        }
        return $data;

    }

    /*
     *
     * 获取访问者IP
     *
     * */

    public static function getIP () {
        global $ip;
        if (getenv("HTTP_CLIENT_IP"))
            $ip = getenv("HTTP_CLIENT_IP");
        else if(getenv("HTTP_X_FORWARDED_FOR"))
            $ip = getenv("HTTP_X_FORWARDED_FOR");
        else if(getenv("REMOTE_ADDR"))
            $ip = getenv("REMOTE_ADDR");
        else $ip = "Unknow";
        return $ip;
    }


    /*
     *
     *  验证时间戳
     *
     */

    public static function isTimestamp($timestamp) {
        if(strtotime(date('m-d-Y H:i:s',$timestamp)) === $timestamp) {
            return $timestamp;
        } else return false;
    }

    /**
     * 是否日期范围
     * @param string $str
     * @return number
     */
    public static function isDateRange($str)
    {
        return preg_match('/\d{4}\/\d{1,2}\/\d{1,2}\-\d{4}\/\d{1,2}\/\d{1,2}/i', trim($str));
    }

    /*
     *  对象转数组
     */
    public static function data($data)
    {
        return json_decode(json_encode($data), true);
    }

    /**
     * 导出csv文件
     * @param $filename
     * @param $data
     */
    public static function exportCsv($filename,$data) {
        header("Content-type:text/csv");
        header("Content-Disposition:attachment;filename=".$filename);
        header('Cache-Control:must-revalidate,post-check=0,pre-check=0');
        header('Expires:0');
        header('Pragma:public');
        echo $data;
    }


    public static function rad($d){
        $result = $d * self::PI / 180.0;
        return $result;
    }

    /**
     * 基于googleMap中的算法得到两经纬度之间的距离,计算精度与谷歌地图的距离精度差不多，相差范围在0.2米以下
     * @param $lon1 第一点的精度
     * @param $lat1 第一点的纬度
     * @param $lon2 第二点的精度
     * @param $lat3 第二点的纬度
     * @return 返回的距离，单位m
     */
    public static function getDistance($lon1, $lat1, $lon2, $lat2){
        $radLat1 = self::rad($lat1);
        $radLat2 = self::rad($lat2);
        $a = $radLat1 - $radLat2;
        $b = self::rad($lon1) - self::rad($lon2);
        $s = 2 * asin(sqrt(pow(sin($a/2),2) + cos($radLat1)*cos($radLat2)*pow(sin($b/2),2)));
        $s = $s * 6370996.81;
        $s = round($s * 10000) / 10000;

        return round($s);
    }

    /**
     * 根据中心坐标获取指定距离的随机坐标点
     * @param $lon1
     * @param $lat1
     * @param $distance
     * @return array
     */
    public static function getRandPoint($lon1, $lat1, $distance){

		$rad360 = 2 * self::PI;
        $maxdist = $distance;
        $maxdist = $maxdist / 6370996.81;
		$startlat = self::rad($lat1);
		$startlon = self::rad($lon1);
		$cosdif = cos($maxdist) - 1;
		$sinstartlat = sin($startlat);
		$cosstartlat = cos($startlat);
		$dist = acos((self::randFloat() * $cosdif + 1));
		$brg = $rad360 * self::randFloat();
		$lat = asin($sinstartlat * cos($dist) + $cosstartlat * sin($dist) * cos($brg));
		$lon = self::deg(self::normalizeLongitude($startlon * 1 + atan2(sin($brg) * sin($dist) * $cosstartlat, cos($dist) - $sinstartlat * sin($lat))));
        $lat = self::deg($lat);
        $lat = self::padZeroRight($lat);
        $lon = self::padZeroRight($lon);

        $location = [];
        $location['lon'] = $lon;
        $location['lat'] = $lat;
		return $location;
    }

    /**
     * 生成0~1随机小数
     * @param  Int   $min
     * @param  Int   $max
     * @return Float
     */
    public static function randFloat($min=0, $max=1){
        return $min + mt_rand()/mt_getrandmax() * ($max-$min);
    }

    public static function deg($rd){
        return ($rd * 180 / self::PI);
    }


    public static function  normalizeLongitude($lon){
        $n = self::PI;
        if ($lon > $n){
            $lon = $lon - 2 * $n;
        } else if ($lon < -$n) {
            $lon = $lon + 2 * $n;
        }

        return $lon;
    }

    public static function padZeroRight($s)
    {
        $sigDigits = 8;
        $s = Round($s * Pow(10, $sigDigits)) / Pow(10, $sigDigits);
        return $s;
    }


    /*
     * 二维数组排序方法
     * @param $arrays     需要排序的数组
     * @param $sort_key   需要排序的字段
     * @param $sort_order 排序方式 SORT_DESC 降序;SORT_ASC 升序
     * @param $sort_type  排序类型 SORT_NUMERIC 每一项按数字排序
     */
    public static function mySort($arrays,$sort_key,$sort_order=SORT_DESC,$sort_type=SORT_NUMERIC ){
        if(is_array($arrays)){
            foreach ($arrays as $array){
                if(is_array($array)){
                    $key_arrays[] = $array[$sort_key];
                }else{
                    return false;
                }
            }
        }else{
            return false;
        }
        array_multisort($key_arrays,$sort_order,$sort_type,$arrays);
        return $arrays;
    }

    /**
     * 求两个日期之间相差的天数
     * (针对1970年1月1日之后，求之前可以采用泰勒公式)
     * @param string $day1
     * @param string $day2
     * @return number
     */
    public static function diffBetweenTwoDays ($day1, $day2)
    {

        $second1 = strtotime($day1);
        $second2 = strtotime($day2);
        if ($second1 > $second2) {
            return -1;
        }else{
            return ($second2 - $second1) / 86400;
        }
    }

    /**
     * post curl
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
        curl_setopt($ch, CURLOPT_POSTFIELDS, http_build_query($params));
        $response = curl_exec($ch);
        curl_close($ch);
        return trim($response);
    }

    /**
     * 主菜单pid为0
     * @return array
     */
    public static function getMenu($data) {
        foreach ($data as $key => $items) {
            if ($items['parent_id'] == "0" ) {
                unset($data[$key]);
                $menu[] = self::buildMenuTree($data, $items, $items['id']);
            }
        }
        return $menu;
    }
    /**
     * 生产多级菜单树
     * @param array $items
     * @param int $rid
     * @return array
     */
    public static function buildMenuTree($data,$items,$rid,$permission=-2) {
        $childs = self::getChildMenu($data, $items, $rid,$permission);
        if (isset($childs['child'])) {
            foreach ($childs['child'] as $key => $value) {          
                    $children = self::buildMenuTree($data, $value, $value['id'],$permission);
                    if (isset($children['child']) && $children['child']) {
                        $childs['child'][$key]['child'] = $children['child'];
                    }
            }
        }
        return $childs;
    }

    /**
     * 获取子菜单
     *
     */
    public static function getChildMenu($data, $items,$rid,$permission) {
        foreach ($data as $key => $value) {
            if($value['type'] !=2){
                if ($value['parent_id'] == $rid) {
                    unset($data[$key]);
                    if($permission !=-2){
                        foreach ($permission as $v) {
                            if($value['id'] == $v){
                                $items['child'][] = $value;
                            }
                        }
                    }else{
                         $items['child'][] = $value;
                    }
                   
                }
            }

        }
        return $items;
    }

    // 获取模块操作路径
    public static function getTopParentName($menu_list,$id){
        $arr = [];
        foreach($menu_list as $v){
            if($v['id'] == $id){
                $arr[$v['id']] = $v['menu_name'];
                $arr = array_merge(self::getTopParentName($menu_list,$v['parent_id']),$arr);
            }
        }
        return $arr;
    }

    /**
     * 多个连续空格只保留一个
     *
     * @param string $string 待转换的字符串
     * @return unknown
     */
    public static function mergeSpaces($string)
    {
        return preg_replace ( "/\s(?=\s)/","\\1", $string);
    }
    /**
     *验证邮箱
     *
     */
    public static function checkemail($email)
    {
        return  filter_var($email, FILTER_VALIDATE_EMAIL);
    }


    //获取时间间隔 返回间隔天数
    public static function timePeriod($startTime,$endTime){
        $period = strtotime($endTime)-strtotime($startTime);
        return $period/86400;
    }

    /*
	 * csv数据拼接
	*/
    public static function csv_output_str($title,$data){
        $str = $title;
        $string = '';
        foreach($data as $row){
            foreach($row as $col){
                @$string .= self::utf2gbk($col);
            }
            $str .= substr($string, 0,-1)."\n";
            unset($string);
        }
        return $str;
    }

    //关于导出csv
    public static function utf2gbk($str)
    {
        if(isset($str)){
            return @iconv('utf-8','gb2312',$str).',';
        }else{
            return '';
        }
    }

    public static function export_csv($filename,$data){
        header("Content-type:text/csv");
        header("Content-Disposition:attachment;filename=".$filename);
        header('Cache-Control:must-revalidate,post-check=0,pre-check=0');
        header('Expires:0');
        header('Pragma:public');
        echo $data;
        exit;
    }

    /**
     * @author injection(injection.mail@gmail.com)
     * @var date1日期1
     * @var date2 日期2
     * @var tags 年月日之间的分隔符标记,默认为'-'
     * @return 相差的月份数量
     * @example:
    $date1 = "2003-08-11";
    $date2 = "2008-11-06";
    $monthNum = getMonthNum( $date1 , $date2 );
    echo $monthNum;
     */
    public static function getMonthNum( $date1, $date2, $tags='-' ){
        $date1_stamp=strtotime($date1);
        $date2_stamp=strtotime($date2);
        list($date_1['y'],$date_1['m'])=explode("-",date('Y-m',$date1_stamp));
        list($date_2['y'],$date_2['m'])=explode("-",date('Y-m',$date2_stamp));
        return abs($date_1['y']-$date_2['y'])*12 +$date_2['m']-$date_1['m'];
    }

    // 获取两个日期之间的所有月份信息
    public static function getAllMonthNum($date1, $date2){
        $all_month = [];
        $start    = new \DateTime($date1);
        $end      = new \DateTime($date2);
        // 时间间距 这里设置的是一个月
        $interval = \DateInterval::createFromDateString('1 month');
        $period   = new \DatePeriod($start, $interval, $end);
        foreach ($period as $dt) {
            $all_month[] = $dt->format("Ym");
        }
        return $all_month;
    }




    //获得单引号分隔好的数据
    public static function splitArr($arr){
        if(!empty($arr)){
            $data_list = implode("','", $arr);
            $data_list = "'".$data_list."'";
            return $data_list;
        }else{
            return '';
        }
    }

    /**
     * 计算出两个日期之间的月份区间
     * @author 微云科技 ROVAST
     * @param  [type] $start_date [开始日期，如2014-03]
     * @param  [type] $end_date   [结束日期，如2015-12]
     * @param  string $explode    [年份和月份之间分隔符，此例为 - ]
     * @param  boolean $addOne    [算取完之后最后是否加一月，用于算取时间戳用]
     * @return [type]             [返回是两个月份之间所有月份字符串]
     */
    public static function dateMonthsSections($start_date,$end_date,$explode='-',$addOne=false)
    {
        $data = [];
        $data = self::dateMonths($start_date, $end_date, $explode, $addOne);
        return $data;
//        $length = sizeof($data);
//
//        $res = array();
//        var_dump($data);die;
//        foreach ($data as $key => $value) {
//            if ($key < ($length - 1)) {
//                $date1 = $value;
//                $date2 = $data[$key + 1];
//                $res[$key][0] = $date1;
//                $res[$key][1] = $date2;
//            }
//        }
//
//        return $res;
    }

    public static function dateMonths($start_date,$end_date,$explode='-',$addOne=false){
        //判断两个时间是不是需要调换顺序
        $start_int = strtotime($start_date);
        $end_int = strtotime($end_date);
        if($start_int > $end_int){
            $tmp = $start_date;
            $start_date = $end_date;
            $end_date = $tmp;
        }


        //结束时间月份+1，如果是13则为新年的一月份
        $start_arr = explode($explode,$start_date);
        $start_year = intval($start_arr[0]);
        $start_month = intval($start_arr[1]);


        $end_arr = explode($explode,$end_date);
        $end_year = intval($end_arr[0]);
        $end_month = intval($end_arr[1]);


        $data = array();
        $data[] = date('Y-m',strtotime($start_date));


        $tmp_month = $start_month;
        $tmp_year = $start_year;


        //如果起止不相等，一直循环
        while (!(($tmp_month == $end_month) && ($tmp_year == $end_year))) {
            $tmp_month ++;
            //超过十二月份，到新年的一月份
            if($tmp_month > 12){
                $tmp_month = 1;
                $tmp_year++;
            }
            $data[] = $tmp_year.$explode.str_pad($tmp_month,2,'0',STR_PAD_LEFT);
        }


        if($addOne == true){
            $tmp_month ++;
            //超过十二月份，到新年的一月份
            if($tmp_month > 12){
                $tmp_month = 1;
                $tmp_year++;
            }
            $data[] = $tmp_year.$explode.str_pad($tmp_month,2,'0',STR_PAD_LEFT);
        }


        return $data;
    }

}
