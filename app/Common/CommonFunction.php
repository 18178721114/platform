<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/6/5
 * Time: 下午12:13
 */

namespace App\Common;
use function foo\func;
use Illuminate\Support\Facades\Mail;
use PHPMailer\PHPMailer\PHPMailer;
use App\BusinessLogic\DataImportLogic;

class CommonFunction{
    //整合函数
//返回的错误时候看下错误码
    /**
     *
     * @param string $url 请求地址
     * @param string $method 请求方法 get/post
     * @param string httpheader
     * @param string http/https请求url方式
     */
    public static function zplay_curl($url,$method='',$post_data=array(),$httpheader=array(),$http=''){
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);  //获取页面内容，不直接输出到页面
        curl_setopt($ch, CURLOPT_HEADER,0);
        if($method=='post'){
            curl_setopt($ch,CURLOPT_POST, 1);
            if(count($post_data)==0){
                echo '无post数据';exit;
            }else
                curl_setopt($ch,CURLOPT_POSTFIELDS,$post_data); //post请求参数
        }else{//get

        }
        if(count($httpheader)!=0){
            curl_setopt($ch, CURLOPT_HTTPHEADER,array('Content-type:text/xml','charset:utf-8'));
        }
        if($http=='https'){
            curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, FALSE);
            curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, FALSE);
        }
        $output = curl_exec($ch);
        curl_close($ch);
        $ret = json_decode($output,true);
        return $ret;
    }

    /**
     * @param $api_key
     * @param $start_dayid
     * @param $end_dayid
     * @return mixed
     */
    public static function get_response($url,$headers=array())
    {
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, $url);

        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_HEADER, 0);
        curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
        curl_setopt($ch, CURLOPT_TIMEOUT,120); //超时时间  秒
        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, FALSE);
        curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, FALSE);

        if (!empty($headers)) {
            curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
        }

        $output = curl_exec($ch);
        curl_close($ch);
        return $output;
    }
    /**
     * 解析csv文件
     */
    public static function parse_csv($content){
        $data = explode("\n", trim($content, "\n"));
        $data = array_map('str_getcsv', $data);
        if (isset($data[1])) {
            $filed = array_map(function ($value) {
                return strtolower(preg_replace('/\s+/', '_', $value));
            }, $data[0]);

            unset($data[0]);
            foreach ($data as &$value) {
                $value = array_combine($filed, $value);
            }
            unset($value);

            return $data;
        }
    }

    // 原表内的收入
    public static function getIncome($table_name, $field, $day_field, $day, $account_id='') {
        if($account_id){
            $sql = "select sum($field) income from $table_name where date($day_field)='{$day}' and account_id='{$account_id}'";
        }
        else{
            $sql = "select sum($field) income from $table_name where date($day_field)='{$day}'";
        }
        $result = query ( $sql );
        if ($result ['0'] ['income'])
            return sprintf ( "%.2f", $result ['0'] ['income'] );
        else
            return false;
    }

    // 新获取到的收入
    public static function dataIncome($data, $money_field,$plat='') {
        $income = 0;
        foreach ( $data as $v ) {
            if($plat=='smaato')
                $income += empty ( $v['kpi'][$money_field] ) ? 0 : $v ['kpi'][$money_field];
            else
                $income += empty ( $v [$money_field] ) ? 0 : $v [$money_field];
        }
        return sprintf ( "%.2f", $income );
    }

    // 数据比较
    public static function compare($tableIncome, $dataIncome) {
        if ($tableIncome < $dataIncome){
            echo "数据表内:".$tableIncome."_接口返回：".$dataIncome;
            return false;
        }
        else{
            return true;
        }
    }

    //查询账号
    public static function searchCount($table,$day_field,$day){
        $sql = "select count(id)  c from $table where DATE($day_field) ='{$day}'";
        $result = query($sql);
        return $result[0]['c'];
    }

    public static function curl_grayLog($plat,$account_id,$dayid,$url,$data=array()){
        global $ini_array;
        $now = date('Y-m-d H:i:s');
        $host = "analyze-web-b132";
        $error_msg = array('short_message'=>"fetch_nodata",'host'=>$host,'facility'=>'reprotApi接口');
        $error_msg['plat']=$plat;
        $error_msg['account_id']=$account_id;
        $error_msg['day']=$dayid;
        $error_msg['now'] = $now;
        $error_msg['url'] = $url;
        if(!empty($data))
            $error_msg['post_data'] = $data;
        $error_msg = json_encode($error_msg);
        $addr = $ini_array['graylog_addr']['addr'];
        $exec = "curl -X POST {$addr} -p0 -d '{$error_msg}'";
        var_dump($exec);
        exec($exec,$return);
        return $return;
    }


    // 发送邮件
    public static function sendMail($message_info = [], $subject = '数据平台取数error'){

        //邮件内容
        $to = env('MAIL_TO_NAME');
        Mail::send('emails.test',['name' => env('MAIL_FROM_NAME'),'info' => json_encode($message_info)], function ($message) use($to, $subject){
            $message
                ->to($to)
                ->subject($subject);
        });
    }

    //发送邮件
    public static function sendCustomMail($message,$title,$filename,$mailTo='data_error@zplay.com'){

        $mail = new PHPMailer(true);
        $mail->SMTPDebug=true ;
        $mail->IsSMTP();
        $mail->SMTPAuth = true;
        $mail->CharSet='UTF-8'; //设置邮件的字符编码
        $mail->Port = 465;
        $mail->SMTPSecure = "ssl";
        $mail->Host = 'smtp.exmail.qq.com';
        $mail->FromName = 'Data-Analysis';
        $mail->Username = 'Data-Analysis@zplay.com';
        $mail->Password = 'Zplay1';
        $mail->From = 'Data-Analysis@zplay.com';
        $mail->IsHTML(true);
        $mail->AddAddress($mailTo);
        $mail->Subject = $title;
        $mail->Body = $message;
        $mail->AddAttachment(storage_path($filename));
        $mail->AltBody = "To view the message, please use an HTML compatible email viewer!"; //当邮件不支持html时备用显示，可以省略
        $status = $mail->Send();
        echo '邮件已发送'.$status;
    }


    public static function multi_array_sort($multi_array,$sort_key,$sort=SORT_ASC){
        if(is_array($multi_array)){
            foreach ($multi_array as $row_array){
                if(is_array($row_array)){
                    $key_array[] = $row_array[$sort_key];
                }else{
                    return false;
                }
            }
        }else{
            return false;
        }
        array_multisort($key_array,$sort,$multi_array);
        return $multi_array;
    }


//    public static function getPlatData($fileInfo,$dayid){
//
//        //$sql = "select sum("."$fileInfo['revenue_field']".") as  income,count(1) c,"."$fileInfo['account_field']"." from $fileInfo['table_name'] where date("."$fileInfo['day_field']".")='{$day}'";'
//
//        $sql = "select date({$fileInfo['day_field']}) dayid, sum({$fileInfo['revenue_field']}) as income,count(1)as c,{$fileInfo['account_field']} as  account_id from {$fileInfo['table_name']} where date({$fileInfo['day_field']}) in ({$dayid})";
//        if($fileInfo['account_field']){
//            $sql.=" group by {$fileInfo['day_field']}, {$fileInfo['account_field']}";
//        }
//        else $sql.=" group by {$fileInfo['day_field']}";
//// //   $result = query ( $sql );
//        $re =  mysql_query("$sql");
//        if($re){
//            while ($res = mysql_fetch_array($re, MYSQL_ASSOC)) {
//                $ret[] = $res;
//            }
//        }
//        return $ret;
//    }
}