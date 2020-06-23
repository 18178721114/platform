<?php

namespace App\Console\Commands\AdDataProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\Common\CommonFunction;
use App\Common\CurlRequest;
use App\Common\Service;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\DataImportLogic;

class GuangdiantongReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'GuangdiantongReportCommond {dayid?} {appid?}';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Command description';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        set_time_limit(0);
        header('content-type:text/html;charset=utf-8');
        // 入口方法
    	$dayid = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));
    	$appid = $this->argument('appid')?$this->argument('appid'):'';
    	var_dump($dayid);

        define('AD_PLATFORM', 'GDT');
        define('SCHEMA', 'ad_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID_CONF', '10026'); // todo 这个需要根据平台信息表确定平台ID
        define('SOURCE_ID', 'pad10'); // todo 这个需要根据平台信息表确定平台ID
        try{
        // todo  数据库配置
//        $PlatInfo = DataImportLogic::getConf(SOURCE_ID_CONF);
//        $PlatInfo = Service::data($PlatInfo);

        $sql = "SELECT  data_account as company_account,account_api_key  as appkey,account_user_id  as agid,account_app_id  as appid from c_platform_account_mapping WHERE platform_id ='pad10'";
        $PlatInfo = DB::select($sql);
        $PlatInfo = Service::data($PlatInfo);

        if (!$PlatInfo){
            $message = "{$dayid}, " . AD_PLATFORM . "广告平台取数失败,失败原因:取数配置信息为空" ;
            DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$message);

            $error_msg_arr = [];
            $error_msg_arr[] = $message;
            CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'广告平台取数error');
            exit;
        }

    	foreach ($PlatInfo as $key => $value) {

            $appkey = $value['appkey'];
            $agid = $value['agid'];
            $appid = $value['appid'];

            $time = time();
    		$sign = sha1($appid . $appkey . $time);
    		$token = base64_encode($agid . ',' . $appid . ',' . $time . ',' . $sign);

    		$post_data['token'] = $token;
    		$post_data['memberId'] = $value['company_account'];

    		$post_data['start_date'] = date('Ymd', strtotime($dayid));
    		$post_data['end_date'] = date('Ymd', strtotime($dayid));
    		$post_data['placement_name'] = '';
    		$post_data['placement_type'] = '';
    		$post_data['medium_name'] = '';
    		$post_data['agid'] = $agid;
    		$post_data['appid'] = $appid;
    		$post_data['key'] = $appkey;

    		$url =env('GUANGDIANTONG_URL').'gdt_tx_service.php';
    		self::zplay_curl($url, 'post', $post_data);
    		$info_url =env('GUANGDIANTONG_URL').$value['company_account'].'.txt';
    		$content = self::get_response($info_url);
    		$content = json_decode($content, true);
         	//获取应用信息
		    if ($content['msg'] == 'Success') {//成功取到数
    			//删除数据库里原来数据
		    	$map['dayid'] = $dayid;
		    	$map['source_id'] = SOURCE_ID;
		    	$map['account'] = $value['company_account'];
                $count = DataImportLogic::getChannelData('ad_data','erm_data',$map)->count();
                if($count>0){ 

            	//删除数据
		    	     DataImportLogic::deleteHistoryData('ad_data','erm_data',$map);
                }
		    	$index =0;
                $insert_data =[];
                $step =[];
		    	foreach ($content['data'] as $v){
		    		$insert_data[$index]['app_id'] = $v['AppId'];
		    		$insert_data[$index]['account'] = $value['company_account'];
		    		$insert_data[$index]['type'] = 2;
		    		$insert_data[$index]['source_id'] = SOURCE_ID;
		    		$insert_data[$index]['dayid'] = $dayid;
		    		$insert_data[$index]['json_data'] =str_replace('\'','\'\'',json_encode($v));
		    		$insert_data[$index]['create_time'] = date("Y-m-d H:i:s");
		    		$insert_data[$index]['year'] = date("Y",strtotime($dayid));
		    		$insert_data[$index]['month'] = date("m",strtotime($dayid));
                    $insert_data[$index]['ad_id'] =$v['PlacementId'];
                    $insert_data[$index]['ad_name'] =$v['PlacementName'];
                    $insert_data[$index]['income'] = isset($v['Revenue']) ? floatval(str_replace(',','',strval($v['Revenue']))) : 0;
		    		$index++;
		    	}
		    	$i = 0;

		    	foreach ($insert_data as $kkkk => $insert_data_info) {
		    		if ($kkkk % 2000 == 0) $i++;
		    		if ($insert_data_info) {
		    			$step[$i][] = $insert_data_info;
		    		}
		    	}

		    	if ($step) {
		    		foreach ($step as $k => $v) {
		    			$result = DataImportLogic::insertChannelData('ad_data','erm_data',$v);
		    			if (!$result) {
		    				 echo 'mysql_error'. PHP_EOL;
		    			}
		    		}
		    	}

		    } else {

                $error_msg = AD_PLATFORM.'广告平台'.$value['company_account'].'账号取数失败,错误信息:'.$content['msg'];
                DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);

                $error_msg_arr = [];
                $error_msg_arr[] = $error_msg;
                CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'广告平台取数error');

		    }
    	}

        // 调用数据处理过程
            Artisan::call('GuangdiantongHandleProcesses',['dayid' => $dayid]);
        } catch (\Exception $e) {
            $error_msg_info = $dayid.'号,'.AD_PLATFORM.'渠道数据匹配失败：'.$e->getMessage();
            DataImportImp::saveDataErrorLog(5,SOURCE_ID,AD_PLATFORM,2,$error_msg_info);

        }
    		
    }
    public static function get_response($url, $headers='')
    {
    	$ch = curl_init();
    	curl_setopt($ch, CURLOPT_URL, $url);

    	curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
    	curl_setopt($ch, CURLOPT_HEADER, 0);
    	curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
    	curl_setopt($ch, CURLOPT_TIMEOUT,120); 
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
}
