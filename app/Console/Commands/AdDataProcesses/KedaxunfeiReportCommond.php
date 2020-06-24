<?php

namespace App\Console\Commands\AdDataProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\Common\CommonFunction;
use App\Common\CurlRequest;
use App\Common\Service;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\DataImportLogic;
use Illuminate\Support\Facades\Artisan;

class KedaxunfeiReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'KedaxunfeiReportCommond {dayid?} {appid?}';

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

        define('AD_PLATFORM', 'KeDaXunFei');
        define('SCHEMA', 'ad_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'pad29'); // todo 这个需要根据平台信息表确定平台ID
        try{
        //这里面要写新测试平台里的数据配置 从数据库里取数据
//    	$info[0]['company_account'] ='weibo@zplay.com';
//    	$info[0]['accessKey'] ='324ed4a52f1b71ff';
//        $info[1]['company_account'] ='global@yumimobi.com';
//        $info[1]['accessKey'] ='26b2d6afdf20f9c6';

        $sql = "SELECT  data_account as company_account,account_api_key  as accessKey from c_platform_account_mapping WHERE platform_id ='pad29' and status = 1";
        $info = DB::select($sql);
        $info = Service::data($info);
        if ($info) {
            foreach ($info as $key => $value) {
                $url = env('KEDAXUNFEI_URL');
                $sign = md5($value['company_account'] . $dayid . $dayid . $value['accessKey']);
                $post_data = array("email" => $value['company_account'], 'start_date' => $dayid, 'end_date' => $dayid, 'sign' => $sign);
                //echo $url . PHP_EOL;
                $data = self::zplay_curl($url, 'post', $post_data);

                // 数据获取重试
                $api_data_i=1;
                while(!$data){
                    $data = self::zplay_curl($url, 'post', $post_data);
                    $api_data_i++;
                    if($api_data_i>3)
                        break;
                }

                if (!$data['msg']) {
                    //删除数据库里原来数据
                    $map['dayid'] = $dayid;
                    $map['source_id'] = SOURCE_ID;
                    $map['account'] = $value['company_account'];

                    $count = DataImportLogic::getChannelData('ad_data','erm_data',$map)->count();
                    if($count>0){

                    //删除数据
                        DataImportLogic::deleteHistoryData('ad_data', 'erm_data', $map);
                    }
                    $index = 0;
                    $insert_data = [];
                    $step = [];
                    foreach ($data['data'] as $v) {
                        $insert_data[$index]['account'] = $value['company_account'];
                        $insert_data[$index]['type'] = 2;
                        $insert_data[$index]['source_id'] = SOURCE_ID;
                        $insert_data[$index]['dayid'] = $dayid;
                        $insert_data[$index]['json_data'] = str_replace('\'', '\'\'', json_encode($v));
                        $insert_data[$index]['create_time'] = date("Y-m-d H:i:s");
                        $insert_data[$index]['year'] = date("Y", strtotime($dayid));
                        $insert_data[$index]['month'] = date("m", strtotime($dayid));
                        $insert_data[$index]['ad_id'] =$v['adunit_id'];
                        $insert_data[$index]['ad_name'] =$v['adunit_name'];
                        $insert_data[$index]['app_id'] =$v['appid'];
                        $insert_data[$index]['app_name'] =$v['app_name'];
                        $insert_data[$index]['income'] =$v['income'];
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
                            $result = DataImportLogic::insertChannelData('ad_data', 'erm_data', $v);
                            if (!$result) {
                                echo 'mysql_error' . PHP_EOL;
                            }
                        }

                    }

                } else {
                    $error_msg = AD_PLATFORM . '广告平台' . $value['company_account'] . '账号取数失败,错误信息:' . $data['msg'];
                    DataImportImp::saveDataErrorLog(1, SOURCE_ID, AD_PLATFORM, 2, $error_msg);

                    $error_msg_arr = [];
                    $error_msg_arr[] = $error_msg;
                    CommonFunction::sendMail($error_msg_arr, AD_PLATFORM . '广告平台取数error');
                }

            }
            Artisan::call('KedaxunfeiHandleProcesses', ['dayid' => $dayid]);
        }
        } catch (\Exception $e) {
            $error_msg_info = $dayid.'号,'.AD_PLATFORM.'广告平台程序失败，失败原因：'.$e->getMessage();
            DataImportImp::saveDataErrorLog(5,SOURCE_ID,AD_PLATFORM,2,$error_msg_info);

        }
    		
    }
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
