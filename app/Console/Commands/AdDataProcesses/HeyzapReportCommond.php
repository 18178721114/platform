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

class HeyzapReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'HeyzapReportCommond {dayid?} {appid?}';

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
        //ini_set('memory_limit','100M');
        header('content-type:text/html;charset=utf-8');
        // 入口方法
    	$dayid = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));
    	$appid = $this->argument('appid')?$this->argument('appid'):'';

        define('AD_PLATFORM', 'Heyzap');
        define('SCHEMA', 'ad_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID_CONF', '10051'); // todo 这个需要根据平台信息表确定平台ID
        define('SOURCE_ID', 'pad36'); // todo 这个需要根据平台信息表确定平台ID
        try{
        // todo  数据库配置
//        $PlatInfo = DataImportLogic::getConf(SOURCE_ID_CONF);
//        $PlatInfo = Service::data($PlatInfo);

        $sql = "SELECT  data_account as company_username,account_api_key  as api_key from c_platform_account_mapping WHERE platform_id ='pad36'";
        $PlatInfo = DB::select($sql);
        $PlatInfo = Service::data($PlatInfo);

        if (!$PlatInfo){
            $message = "{$dayid},".AD_PLATFORM."广告平台取数失败,失败原因:取数配置信息为空" ;
            DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$message);

            $error_msg_arr = [];
            $error_msg_arr[] = $message;
            CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'广告平台取数error');
            exit;
        }

    	foreach ($PlatInfo as $key => $value) {
        	//获取应用信息
//            $key_list = json_decode($value['key_list'],true);
            $api_key = $value['api_key'];
    		$url = str_replace(array('_COMPANY_USERNAME_','_API_KEY_'),array($value['company_username'],$api_key),env('HEYZAP_APP'));
    		$appInfoList =self::get_response($url);
    		$appInfoList = json_decode($appInfoList,true);
    		if(!empty($appInfoList['data'])){
    			foreach ($appInfoList['data'] as $appInfo){
    				$dataUrl = str_replace(array('_COMPANY_USERNAME_','_API_KEY_','_END_DATE_','_BEGIN_DATE_','_APP_ID_'),array($value['company_username'],$api_key,$dayid,$dayid,$appInfo['app_id']),env('HEYZAP_INFO'));
    				$dataInfo = self::get_response($dataUrl);
    				$dataInfo = json_decode($dataInfo,true);
    				if(!empty($dataInfo['data'])){
    					$map['dayid'] = $dayid;
    					$map['source_id'] = SOURCE_ID;
    					$map['account'] = $value['company_username'];
    					$map['app_id'] = $appInfo['app_id'];
                        $count = DataImportLogic::getChannelData('ad_data','erm_data',$map)->count();
                        if($count>0){

                    	   //删除数据
                    	   DataImportLogic::deleteHistoryData('ad_data','erm_data',$map);
                        }
                        $index = 0;
                        $insert_data =[];
                        $step =[];

    					foreach ($dataInfo['data'] as $data){
    							$appName = addslashes($data['app_name']);

                                foreach ($data['data'] as $k => $v) {
                                	$data['data'][$k]['app_id'] = $data['app_id'];
                                	$data['data'][$k]['app_name'] = $appName;
                                	$data['data'][$k]['network'] = $data['network'];
                                	$data['data'][$k]['platform'] = $data['platform'];
                                	$data['data'][$k]['format'] = $data['format'];
                                }

                                foreach ($data['data'] as $k1 => $v1) {
                                    if($data['network'] !='heyzap'){
                                        continue;
                                    }
                                    $insert_data[$index]['account'] = $value['company_username'];
                                    $insert_data[$index]['type'] = 2;
                                    $insert_data[$index]['app_name'] = $appName;
                                    $insert_data[$index]['app_id'] = $data['app_id'];
                                    $insert_data[$index]['source_id'] = SOURCE_ID;
                                    $insert_data[$index]['dayid'] = $dayid;
                                    $insert_data[$index]['json_data'] =str_replace('\'','\'\'',json_encode($v1));
                                    $insert_data[$index]['create_time'] = date("Y-m-d H:i:s");
                                    $insert_data[$index]['year'] = date("Y",strtotime($dayid));
                                    $insert_data[$index]['month'] = date("m",strtotime($dayid));
                                    $insert_data[$index]['income'] =$v1['revenue_in_cents'];
                                    $index++;
                                    # code...
                                }


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
    				}else{
    					if(count($dataInfo['data']) ==0){
//                            $error_msg = AD_PLATFORM.'广告平台'.$value['company_username'].'账号'.'获取应用id为：'.$appInfo['app_id'].'数据为空'. PHP_EOL;
//                            DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);

    					}else{
                            $error_msg =  AD_PLATFORM.'广告平台'.$value['company_username'].'账号'.'获取应用id为：'.$appInfo['app_id'].'数据失败'. PHP_EOL;
                            DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);

                            $error_msg_arr = [];
                            $error_msg_arr[] = $error_msg;
                            CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'广告平台取数error');
    					}

    				}
    			}
    		}else {
                $error_msg = AD_PLATFORM.'广告平台'.$value['company_username'].'账号获取应用数据失败,错误信息:'.$appInfoList['message'];
                DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);
    		}
    	}

    	sleep(60);
        // todo 调用数据处理过程
            Artisan::call('HeyzapHandleProcesses',['dayid' => $dayid]);
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
}
