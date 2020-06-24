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

class ChangsiReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'ChangsiReportCommond {dayid?} {appid?}';

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

        define('AD_PLATFORM', '畅思');
        define('SCHEMA', 'ad_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID_CONF', '10023'); // todo 这个需要根据平台信息表确定平台ID
        define('SOURCE_ID', 'pad12'); // todo 这个需要根据平台信息表确定平台ID
        try{
        // todo  数据库配置
//        $PlatInfo = DataImportLogic::getConf(SOURCE_ID_CONF);
//        $PlatInfo = Service::data($PlatInfo);

        $sql = " SELECT  data_account as company_account,account_api_key  as api_key from c_platform_account_mapping WHERE platform_id ='pad12' and status = 1 ";
        $PlatInfo = DB::select($sql);
        $PlatInfo = Service::data($PlatInfo);

        if (!$PlatInfo){
            $message = "{$dayid}, " . AD_PLATFORM . " 广告平台取数失败,失败原因:取数配置信息为空" ;
            DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$message);
            $error_msg_arr[] = $message;
            CommonFunction::sendMail($error_msg_arr,'广告平台取数error');
            exit;
        }

    	foreach ($PlatInfo as $key => $value) {

            $access_key = $value['api_key'];

            $key = md5($access_key);
            $key = strtolower($key);

            $url = str_replace(array('_USERNAME_','_KEY_','_END_DATE_','_BEGIN_DATE_'),array($value['company_account'],$key,$dayid,$dayid),env('CHANGSI_URL'));
            $data = self::get_response($url);
            $ret = json_decode($data, true);

            // 数据获取重试
            $api_data_i=1;
            while(!$ret){
                $data = self::get_response($url);
                $ret = json_decode($data, true);
                $api_data_i++;
                if($api_data_i>3)
                    break;
            }
            //取数四次 取数结果仍为空
            if($api_data_i ==4 && empty($ret) ){
                $error_msg_1 = AD_PLATFORM.'广告平台'.$value['company_account'].'账号取数失败,错误信息:返回数据为空('.$data.')';
                DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg_1);
                continue;

            }

            if(isset($ret['status']) && $ret['status'] == 1){
                if ($ret['data']){
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
                    foreach ($ret['data'] as $appid => $appInfo) {
                        foreach ($appInfo as $adtypeid => $adtype_stats) {
                            $insert_data[$index]['account'] = $value['company_account'];
                            $insert_data[$index]['type'] = 2;
                            $insert_data[$index]['app_id'] = $appid;
                            $insert_data[$index]['source_id'] = SOURCE_ID;
                            $insert_data[$index]['dayid'] = $dayid;
                            $adtype_stats['list'][$dayid]['appname'] =$adtype_stats['appname'];
                            $adtype_stats['list'][$dayid]['adform'] =$adtype_stats['adform'];
                            $insert_data[$index]['json_data'] =str_replace('\'','\'\'',json_encode($adtype_stats['list'][$dayid]));
                            $insert_data[$index]['create_time'] = date("Y-m-d H:i:s");
                            $insert_data[$index]['year'] = date("Y",strtotime($dayid));
                            $insert_data[$index]['month'] = date("m",strtotime($dayid));
                            $insert_data[$index]['income'] = $adtype_stats['list'][$dayid]['income'];
                            $index++;
                        }
                    }
                    $index =0;
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
                    $error_msg = AD_PLATFORM.'广告平台'.$value['company_account'].'暂无数据';
                    DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);
                }

            }else{
                $error_msg = AD_PLATFORM.'广告平台'.$value['company_account'].'账号取数失败,错误信息:'.(isset($ret['error']) ? $ret['error'] : '返回数据为空');
                DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);

                $error_msg_arr[] = $error_msg;
                CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'广告平台取数error');
            }

    	}

        // 调用数据处理过程
            Artisan::call('ChangsiHandleProcesses',['dayid' => $dayid]);
        } catch (\Exception $e) {
            $error_msg_info = $dayid.'号,'.AD_PLATFORM.'广告平台程序失败，失败原因：'.$e->getMessage();
            DataImportImp::saveDataErrorLog(5,SOURCE_ID,AD_PLATFORM,2,$error_msg_info);

        }
    		
    }
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

}
