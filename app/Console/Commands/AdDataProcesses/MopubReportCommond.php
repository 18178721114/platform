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

class MopubReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'MopubReportCommond {dayid?} {appid?}';

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
    	$dayid = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-2 day'));
    	$appid = $this->argument('appid')?$this->argument('appid'):'';
    	var_dump('MoPub-pad34-'.$dayid);

        define('AD_PLATFORM', 'MoPub');
        define('SCHEMA', 'ad_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'pad34'); // todo 这个需要根据平台信息表确定平台ID
        try{

        // todo  数据库配置
//        $PlatInfo = DataImportLogic::getConf(SOURCE_ID_CONF);
//        $PlatInfo = Service::data($PlatInfo);

        $sql = " SELECT  data_account as company_account,account_api_key  as api_key,account_user_id  as CAMPAIGN_REPORT_ID from c_platform_account_mapping WHERE platform_id ='pad34' and status = 1 and account_user_id <> '' and account_user_id is not null";
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

            $reprot_key = $value['CAMPAIGN_REPORT_ID'];
            $company_account = $value['company_account'];
            $api_key = $value['api_key'];
            $url = "https://app.mopub.com/reports/custom/api/download_report?report_key={$reprot_key}&api_key={$api_key}&date={$dayid}";
            $datalist = self::get_response($url);
            $i=1;
            while(!$datalist){
                $datalist = self::get_response($url);
                $i++;
                if($i>6)
                    break;
            }
            $data = self::parse_csv($datalist);

            if ($data){
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
                foreach ($data as  $appInfo) {

                        $insert_data[$index]['account'] = $value['company_account'];
                        $insert_data[$index]['type'] = 2;
                        $insert_data[$index]['source_id'] = SOURCE_ID;
                        $insert_data[$index]['dayid'] = $dayid;
                        $insert_data[$index]['json_data'] =str_replace('\'','\'\'',json_encode($appInfo));
                        $insert_data[$index]['create_time'] = date("Y-m-d H:i:s");
                        $insert_data[$index]['year'] = date("Y",strtotime($dayid));
                        $insert_data[$index]['month'] = date("m",strtotime($dayid));
                        $insert_data[$index]['ad_id'] =$appInfo['adunit_id'];
                        $insert_data[$index]['ad_name'] =$appInfo['adunit'];
                        $insert_data[$index]['app_id'] =$appInfo['app_id'];
                        $insert_data[$index]['app_name'] =$appInfo['app'];
                        $insert_data[$index]['income'] =$appInfo['revenue'];
                        $index++;

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
                $error_msg = AD_PLATFORM.'广告平台'.$value['company_account'].'账号取数失败,错误信息:'.json_encode($datalist);
                DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);

                $error_msg_arr[] = $error_msg;
                CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'广告平台取数error');

                self::curl_grayLog('mopub',$company_account,$dayid,$url);
            }
    	}

        // 调用数据处理过程
            Artisan::call('MopubHandleProcesses',['dayid' => $dayid]);
        } catch (\Exception $e) {
            $error_msg_info = $dayid.'号,'.AD_PLATFORM.'广告平台程序失败，失败原因：'.$e->getMessage();
            DataImportImp::saveDataErrorLog(5,SOURCE_ID,AD_PLATFORM,2,$error_msg_info);

        }
    		
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

}
