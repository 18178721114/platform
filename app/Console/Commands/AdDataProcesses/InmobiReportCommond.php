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

class InmobiReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'InmobiReportCommond {dayid?} {appid?}';

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
       // ini_set('memory_limit','100M');
        header('content-type:text/html;charset=utf-8');
        // 入口方法
        $dayid = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));
        var_dump($dayid);

        define('AD_PLATFORM', 'Inmobi');
        define('SCHEMA', 'ad_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'pad02'); // todo 这个需要根据平台信息表确定平台ID

        // todo 这里面要写新测试平台里的数据配置 从数据库里取数据

//        $info[0]['company_account'] ='arlin@noodlecake.com';
//        $info[0]['company_pass'] ='noodlecake';
//        $info[0]['secretKey'] ='b2ff61a60ef444d98e36af5c1b5696c0';
//        $info[1]['company_account'] ='global@yumimobi.com';
//        $info[1]['company_pass'] ='Godblesszplay1';
//        $info[1]['secretKey'] ='108820daaa0b48f4b8ba59752697ea78';
//        $info[2]['company_account'] ='yanxiaoyu@zplay.cn';
//        $info[2]['company_pass'] ='zplaygogogo1';
//        $info[2]['secretKey'] ='b046c4fe1d6742faa40764ff39f82bf9';
//
        $sql = "SELECT  data_account as company_account,account_pass  as company_pass,account_token  as secretKey from c_platform_account_mapping WHERE platform_id ='pad02' ";
        $info = DB::select($sql);
        $info = Service::data($info);
        if (!$info) return;

        foreach ($info as $key => $value) {
            $data =self::getContents1($dayid,$dayid,$value);
            $data = json_decode($data,true);
            if (!$data['error']) {
                $arr =isset($data ['respList'])?$data ['respList']:[];
                $map['dayid'] = $dayid;
                $map['source_id'] = SOURCE_ID;
                $map['account'] = $value['company_account'];
                $count = DataImportLogic::getChannelData('ad_data','erm_data',$map)->count();
                if($count>0){
                //删除数据
                    DataImportLogic::deleteHistoryData('ad_data','erm_data',$map);
                }
                $insert_data =[];
                $index=0;
                foreach ( $arr as $k => $v ) {
                    $insert_data[$index]['account'] = $value['company_account'];
                    $insert_data[$index]['type'] = 2;
                    $insert_data[$index]['source_id'] = SOURCE_ID;
                    $insert_data[$index]['dayid'] = $dayid;
                    $insert_data[$index]['json_data'] =str_replace('\'','\'\'',json_encode($v));
                    $insert_data[$index]['create_time'] = date("Y-m-d H:i:s");
                    $insert_data[$index]['year'] = date("Y",strtotime($dayid));
                    $insert_data[$index]['month'] = date("m",strtotime($dayid));
                    $insert_data[$index]['ad_id'] =$v['siteId'];
                    $insert_data[$index]['ad_name'] =$v['siteName'];
                    $insert_data[$index]['app_id'] =$v['placementId'];
                    $insert_data[$index]['app_name'] =$v['placementName'];
                    $insert_data[$index]['income'] =$v['earnings'];
                    $index++;

                }
                $i = 0;
                $step=[];
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
                $error_msg = AD_PLATFORM.'广告平台'.$value['company_account'].'账号取数失败,错误信息:';
                if (isset($data['errorList']) && $data['errorList']){
                    $error_msg .= $data['errorList'][0]['message'];
                }else{
                    $error_msg .= '未知';
                }
                DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);

                $error_msg_arr = [];
                $error_msg_arr[] = $error_msg;
                CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'广告平台取数error');
            }

        }
         Artisan::call('InmobiHandleProcesses' ,['dayid'=>$dayid]);

    }


    public static function getContents1($stime, $etime, $inmobi_info) {
        $ch = curl_init ();
        curl_setopt ( $ch, CURLOPT_URL, "https://api.inmobi.com/v1.0/generatesession/generate" );
        curl_setopt ( $ch, CURLOPT_RETURNTRANSFER, 1 );
        curl_setopt ( $ch, CURLOPT_HEADER, 0 );
        $header = array (
            "userName:{$inmobi_info['company_account']}",
            "password:{$inmobi_info['company_pass']}",
            "secretKey:{$inmobi_info['secretKey']}",
            "Content-type:application/json"
        );

        curl_setopt ( $ch, CURLOPT_SSL_VERIFYHOST, 2 );
        curl_setopt ( $ch, CURLOPT_SSL_VERIFYPEER, false );
        curl_setopt ( $ch, CURLOPT_HTTPHEADER, $header );
        $output = curl_exec ( $ch );
        curl_close ( $ch );
        $ret = json_decode ( $output ,true);
        if (!$ret['respList']){
            $error_list = $ret['errorList'][0];
            $error_msg = $error_list['message'];
            $error_msg = AD_PLATFORM.'平台'.$inmobi_info['company_account'].'账号获取sessionId、accountId数据失败,错误信息:'.$error_msg;
            DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);
        }else{
            $rew = $ret['respList'][0];
            $sessionId = $rew['sessionId'];
            $accountId = $rew['accountId'];
            $ch = curl_init ();
            curl_setopt ( $ch, CURLOPT_URL, "https://api.inmobi.com/v1.1/reporting/publisher.json" );
            curl_setopt ( $ch, CURLOPT_RETURNTRANSFER, 1 );
            curl_setopt ( $ch, CURLOPT_POST, true );
            curl_setopt ( $ch, CURLOPT_HEADER, 0 );
            $header = array (
                'Content-type:application/json',
                "accountId:$accountId",
                "secretKey:{$inmobi_info['secretKey']}",
                "sessionId:$sessionId"
            );
            $data = '{"reportRequest": {
	    		"metrics": [
	    		"clicks",
	    		"earnings",
	    		"adImpressions",
	    		"adRequests",
	    		"CTR",
	    		"costPerClick"
	    		],
	    		"timeFrame": "' . $stime . ':' . $etime . '",
	    		"groupBy": [
	    		"country",
	    		"site",
	    		"date",
	    		"account",
	    		"placement",
	    		],
	    		"orderBy": [
	    		"date"
	    		],
	    		"orderType": "desc",
	    		"offset": 0,
	    		"length": 40000
	    	}
	    }';

            curl_setopt ( $ch, CURLOPT_POSTFIELDS, $data );
            curl_setopt ( $ch, CURLOPT_SSL_VERIFYHOST, 0 );
            curl_setopt ( $ch, CURLOPT_SSL_VERIFYPEER, false );
            curl_setopt ( $ch, CURLOPT_HTTPHEADER, $header );
            $output = curl_exec ( $ch );
            curl_close ( $ch );
            return $output;
        }

    }
}
