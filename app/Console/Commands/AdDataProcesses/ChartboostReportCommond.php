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
use Symfony\Component\VarDumper\Cloner\Data;

class ChartboostReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'ChartboostReportCommond {dayid?} {appid?}';

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

        define('AD_PLATFORM', 'Chartboost');
        define('SCHEMA', 'ad_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID_CONF', '10006'); // todo 这个需要根据平台信息表确定平台ID
        define('SOURCE_ID', 'pad03'); // todo 这个需要根据平台信息表确定平台ID

        // todo  数据库配置
//        $PlatInfo = DataImportLogic::getConf(SOURCE_ID_CONF);
//        $PlatInfo = Service::data($PlatInfo);
        $sql = "SELECT  data_account as company_account,account_user_id  as user_id,account_token  as user_signature from c_platform_account_mapping WHERE platform_id ='pad03' ";
        $PlatInfo = DB::select($sql);
        $PlatInfo = Service::data($PlatInfo);

        if (!$PlatInfo){
            $message = "{$dayid}, " . AD_PLATFORM . " 广告平台取数失败,失败原因:取数配置信息为空" ;
            DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$message);
            $error_msg_arr = [];
            $error_msg_arr[] = $message;
            CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'广告平台取数error');
            exit;
        }

    	foreach ($PlatInfo as $key => $value) {

            $user_id = $value['user_id'];
            $user_signature = $value['user_signature'];

    		$url = str_replace(array('_USERID_','_USER_SIGNATURE_','_END_DATE_','_BEGIN_DATE_'),array($user_id,$user_signature,$dayid,$dayid),env('CHARTBOOST_URL'));
    		echo $url;

    		$data = self::getContent($url,$value['company_account']);
    		if ($data) {
        			//删除数据库里原来数据
                $map['dayid'] = $dayid;
                $map['source_id'] = SOURCE_ID;
                $map['account'] = $value['company_account'];
                $count = DataImportLogic::getChannelData('ad_data','erm_data',$map)->count();
                if($count>0){ 
                	//删除数据
    		    	DataImportLogic::deleteHistoryData(SCHEMA,TABLE_NAME,$map);
                }
		    	$index =0;
                $insert_data =[];
                $step =[];
		    	foreach ($data as $v){
		    		$insert_data[$index]['account'] = $value['company_account'];
		    		$insert_data[$index]['type'] = 2;
		    		$insert_data[$index]['source_id'] = SOURCE_ID;
		    		$insert_data[$index]['dayid'] = $dayid;
		    		$insert_data[$index]['json_data'] =str_replace('\'','\'\'',json_encode($v));
		    		$insert_data[$index]['create_time'] = date("Y-m-d H:i:s");
		    		$insert_data[$index]['year'] = date("Y",strtotime($dayid));
		    		$insert_data[$index]['month'] = date("m",strtotime($dayid));
		    		$insert_data[$index]['ad_id'] =$v['appId'];
                    $insert_data[$index]['ad_name'] =$v['app'];
                    $insert_data[$index]['income'] =$v['moneyEarned'];
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
		    			$result = DataImportLogic::insertChannelData(SCHEMA,TABLE_NAME,$v);
		    			if (!$result) {
		    				 echo 'mysql_error'. PHP_EOL;
		    			}
		    		}
		    	}

    		}else{
                $error_msg = 'Chartboos广告平台'.$value['company_account'].'账号取数失败';
                DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);
                $error_msg_arr = [];
                $error_msg_arr[] = $error_msg;
                CommonFunction::sendMail($error_msg_arr,'广告平台取数error');
    		}
    	}

        // 调用数据处理过程
        Artisan::call('ChartboostHandleProcesses',['dayid' => $dayid]);
    		
    }
    public static function getContent($url,$company_account)
    {
    	static $degree = 0;

    	if (!$content = CurlRequest::get_response($url)) {
    		if ($degree > 1) {
    			return false;
    		}
    		$degree++;
    		sleep(2);
    		return self::getContent($url,$company_account);
    	}

    	$data = json_decode($content, true);
    	if (!$data || isset($data['status'])) {

            $error_msg = 'Chartboos广告平台'.$company_account.'账号取数失败,错误信息:'.isset($data['message']);
            DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);
            $error_msg_arr = [];
            $error_msg_arr[] = $error_msg;
            CommonFunction::sendMail($error_msg_arr,'数据平台取数error');

    		if ($degree > 1) {
    			return false;
    		}
    		$degree++;
    		sleep(1);
    		return self::getContent($url,$company_account);
    	}

    	return $data;
    }

}
