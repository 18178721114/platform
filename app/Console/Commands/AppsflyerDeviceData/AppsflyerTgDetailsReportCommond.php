<?php

namespace App\Console\Commands\AppsflyerDeviceData;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\Common\CurlRequest;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\DataImportLogic;
use Illuminate\Support\Facades\Artisan;
use App\Common\CommonFunction;
use App\Common\Service;

class AppsflyerTgDetailsReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'AppsflyerTgDetailsReportCommond {appkey?} {dayid?} {data_file?} {type?}';

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
        error_reporting(0);
        date_default_timezone_set("PRC");
    	set_time_limit(0);
//    	ini_set("memory_limit","2048M");
        header('content-type:text/html;charset=utf-8');
        // 入口方法
        $appkey = $this->argument('appkey')?$this->argument('appkey'):'';
        $dayid = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));
        $file = $this->argument('data_file')?$this->argument('data_file'):'';
        $type = $this->argument('type')?$this->argument('type'):'';
    	var_dump($dayid);
    	var_dump($file);
    	var_dump($type);
    	var_dump($appkey);

        define('AD_PLATFORM', 'Appsflyer');
        define('SCHEMA', 'appsflyer_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'ptg02'); // todo 这个需要根据平台信息表确定平台ID

        // todo 文件目录
        $fullFile = realpath("/data/appsflyer/" . $file);
//        $fullFile = realpath("/Users/zhenliye/Desktop/appsflury/" . $file);
        echo $fullFile . "\n";

        if (!file_exists($fullFile)) {
            echo "{$fullFile} no exists\n";
        }

        // todo 删除历史数据
        if ($type){
            //  删除自然量数据
            $map['dayid'] = $dayid;
            $map['appid'] = $appkey;
            $map['mediasource'] = 'organic';
            $count = DataImportLogic::getChannelData(SCHEMA,TABLE_NAME,$map)->count();
            var_dump("删除数据条数：".$count);
            if($count>0){
                //删除数据
                DataImportLogic::deleteHistoryData(SCHEMA,TABLE_NAME,$map);
            }

        }else{
            //  删除非自然量数据
            $map['dayid'] = $dayid;
            $map['appid'] = $appkey;
            $map[] = ['mediasource','<>','organic'];
            $count = DataImportLogic::getChannelData(SCHEMA,TABLE_NAME,$map)->count();
            var_dump("删除数据条数：".$count);
            if($count>0){
                //删除数据
                DataImportLogic::deleteHistoryData(SCHEMA,TABLE_NAME,$map);
            }
        }

        $content = file_get_contents($fullFile);
        $data_array = self::parse_csv($content);
        var_dump(count($data_array));
        if ($data_array) {
            foreach ($data_array as $data_array_key => $data_array_value) {

                if ($type) {
                    $data_array[$data_array_key]['mediasource'] = 'organic';
                }
                $data_array[$data_array_key]['dayid'] = $dayid;
                $data_array[$data_array_key]['year'] = date("Y", strtotime($dayid));
                $data_array[$data_array_key]['month'] = date("m", strtotime($dayid));
                $data_array[$data_array_key]['originalurl'] = '';
                $data_array[$data_array_key]['httpreferrer'] = '';
            }

            $i = 0;
            $step = [];
            foreach ($data_array as $kkkk => $insert_data_info) {
                if ($kkkk % 500 == 0) $i++;
                if ($insert_data_info) {
                    $step[$i][] = $insert_data_info;
                }
            }

            if ($step) {
                foreach ($step as $k => $v) {
                    $result = DataImportLogic::insertChannelData(SCHEMA, TABLE_NAME, $v);
                    if (!$result) {
                        echo 'mysql_error' . PHP_EOL;
                    }
                }
            }
        }

//        Artisan::call('AppsflyerTgHandleProcesses',['dayid'=>$dayid]);
    }

    public static function parse_csv($content){
    	$data = explode("\n", trim($content, "\n"));
    	$data = array_map('str_getcsv', $data);
    	if (isset($data[1])) {
    		$filed = array_map(function ($value) {
    			return strtolower(preg_replace('/\s+/', '', $value));
    		}, $data[0]);

            // 无数据判断 字段 不足81  无数据
            if (count($filed) < 81)  return [];

    		unset($data[0]);
    		foreach ($data as &$value) {
                if(count($filed) ==count($value) ){
                    $value = array_combine($filed, $value);

                }else{
                     file_put_contents('./storage/tgDataLogs/appsflyer.log',date('Y-m-d H:i:s').json_encode($filed)."\n",FILE_APPEND);
                     file_put_contents('./storage/tgDataLogs/appsflyer.log',date('Y-m-d H:i:s').json_encode($value)."\n",FILE_APPEND);
                }
    			
    		}
    		unset($value);

    		return $data;
    	}
    }
    public static function get_response($url)
    {
    	$ch = curl_init();
    	curl_setopt($ch, CURLOPT_URL, $url);

    	curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
    	curl_setopt($ch, CURLOPT_HEADER, 0);
    	curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
		curl_setopt($ch, CURLOPT_TIMEOUT,120); //超时时间  秒
		curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, FALSE);
		curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, FALSE);
		$output = curl_exec($ch);
		curl_close($ch);
		return $output;
	}
}
