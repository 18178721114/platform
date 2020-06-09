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

class UnityReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'UnityReportCommond {dayid?} {appid?}';

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
        header('Content-Type: text/html; charset=utf-8');
        // 入口方法
        $dayid = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));
        $appid = $this->argument('appid')?$this->argument('appid'):'';

        define('AD_PLATFORM', 'Unity');
        define('SCHEMA', 'ad_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'pad24'); // todo 这个需要根据平台信息表确定平台ID

        //这里面要写新测试平台里的数据配置 从数据库里取数据
//        $info[0]['company_account'] ='weibo@zplay.com';
//        $info[0]['SecretKey'] ='4606dd2e5281f10f42a285125c7587725f950880fcff56a993ed669ac0369b8f';
//        $info[1]['company_account'] ='global@yumimobi.com';
//        $info[1]['SecretKey'] ='74260d9bf25dc8846bbd0332e7a136bf316ce0d9b00ccbc936b8e6545b8b4e56';
//        $info[2]['company_account'] ='noodlecake';
//        $info[2]['SecretKey'] ='006c90593aea438ef12e1023c6544666e87da1e919a76d647ce7b3267e5aca12';

        $sql = " SELECT  data_account as company_account,account_token  as SecretKey,account_user_id as organizationId from c_platform_account_mapping WHERE platform_id ='pad24' and account_user_id is not null ";
        $info = DB::select($sql);
        $info = Service::data($info);
//        var_dump($info);
        if ($info){
            foreach ($info as $key => $value) {
//                if ($value['company_account'] == 'kimmihua@togethergames.com') continue;
                $apikey = $value['SecretKey'];
                $organizationId = $value['organizationId'];

                $url = "https://monetization.api.unity.com/stats/v1/operate/organizations/{$organizationId}?groupBy=country,placement,game&fields=adrequest_count,available_sum,revenue_sum,start_count,view_count&scale=day&start={$dayid}T00:00:00Z&end={$dayid}T23:59:59Z&apikey={$apikey}";

//                var_dump($url);
                $data =self::getContent($url);
//                var_dump(count($data));

                if (!$data) {
                    $error_msg = AD_PLATFORM.'广告平台'.$value['company_account'].'账号取数失败,错误信息:账号、SecretKey有误';
                    DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,2,$error_msg);

                    $error_msg_arr = [];
                    $error_msg_arr[] = $error_msg;
                    CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'广告平台取数error');

                    echo "{$dayid}：{$value['company_account']} 取数失败" . PHP_EOL;
                    continue;
                } else {

                    $map['dayid'] = $dayid;
                    $map['source_id'] = SOURCE_ID;
                    $map['account'] = $value['company_account'];
                    $count = DataImportLogic::getChannelData('ad_data','erm_data',$map)->count();
                    if($count>0){
                    //删除数据
                        DataImportLogic::deleteHistoryData('ad_data','erm_data',$map);
                    }
                    $index = 0;
                    $insert_data =[];
                    $step =[];
                    foreach ($data as  $v1) {
                        //去应用接口和数据接口 进行数据拼接
                        $insert_data[$index]['account'] = $value['company_account'];
                        $insert_data[$index]['type'] = 2;
                        $insert_data[$index]['source_id'] = SOURCE_ID;
                        $insert_data[$index]['dayid'] = $dayid;
                        $insert_data[$index]['json_data'] =str_replace('\'','\'\'',json_encode($v1));
                        $insert_data[$index]['create_time'] = date("Y-m-d H:i:s");
                        $insert_data[$index]['year'] = date("Y",strtotime($dayid));
                        $insert_data[$index]['month'] = date("m",strtotime($dayid));
                        $insert_data[$index]['app_id'] = isset($v1['source_game_id']) ? $v1['source_game_id'] : '';
                        $insert_data[$index]['app_name'] = isset($v1['source_name']) ? $v1['source_name'] : '';
                        $insert_data[$index]['income'] = isset($v1['revenue_sum']) ? $v1['revenue_sum'] : 0;
                        $index++;
                    }
                    //批量插入
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

                }
            }
            Artisan::call('UnityHandleProcesses' ,['dayid'=>$dayid]);
        }

    }


    public static function getContent($url) {
        $content = self::get_response ( $url );

        // 数据获取重试
        $api_data_i=1;
        while(!$content){
            $content = self::get_response ( $url );
            $api_data_i++;
            if($api_data_i>3)
                break;
        }

        if (! $content) {
            return false;
        }else{
            $data = explode ( "\n", trim ( $content, "\n" ) );
            $data1 = array_map ( 'str_getcsv', $data );
            var_dump(count($data1));
            $filed = array_map ( function ($value) {
                 return  strtolower(preg_replace ( '/\s+/', '_', $value ));

            }, $data1[0]);
            unset ($data[0]);
            $normal_data = [];
            foreach ($data as $key => $value) {
                 $value_info = explode(',',$value);
                $a = array_map ( function ($value1) {
                   return  preg_replace ( '/\"/', '', $value1 ) ;

               }, $value_info);
               
                if(count($filed) == count($a) ){
                    $b = array_combine ( $filed, $a );
                    $normal_data[] = $b;
                }else{
                 self::saveLog(AD_PLATFORM,implode('——',$a));
                }
            }
            return $normal_data;
        }
    }


    // 保存日志
    private static function saveLog($platform_name = '未知', $message = ''){

        $fileName = date('Y-m-d',time());
        $dir = './storage/adDataLogs';

        if (!is_dir($dir)) {
            mkdir($dir,0777,true);
        }
        $logFilename = $dir.'/'.$platform_name.'_ad'.'.log';
        //生成日志
        file_put_contents( $logFilename,$message . "\n\n",FILE_APPEND);
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
