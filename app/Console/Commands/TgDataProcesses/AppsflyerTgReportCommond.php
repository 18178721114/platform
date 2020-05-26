<?php

namespace App\Console\Commands\TgDataProcesses;

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

class AppsflyerTgReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'AppsflyerTgReportCommond {dayid?} {appid?}';

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

    	/*
        define('AD_PLATFORM', 'Appsflyer');
        define('SCHEMA', 'tg_data');
        define('TABLE_NAME', 'erm_data');
        define('SOURCE_ID', 'ptg02'); // todo 这个需要根据平台信息表确定平台ID

        //这里面要写新测试平台里的数据配置 从数据库里取数据
        $sql = " select distinct a.platform_id,a.data_account as company_account,application_id,token from c_platform_account_mapping a left join c_generalize b on b.platform_id = a.platform_id and a.account = b.data_account where a.platform_id = 'ptg02' ";
        $app_list = DB::select($sql);
        $app_list = Service::data($app_list);
        if (!$app_list) return;

//    	$api_token ='6efa7c19-07c0-4457-bfd4-67e5c19f0997';
//    	$gameDataSql = "select * from c_generalize where platform_id ='ptg02'";
//    	$app_list = DB::select($gameDataSql);
//        $app_list = Service::data($app_list);
    	foreach ($app_list as $key => $row) {
            $row['appid'] =$row['application_id'];
            $api_token = $row['token'];
        	//获取应用信息
    		$url = "https://hq.appsflyer.com/export/{$row['appid']}/geo_by_date_report/v5?api_token={$api_token}&from={$dayid}&to={$dayid}";
    		$info  =self::get_response ( $url );

    		$data_array = self::parse_csv($info);

    		if(!empty($data_array)){
        	    $app_list = DB::delete("delete from appsflyer where date='{$dayid}' and appid='{$row['appid']}'");
                foreach ($data_array as $data){
    //                if(!isset($data['impressions'])) continue;
                    //插入
                    if( !isset($data['impressions']) ||  $data['impressions']=='N/A'  )
                        $impressions = 0;
                    else
                        $impressions = $data['impressions'];
                    if( !isset($data['clicks']) || $data['clicks']=='N/A'  )
                        $clicks = 0;
                    else
                        $clicks = $data['clicks'];
                    if( !isset($data['ctr']) || $data['ctr']=='N/A'  )
                        $ctr = 0;
                    else
                        $ctr = $data['ctr'];
                    if( !isset($data['installs']) ||  $data['installs']=='N/A' )
                        $installs = 0;
                    else
                        $installs = $data['installs'];
                    if( !isset($data['conversion_rate']) ||  $data['conversion_rate']=='N/A'  )
                        $conversion_rate = 0;
                    else
                        $conversion_rate = $data['conversion_rate'];
                    if( !isset($data['sessions']) ||  $data['sessions']=='N/A' )
                        $sessions = 0;
                    else
                        $sessions = $data['sessions'];
                    if( !isset($data['total_cost']) ||  $data['total_cost']=='N/A'  )
                        $total_cost = 0;
                    else
                        $total_cost = $data['total_cost'];
                    if( !isset($data['roi']) || $data['roi']=='N/A'  )
                        $roi = 0;
                    else
                        $roi = $data['roi'];

                    if( !isset($data['average_ecpi']) || $data['average_ecpi']=='N/A'  )
                        $average_ecpi = 0;
                    else
                        $average_ecpi = $data['average_ecpi'];
                    if(  !isset($data['loyal_users/installs']) || $data['loyal_users/installs']=='N/A'  )
                        $loyal_users_installs = 0;
                    else  $loyal_users_installs = $data['loyal_users/installs'];
                    if( !isset($data['arpu']) || $data['arpu']=='N/A' )
                        $data['arpu'] = 0;
                    if( !isset($data['country']) || $data['country']=='N/A' )
                        $data['country'] = 16;
                    if( !isset($data['agency/pmd_(af_prt)']) || $data['agency/pmd_(af_prt)']=='N/A' )
                        $data['agency/pmd_(af_prt)'] = 0;
                    if( !isset($data['media_source_(pid)']) || $data['media_source_(pid)']=='N/A' )
                        $data['media_source_(pid)'] = 0;
                    if( !isset($data['campaign_(c)']) || $data['campaign_(c)']=='N/A' )
                        $data['campaign_(c)'] = 0;
                    if( !isset($data['loyal_users']) || $data['loyal_users']=='N/A' )
                        $data['loyal_users'] = 0;
                    if( !isset($data['total_revenue']) || $data['total_revenue']=='N/A' )
                        $data['total_revenue'] = 0;

                    $sql = "insert into appsflyer (date,country,`agency/pmd_(af_prt)`,`media_source_(pid)`,`campaign_(c)`,impressions,clicks,ctr,installs,conversion_rate,sessions,loyal_users,`loyal_users/installs`,total_revenue,total_cost,roi,arpu,average_ecpi,appid,in_time) values(
                    '{$dayid}','{$data['country']}','{$data['agency/pmd_(af_prt)']}','{$data['media_source_(pid)']}','{$data['campaign_(c)']}',$impressions,$clicks,'{$ctr}',$installs,'{$conversion_rate}',$sessions,{$data['loyal_users']},'{$loyal_users_installs}','{$data['total_revenue']}','{$total_cost}','{$roi}','{$data['arpu']}','{$average_ecpi}','{$row['appid']}',now())";
                    $app_list = DB::insert($sql);


                }
            }else{
                    $error_msg = AD_PLATFORM.'推广平台'.$row['appid'].'账号取数失败,错误信息:'.$info;
                    DataImportImp::saveDataErrorLog(1,SOURCE_ID,AD_PLATFORM,4,$error_msg);
                    $error_msg_arr[] = $error_msg;
                    CommonFunction::sendMail($error_msg_arr,AD_PLATFORM.'推广平台取数error');

            }
        }

        Artisan::call('AppsflyerTgHandleProcesses',['dayid'=>$dayid]);
    	*/
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
