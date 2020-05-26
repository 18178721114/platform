<?php

namespace App\Console\Commands\TjHandleProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessImp\PlatformImp;
use App\BusinessLogic\AdReportLogic;
use App\BusinessLogic\DataImportLogic;
use App\BusinessLogic\PlatformLogic;
use App\Common\CommonFunction;
use App\Common\CurlRequest;
use App\Common\ParseDayid;
use App\Common\Service;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\ApplicationLogic;
use App\BusinessLogic\CommonLogic;
use App\BusinessLogic\ChannelLogic;
use App\Common\ApiResponseFactory;

class HuaweiHandworkTjHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'HuaweiHandworkTjHandleProcesses {dayid?} ';

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
    	$channel_id= 54;
      	//查询渠道关联广告平台
        $map = []; // 查询条件
        $map['c_channel.id'] = $channel_id ;
        $fields = ['c_channel.*','c_channel_payment_mapping.pay_platform_id'];

        $map['leftjoin'] = [
            ['c_channel_payment_mapping','c_channel.id', 'c_channel_payment_mapping.channel_id'],
        ];
        // 获取分页数据
        $channel_list = ChannelLogic::getChannelList($map, $fields)->first();
        $channel_list = Service::data($channel_list);

        $source_id = $channel_list['channel_id'];
        $billing_name =$channel_list['channel_name'];

        //最终表
        $mysql_table = 'zplay_basic_report_daily_channel';

        $dayid = date('Y-m-d');
        $date_arr =[];
        $date_arr = $this->argument('dayid');
        //查询pgsql 的数据
        $map =[];
        $map['in'] = ['dayid',$date_arr];
        $map['type']  =1;
        $map['source_id']  = $channel_id;
        $info = DataImportLogic::getChannelData('tj_data','erm_data',$map)->get();
        $info = Service::data($info);
        var_dump(count($info));
        
        if(!$info){
//            $error_msg = $dayid.'号，'.$billing_name.'渠道统计数据处理程序获取原始数据为空';
//            DataImportImp::saveDataErrorLog(2,$source_id,$billing_name,3,$error_msg);
            exit;
        }


        //获取匹配应用的数据
         $sql ="SELECT DISTINCT 
         id,        
        `app_id`,
        `company_id` as game_creator,
        os_id AS os_id,
        app_category_id AS game_category_id,
        release_group AS game_group
        FROM
        `c_app`
        ";
        $app_list = DB::select($sql);
        $app_list = Service::data($app_list);
        if(!$app_list){
        	$error_msg = $billing_name.'渠道统计数据处理程序应用数据查询为空';
            DataImportImp::saveDataErrorLog(2,$source_id,$billing_name,3,$error_msg);
            ApiResponseFactory::apiResponse([],[],'',$error_msg);
            exit;
        }
        $array =[];
        $error_log_arr=[];
        $error_detail_arr = [];
        $num = 0;
        try {
            DB::beginTransaction();
            foreach ($info as $k => $v) {

            	$json_info = json_decode($v['json_data'],true);
            	foreach ($app_list as $app_k => $app_v) {
                        if($json_info['应用ID'] ==$app_v['app_id']  ){
                            $array[$k]['app_id'] = $app_v['id'];
                            $array[$k]['os_id'] = $app_v['os_id'];
                            $array[$k]['game_creator'] = $app_v['game_creator'];
                            $array[$k]['game_category_id'] = $app_v['game_category_id'];
                            $array[$k]['game_group'] = $app_v['game_group'];

                            $num = 0;
                            break;
                        }else{
                            $num++;

                        }
            	}
                if($num){
                    $error_log_arr['app_id'][]=$json_info['应用ID'];
                }
                if(($num)>0){

                    $error_detail_arr[$k]['platform_id'] = 'ptj268';
                    $error_detail_arr[$k]['platform_name'] = '华为统计数据';
                    $error_detail_arr[$k]['platform_type'] =1;
                    $error_detail_arr[$k]['err_date'] = $json_info['date_time'];
                    $error_detail_arr[$k]['first_level_id'] = $json_info['应用ID'];
                    $error_detail_arr[$k]['first_level_name'] = '';
                    $error_detail_arr[$k]['second_level_id'] = '';
                    $error_detail_arr[$k]['second_level_name'] = '';
                    $error_detail_arr[$k]['money'] = $json_info['新增用户']; // 流水原币
                    $error_detail_arr[$k]['account'] = isset($v['account']) ? $v['account'] : 'zplay';
                    $error_detail_arr[$k]['create_time'] = date('Y-m-d H:i:s');

                    unset($array[$k]);
                    //插入错误数据
                    continue;
                }
                $dayid = date('Y-m-d');
                $one_day = date('Y-m-d',strtotime($json_info['date_time'])-86400);
                $three_day = date('Y-m-d',strtotime($json_info['date_time'])-86400*3);
                $seven_day = date('Y-m-d',strtotime($json_info['date_time'])-86400*7);
                $day_array[0] =$one_day;
                $day_array[1] =$three_day;
                $day_array[2] =$seven_day;
                $map =[];
                $map1['in'] = ['dayid',$day_array];
                $map1['type']  =1;
                $map1['source_id']  = $channel_id;
                $map1["app_id"]  = $json_info['应用ID'];
                $info1 = DataImportLogic::getChannelData('tj_data','erm_data',$map1)->get();
                $info1 = Service::data($info1);
                // echo '<pre>';
                //  var_dump($array[$k]['app_id']);
                // var_dump($info1);
                if(!empty($info1)){
                    foreach ($info1 as $key => $value) {
                        $json_info1 = json_decode($value['json_data'],true);
                        if($value['dayid'] ==$one_day ){
                            $array[$k]['retn_new_one'] = $json_info1['新增用户']*$json_info['次日留存率'];

                        }elseif ($value['dayid'] ==$three_day) {
                             $array[$k]['retn_new_three'] = $json_info1['新增用户']*$json_info['三日留存率'];

                        }elseif($value['dayid'] ==$seven_day) {
                            $array[$k]['retn_new_seven'] = $json_info1['新增用户']*$json_info['七日留存率'];

                        }

                    }

                }
                $array[$k]['country_id'] = 64;
                $array[$k]['platform_id'] ='ptj268';
                $array[$k]['date_time'] =$json_info['date_time'];
                $array[$k]['channel_id'] = $channel_id;
                $array[$k]['plat_type'] = 'tj';
                $array[$k]['new_ff'] = $json_info['新增用户'];
                $array[$k]['active_ff'] = $json_info['活跃用户'];
                // $array[$k]['retn_new_one'] = $json_info['次日留存率'];
                // $array[$k]['retn_new_three'] = $json_info['三次日留存率'];
                // $array[$k]['retn_new_seven'] = $json_info['七次日留存率'];
                $array[$k]['flow_type'] = 1;
                $array[$k]['statistics'] = 0;
                $array[$k]['create_time'] = date('Y-m-d H:i:s');
                //删除数据
                $map_delete['channel_id'] =$channel_id;
                $map_delete['app_id'] = $array[$k]['app_id'];
                $map_delete['date_time'] = $json_info['date_time'];
                DataImportLogic::deleteMysqlHistoryData($mysql_table,$map_delete);

                $result = DataImportLogic::insertAdReportInfo($mysql_table, $array[$k]);
                if (!$result){
                    DB::rollBack();
                }
                $array =[];

            }
            DB::commit();
            // 调用存储过程更新总表数据
//            DB::update("call tj_summary('ptj268')");
            Artisan::call('TjSummaryProcesses',['platform_id'=>$source_id]);

        } catch (\Exception $e) {
            $error_msg_info = $dayid.'号,'.$billing_name.'渠道数据匹配失败：'.$e->getMessage();
            ApiResponseFactory::apiResponse([],[],'',$error_msg_info); 
        }

                // 保存错误信息
        if ($error_log_arr){
            //var_dump($error_log_arr);die;
            $error_msg_array = [];
            if (isset($error_log_arr['app_id'])){
                $app_id = implode(',',array_unique($error_log_arr['app_id']));
                $error_msg_array[] = '应用id匹配失败,ID为:'.$app_id;
            }
            DataImportImp::saveDataErrorLog(2,$source_id,$billing_name,3,implode(';',$error_msg_array));

            foreach ($date_arr as $key => $value) {
                $array_err =[];
                foreach ($error_detail_arr as $k => $v) {
                    if($v['err_date'] ==$value){
                        $array_err[$k] = $v;
                        DataImportImp::saveDataErrorMoneyLog($source_id,$value,$array_err);

                    }

                }
            }
//            CommonFunction::sendMail($error_msg_array,$billing_name.'渠道计费数据处理error');
        }

    }
}