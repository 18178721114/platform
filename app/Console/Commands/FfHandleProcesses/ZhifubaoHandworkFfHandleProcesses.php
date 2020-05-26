<?php

namespace App\Console\Commands\FfHandleProcesses;

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

class ZhifubaoHandworkFfHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'ZhifubaoHandworkFfHandleProcesses {dayid?} {channel_id?} ';

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
        //支付宝手工导数处理过程   先把手工导数 的数据 吧商品订单id 拿出来 做成字符串 然后  查出在mysql（zplay_ff_data表中）抓取数据的 数据里查出 不在这些字符串里的数据 的 id 把这些id tongji_type更新成-1
        set_time_limit(0);
        $source_id = 'pff05';
        $billing_name ='支付宝';
        //最终表
        $mysql_table = 'zplay_ff_report_daily';
        $dayid = $this->argument('dayid')?$this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));
        $channel_id = $this->argument('channel_id');
        $date_arr =[];
        if(gettype($dayid) == 'array'){
            $date = implode("','",$dayid);
            $date_arr = $dayid;
        }else{
            $date = $dayid;
            $date_arr[] = $dayid;
        }
        try { 
              //查询pgsql 的数据
            $map =[];
            $map['in'] = ['dayid',$date_arr];
            $map['type']  =1;
            $map['source_id']  = $channel_id;
            $map['notlike'][] = ["json_data->业务类型",'not like','提现'];
            $handinfo = DataImportLogic::getChannelData('ff_data','erm_data',$map)->get();
            $handinfo = Service::data($handinfo);
            if(!$handinfo){
//                $error_msg = $dayid.'号，支付宝计费平台数据处理程序获取手工导数原始数据为空';
//                echo $error_msg;
//                DataImportImp::saveDataErrorLog(2,$source_id,$billing_name,3,$error_msg);
                exit;
            }
            $sql_count = "select count(1) as  count from $mysql_table where platform_id ='$source_id' and date in  ('$date')";
            $count = DB::select($sql_count);
            $count = Service::data($count)[0]['count'];
            if(count($handinfo)<$count){
                 $error_msg = $date.'号，支付宝计费平台数据处理程序获取手工导数原始数据小于接口数据'.(count($handinfo)-(int)$count);
                DataImportImp::saveDataErrorLog(2,$source_id,$billing_name,3,$error_msg);
            }
            if(count($handinfo)>$count){
               $error_msg = $date.'号，支付宝计费平台数据处理程序获取手工导数原始数据大于接口数据'.((int)$count-count($handinfo));
               DataImportImp::saveDataErrorLog(2,$source_id,$billing_name,3,$error_msg);
           }

            $str = '';
            foreach ($handinfo as $key => $value) {

                $json_info = json_decode($value['json_data'],true);
                $str .="'".$json_info['商户订单号']."',";
            }
            $str = trim($str,',');
            $sql = "select id from $mysql_table where platform_id ='$source_id' and date  in  ('$date') and concat(date_format(date, '%Y%m'),remark) not in ($str) ";
            $info = DB::select($sql);
            $info = Service::data($info);

        } catch (\Exception $e) {
            $error_msg_info = $date.'号,'.$billing_name.'渠道数据匹配失败：'.$e->getMessage();
            ApiResponseFactory::apiResponse([],[],'',$error_msg_info); 
        } 
        if(!empty($info)){
            $str_id='';
            foreach ($info as $k => $v) {
                 $str_id .="'".$v['id']."',";
            }
            $str_id = trim($str_id,',');   

            $update_sql = "update $mysql_table set tongji_type = -1,update_time =now()  where id in ($str_id)";
            DB::update($update_sql);

            if (is_array($date_arr)){
                foreach ($date_arr as $date_time){
                     Artisan::call('FfSummaryProcesses',['begin_date'=>$date_time,'end_date'=>$date_time,'platform_id'=>$source_id]);
                    //DB::update("call ff_summary('$date_time','$date_time','$source_id')");
                }
            }else{
                Artisan::call('FfSummaryProcesses',['begin_date'=>$dayid,'end_date'=>$dayid,'platform_id'=>$source_id]);
                //DB::update("call ff_summary('$dayid','$dayid','$source_id')");
            }
            //echo '处理完成';
        }else{
             //echo '暂无处理数据';
        }

        
    }
}