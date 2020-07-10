<?php

namespace App\Console\Commands\ShellScript;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\Common\CurlRequest;
use App\Common\Service;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\DataImportLogic;
use App\Common\CommonFunction;
use Illuminate\Support\Facades\Redis;
use App\BusinessImp\PlatformImp;

class AdConfigMaintainProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'AdConfigMaintainProcesses {begin_date?} {end_date?} {platform_id?} ';

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
        $begin_date = $this->argument('begin_date') ? $this->argument('begin_date'):date('Y-m-d',strtotime('-3 months'));
        $end_date = $this->argument('end_date') ? $this->argument('end_date'):date('Y-m-d',strtotime('-1 month'));
        $platform_id = $this->argument('platform_id') ? $this->argument('platform_id'):'';
        // todo 暂时只支持Facebook
//        $where_platform = '';
//        if ($platform_id) {
//            $where_platform = "  and platform_id = '$platform_id'";
//        }else{
//            $where_platform = "  and platform_id = 'ptg33'";
//        }

        try {

            $ad_sql = " select tmp.conf_id from (
                select conf.conf_app_id, conf.conf_id, cost.cost_app_id, cost.cost from 
                (
                    select distinct a.app_id as conf_app_id,g.app_id as conf_id from `c_app_ad_platform` g left join c_app a on a.id = g.app_id where g.`platform_id` = 'pad23' and g.`status` = 1 and g.`redundancy_status` = 1 and a.create_time < '{$end_date}'
                ) conf
                left join 
                (
                    select app_id as cost_app_id,sum(earning_fix) as cost from `zplay_ad_report_daily` where date between '{$begin_date}' and '{$end_date}' and `platform_id` = 'pad23' group by app_id
                ) cost 
                on cost.cost_app_id = conf.conf_app_id where cost.cost_app_id is null and cost.cost is null
                ) tmp ";
            $ad_sel_info = DB::select($ad_sql);
            $ad_sel_info = Service::data($ad_sel_info);
            var_dump(count($ad_sel_info));
            if ($ad_sel_info){
                DB::beginTransaction();
                $ad_update_sql = " update c_app_ad_platform set redundancy_status = 2 where app_id in 
                    (
                    select tmp.conf_id from (
                    select conf.conf_app_id, conf.conf_id, cost.cost_app_id, cost.cost from 
                    (
                        select distinct a.app_id as conf_app_id,g.app_id as conf_id from `c_app_ad_platform` g left join c_app a on a.id = g.app_id where g.`platform_id` = 'pad23' and g.`status` = 1 and g.`redundancy_status` = 1 and a.create_time < '{$end_date}'
                    ) conf
                    left join 
                    (
                        select app_id as cost_app_id,sum(earning_fix) as cost from `zplay_ad_report_daily` where date between '{$begin_date}' and '{$end_date}' and `platform_id` = 'pad23' group by app_id
                    ) cost 
                    on cost.cost_app_id = conf.conf_app_id where cost.cost_app_id is null and cost.cost is null
                    ) tmp
                    ) and `platform_id` = 'pad23' and `status` = 1 and `redundancy_status` = 1
                    ";
                $update_result = DB::update($ad_update_sql);
                if (!$update_result) {
                    DB::rollBack();
                }
                DB::commit();
            }


        }catch (\Exception $e) {
            // 异常报错
            $message = date("Y-m-d")."号,广告平台配置维护程序报错,报错原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, 'pad-001', '广告平台', 2, $message);
            $error_msg_arr[] = $message;
            CommonFunction::sendMail($error_msg_arr, '广告平台程序error');
            exit;
        }
    }
}