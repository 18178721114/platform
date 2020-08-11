<?php

namespace App\Console\Commands\DivideHandleProcesses;

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

class ChinaChannelHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'ChinaChannelHandleProcesses {start_date?} {end_date?} ';

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
        try {
            set_time_limit(0);
            $source_id = 'pco01';
            $source_name = '国内安卓渠道广告分成';
            $start_date = $this->argument('start_date') ? $this->argument('start_date') : date('Y-m-d', strtotime('-9 day'));
            $end_date = $this->argument('end_date') ? $this->argument('end_date') : date('Y-m-d', strtotime('-2 day'));

            $sql = "delete from d_channel_ad where date_time>='$start_date' and date_time<='$end_date';";
            $delete_info = DB::delete($sql);
            $error_log_arr = [];

            // 国内安卓渠道广告分成
            // 先根据 c_channel_app_divide 和广告收入小表判断是否  这个应用这个渠道是否参与广告分成
            // 在根据应用所关联用户数  如果有活跃 那么就是广告收入乘渠道分成比例 这个是渠道的收入 否则为零
            $sql = "insert into d_channel_ad (app_id,
        date_time,
        channel_id_show,
        channel_id,
        ad_divide,
        ad_earning,
        ad_new,
        ad_active,
        ad_divide_tr,
        ad_income_tr ,
        ad_income,
        plat_id_cost,
        create_time,
        update_time )
        SELECT
        a.app_id as app_id,
        a.date as date_time,
        a.channel_id_show as channel_id_show,
        a.channel_id as channel_id,
        (CASE when c.active_user =0 THEN 0 ELSE a.earning_fix*d.divide_ad/100 end) as ad_divide,
        a.earning_fix as ad_earning,
        c.new_user as ad_new,
        c.active_user as ad_active,
        d.divide_ad as ad_divide_tr,
        100 as ad_income_tr ,
        a.earning_fix as ad_income,
        'pco01' as plat_id_cost,
        now() as create_time,
        now() as update_time
        FROM
        (
        SELECT
        a.app_id,
        a.date,
        i.channel_id_show ,
        a.channel_id ,
        sum(a.earning_fix) AS earning_fix
        FROM
        zplay_ad_report_daily AS a,
        c_channel_app_divide AS i
        WHERE
        a.app_id = i.app_id
        AND a.channel_id = i.channel_id
        AND a.statistics = 2
        AND a.date >= '$start_date'
        AND a.date <= '$end_date'
        AND a.data_platform_id = 'pad262'
        GROUP BY
        a.app_id,
        a.channel_id,
        a.date
        ) AS a
        LEFT JOIN (
        SELECT
        b.date,
        sum(b.new_user) AS new_user,
        SUM(b.active_user) AS active_user,
        b.app_id,
        c_channel.channel_id,
        c_channel.id
        FROM
        zplay_user_tj_report_daily b
        LEFT JOIN c_channel ON b.channel_id = c_channel.id
        WHERE
        b.date >= '$start_date'
        AND b.date <= '$end_date'
        AND b.ad_status = 1
        GROUP BY
        b.app_id,
        channel_id,
        b.date
        ) c ON a.app_id = c.app_id
        AND a.channel_id = c.channel_id
        AND a.date = c.date
        LEFT JOIN (SELECT
        c_channel.channel_id,
        c_divide.divide_ad
        FROM
        c_divide
        LEFT JOIN c_channel ON c_divide.app_channel_id = c_channel.id
        WHERE
        c_divide.type = 2 )d ON a.channel_id = d.channel_id
        -- WHERE c.app_id is not  null  and d.channel_id is not  null 
        ";
            $insert_info = DB::insert($sql);


            //未统计到用户数的应用
            $sql = "
        SELECT
        a.app_id as app_id,
        a.date as date_time,
        a.channel_id as channel_id,
        (CASE when c.active_user =0 THEN 0 ELSE a.earning_fix*d.divide_ad end) as ad_divide,
        a.earning_fix as ad_earning,
        c.new_user as ad_new,
        c.active_user as ad_active,
        d.divide_ad as ad_divide_tr,
        100 as ad_income_tr ,
        a.earning_fix as ad_income,
        'pco01' as plat_id_cost,
        now() as create_time,
        now() as update_time,
        a.app_name,
        a.channel_name
        FROM
        (
        SELECT
        a.app_id,
        a.date,
        a.channel_id,
        sum(a.earning_fix) AS earning_fix,
        i.app_name,
        i.channel_name
        FROM
        zplay_ad_report_daily AS a,
        c_channel_app_divide AS i
        WHERE
        a.app_id = i.app_id
        AND a.channel_id = i.channel_id
        AND a.statistics = 2
        AND a.date >= '$start_date'
        AND a.date <= '$end_date'
        AND a.data_platform_id = 'pad262'
        GROUP BY
        a.app_id,
        a.channel_id,
        a.date
        ) AS a
        LEFT JOIN (
        SELECT
        b.date,
        sum(b.new_user) AS new_user,
        SUM(b.active_user) AS active_user,
        b.app_id,
        c_channel.channel_id,
        c_channel.id
        FROM
        zplay_user_tj_report_daily b
        LEFT JOIN c_channel ON b.channel_id = c_channel.id
        WHERE
        b.date >= '$start_date'
        AND b.date <= '$end_date'
        AND b.ad_status = 1
        GROUP BY
        b.app_id,
        channel_id,
        b.date
        ) c ON a.app_id = c.app_id
        AND a.channel_id = c.channel_id
        AND a.date = c.date
        LEFT JOIN (SELECT
        c_channel.channel_id,
        c_divide.divide_ad
        FROM
        c_divide
        LEFT JOIN c_channel ON c_divide.app_channel_id = c_channel.id
        WHERE
        c_divide.type = 2 )d ON a.channel_id = d.channel_id
        WHERE c.app_id is null   
        ";
            $error_tj_info = DB::select($sql);
            $error_tj_info = Service::data($error_tj_info);
            if ($error_tj_info) {
                foreach ($error_tj_info as $key => $value) {
                    $error_log_arr['tj'][] = $value['app_id'] . "(" . $value['app_name'] . ")";
                }
            }
            //c_divide 表没有渠道信息
            $sql = "
        SELECT
        a.app_id as app_id,
        a.date as date_time,
        a.channel_id as channel_id,
        (CASE when c.active_user =0 THEN 0 ELSE a.earning_fix*d.divide_ad end) as ad_divide,
        a.earning_fix as ad_earning,
        c.new_user as ad_new,
        c.active_user as ad_active,
        d.divide_ad as ad_divide_tr,
        100 as ad_income_tr ,
        a.earning_fix as ad_income,
        'pco01' as plat_id_cost,
        now() as create_time,
        now() as update_time,
        a.app_name,
        a.channel_name
        FROM
        (
        SELECT
        a.app_id,
        a.date,
        a.channel_id,
        sum(a.earning_fix) AS earning_fix,
        i.app_name,
        i.channel_name
        FROM
        zplay_ad_report_daily AS a,
        c_channel_app_divide AS i
        WHERE
        a.app_id = i.app_id
        AND a.channel_id = i.channel_id
        AND a.statistics = 2
        AND a.date >= '$start_date'
        AND a.date <= '$end_date'
        AND a.data_platform_id = 'pad262'
        GROUP BY
        a.app_id,
        a.channel_id,
        a.date
        ) AS a
        LEFT JOIN (
        SELECT
        b.date,
        sum(b.new_user) AS new_user,
        SUM(b.active_user) AS active_user,
        b.app_id,
        c_channel.channel_id,
        c_channel.id
        FROM
        zplay_user_tj_report_daily b
        LEFT JOIN c_channel ON b.channel_id = c_channel.id
        WHERE
        b.date >= '$start_date'
        AND b.date <= '$end_date'
        AND b.ad_status = 1
        GROUP BY
        b.app_id,
        channel_id,
        b.date
        ) c ON a.app_id = c.app_id
        AND a.channel_id = c.channel_id
        AND a.date = c.date
        LEFT JOIN (SELECT
        c_channel.channel_id,
        c_divide.divide_ad
        FROM
        c_divide
        LEFT JOIN c_channel ON c_divide.app_channel_id = c_channel.id
        WHERE
        c_divide.type = 2 )d ON a.channel_id = d.channel_id
        WHERE d.channel_id is null   
        ";
            $error_tj_info = DB::select($sql);
            $error_tj_info = Service::data($error_tj_info);
            if ($error_tj_info) {
                foreach ($error_tj_info as $key => $value) {
                    $error_log_arr['channel'][] = $value['channel_id'] . "(" . $value['channel_name'] . ")";
                }
            }

            // 保存错误信息
            if ($error_log_arr) {
                $error_msg_array = [];
                $error_msg_mail = [];
                if (isset($error_log_arr['tj'])) {
                    $tj = implode(',', array_unique($error_log_arr['tj']));
                    $error_msg_array[] = '未统计到用户数的应用id,ID为:' . $tj;
                    $error_msg_mail[] = '未统计到用户数的应用id，ID为：' . $tj;
                }

                if (isset($error_log_arr['channel'])) {
                    $channel = implode(',', array_unique($error_log_arr['channel']));
                    $error_msg_array[] = '未匹配到渠道分成比例的渠道id,code为:' . $channel;
                    $error_msg_mail[] = '未匹配到渠道分成比例的渠道id，code为：' . $channel;
                }

                DataImportImp::saveDataErrorLog(2, $source_id, $source_name, 5, implode(';', $error_msg_array));
                // 发送邮件
                CommonFunction::sendMail($error_msg_mail, '国内安卓渠道分成数据处理error');


            }
            echo '处理完成';
        }catch (\Exception $e) {
            // 异常报错
            $message = date("Y-m-d")."号,渠道广告分成数据程序报错,报错原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, 'pad-001', '渠道广告分成国家数据', 2, $message);
            $error_msg_arr[] = $message;
            CommonFunction::sendMail($error_msg_arr, '渠道广告分成数据');
            exit;
        }
    }
}