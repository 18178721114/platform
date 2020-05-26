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

class ChannelPushShowHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'ChannelPushShowHandleProcesses {start_date?} {end_date?} ';

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
        $table ='out_ad_zplay_channel';
        $start_date = $this->argument('start_date')?$this->argument('start_date'):date('Y-m-d',strtotime('-8 day'));
        $end_date = $this->argument('end_date')?$this->argument('end_date'):date('Y-m-d',strtotime('-3 day'));
        $begin_date = str_replace('-','',$start_date);
        $end_date1 = str_replace('-','',$end_date);


        DB::beginTransaction();
        $sel_sql = "select count(1) as count  FROM
          $table
        WHERE
         date_time between '$begin_date' and '$end_date1'  ";
        $sel_info = DB::connection('mysql_channel_1')->select($sel_sql);
        $sel_info = Service::data($sel_info);
        //var_dump($sel_info);die;
        if($sel_info[0]['count'] !=0){
            $del_sql ="DELETE
            FROM
                $table
            WHERE date_time between '$begin_date' and '$end_date1'";
            $delete_info =DB::connection('mysql_channel_1')->delete($del_sql);

            if(!$delete_info){
                DB::rollBack();
                }
            }



        $sql ="SELECT a.date_time,a.channel_id_show,a.gameid,sum(ad_new) as ad_new,sum(ad_active) as ad_active,round(sum(ad_income),2) as ad_income,c_channel.td_channel_id as channel_id_s from (SELECT
                d_channel_ad.date_time,
                d_channel_ad.ff_new,
                d_channel_ad.ff_active,
                d_channel_ad.ff_payuser,
                d_channel_ad.ff_earning,
                d_channel_ad.ff_bad_tr,
                d_channel_ad.ff_income,
                d_channel_ad.ff_divide_tr,
                d_channel_ad.ff_divide,
                d_channel_ad.ad_new,
                d_channel_ad.ad_active,
                d_channel_ad.ad_earning,
                d_channel_ad.ad_income_tr,
                d_channel_ad.ad_income,
                d_channel_ad.ad_divide_tr,
                d_channel_ad.ad_divide,
                d_channel_ad.plat_id_cost,
                application.gameid,
                c_channel.td_channel_id as channel_id_show,
                d_channel_ad.channel_id
            FROM
                d_channel_ad,
                application,
                c_channel
            WHERE
                d_channel_ad.app_id = application.new_app_id
                and d_channel_ad.channel_id_show = c_channel.channel_id
                AND date_time BETWEEN '$start_date'  and '$end_date'
                AND ad_active != 0
                AND ad_income != 0) a,c_channel WHERE  a.channel_id = c_channel.channel_id group  by a.date_time,a.channel_id_show,a.gameid";
        $info = DB::select($sql);
        $data = Service::data($info);
        $insert_data = [];
        foreach ($data as $k => $v) {
            $insert_data[$k]['game_id_z'] =$v['gameid'];
            $insert_data[$k]['channel_id_z'] =$v['channel_id_show'];
            $insert_data[$k]['date_time'] =str_replace('-','',$v['date_time']);
            $insert_data[$k]['new'] =$v['ad_new'];
            $insert_data[$k]['active'] =$v['ad_active'];
            $insert_data[$k]['adactive'] =$v['ad_active'] ;
            $insert_data[$k]['adactive_per'] ='100%';
            $insert_data[$k]['ff_income'] = 0;
            $insert_data[$k]['payuser'] =0;
            $insert_data[$k]['dpr'] =0;
            $insert_data[$k]['arppu'] = 0;
            $insert_data[$k]['ad_income'] = $v['ad_income'];
            $insert_data[$k]['ad_arpu'] =round($v['ad_income']/$v['ad_active'],2);
            $insert_data[$k]['gross_income'] = 0;
            $insert_data[$k]['adincome_per'] = 0;
            $insert_data[$k]['retention_rate'] =0;
            $insert_data[$k]['remark'] = 1;
            $insert_data[$k]['create_time'] = date("Y-m-d H:i:s");
        }

        //拆分批次
        $step = array();
        $i = 0;
        foreach ($insert_data as $kkkk => $insert_data_info) {
            if ($kkkk % 300 == 0) $i++;
            if ($insert_data_info) {
                $step[$i][] = $insert_data_info;
            }
        }
        $is_success = [];
        if ($step) {
            foreach ($step as $k => $v) {
                $result = DataImportLogic::insertAdReportInfoDatabase('mysql_channel_1',$table, $v);
                if (!$result) {
                    $is_success[] = $k;
                }
            }
        }
        DB::commit();



    }
}