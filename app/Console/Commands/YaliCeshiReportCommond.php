<?php

namespace App\Console\Commands;

use App\BusinessLogic\AdReportLogic;
use App\Common\CurlRequest;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\DataImportLogic;

class YaliCeshiReportCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'YaliCeshiReportCommond';

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
        $index =10000;
        $insert_data =[];
        $step =[];
        for ($i=0; $i <$index ; $i++) { 
            $insert_data[$i]['account'] = '原生-01掌游B19KA02283';
            $insert_data[$i]['type'] = 2;
            $insert_data[$i]['source_id'] = 20;
            $insert_data[$i]['dayid'] = "2019-06-13";
            $insert_data[$i]['json_data'] ='{"impression": "139", "aggreffectiveconsults": "0", "highqualityconsults": "0", "campaign_id": "106141094", "cost": "0.00", "campaign_name": "Star-QXD-A3-1-1-1-720X1280-17S_0603", "click": "0", "adgroup_id": "4067034094", "aggrordersubmitsuccess": "0", "aggrphoneclicks": "0", "adgroup_name": "Star-QXD-A3-1-1-1-720X1280-17S_0603", "account_id": "27511371", "activeconversions": "0", "aggrconcultclicks": "0", "account": "\u539f\u751f-01\u638c\u6e38B19KA02283", "aggrformclicksuccess": "0", "cpm": "0.00", "ctr": "0.00%", "cpc": "0.00", "dayid": "2019-06-13", "aggrreverseclicks": "0", "aggrformsubmitsuccess": "0"}';
            $insert_data[$i]['create_time'] = date("Y-m-d H:i:s");
            $insert_data[$i]['year'] = date("Y",strtotime("2019-06-13"));
            $insert_data[$i]['month'] = date("m",strtotime("2019-06-13"));
        }
        $i = 0;
        foreach ($insert_data as $kkkk => $insert_data_info) {
            if ($kkkk % 2000 == 0) $i++;
            if ($insert_data_info) {
                $step[$i][] = $insert_data_info;
            }
        }

        if ($step) {
            $start_time = time();
            foreach ($step as $k => $v) {
                $result = DataImportLogic::insertChannelData('ad_data','erm_data',$v); 
                if (!$result) {
                   echo 'mysql_error'. PHP_EOL;
               }
           }
           $end_time = time();
           $diff_time = $end_time-$start_time;
           file_put_contents('./yaliceshi.log',date('Y-m-d H:i:s').'开始时间:'.date('Y-m-d H:i:s',$start_time).'------结束时间'.date('Y-m-d H:i:s',$end_time).'---时间差'.$diff_time."\n", FILE_APPEND);

       }

    }
}
