<?php

namespace App\Console\Commands\CrontabMysqlHome;

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
use Illuminate\Support\Facades\Redis;

class HomeUsdCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'HomeUsdCommond {dayid?} ';

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

//        $tg_info = env('REDIS_TG_KEYS');
//        //获取广告数据长度
//        $tg_len = Redis::llen($tg_info);
//        if($tg_len>0){
//            die;
//        }
        try {
            // 入口方法
            $dayid = $this->argument('dayid') ? $this->argument('dayid') : date('Y-m-d', strtotime('-2 day'));
            var_dump($dayid);

            $currency_type = 60;
            echo $currency_type . ' 开始时间：' . date('Y-m-d H:i:s') . "\r\n";
            $this->insertBasicDataHomePage($dayid, $currency_type);
        }catch (\Exception $e) {
            // 异常报错
            $message = date("Y-m-d")."号,首页美元数据程序报错,报错原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, 'pad-001', '首页美元数据', 2, $message);
            $error_msg_arr[] = $message;
            CommonFunction::sendMail($error_msg_arr, '首页美元数据');
            exit;
        }

    }

    public function insertBasicDataHomePage($dayid,$currency_type){
        $month_begin = date('Y-m-01',strtotime($dayid));
        $month_end = date('Y-m-d', strtotime("$month_begin 0 month -1 day"));
        $month_end1 = date('Y-m-d', strtotime("$month_begin -1 month -1 day"));
        $mysql_table ='s_basic_data_homepage';
        if ($currency_type == 60){
            $mysql_table ='s_basic_data_homepage_usd';
        }

        $dim_id = $this->getDimId('s_cfg_select_dim','homepage_on',1,$currency_type);
        //var_dump($dim_id);die;
        $sql_str = '';
        foreach($dim_id as $key => $value) {
//            if($value['dim_table_id'] != 'ltv'){
//                $sql_str .= $value['dim_value'].' as '.$value['dim_table_id'].' , ';
//            }else{
//                $sql_str .= ' 0 as ltv , ';
//            }
            if($value['dim_table_id'] == 'ltv')  continue;
            $sql_str .= $value['dim_value'].' as '.$value['dim_table_id'].' , ';

        }
        $sql_str= rtrim($sql_str,', ');

        $sql = "
        SELECT
        '{$dayid}' as date_time,
        os_id,
        app_id,
        game_creator,
        0 as date_type,
        {$sql_str}
        FROM
        zplay_basic_report_daily
        WHERE
        date_time ='{$dayid}' and flow_type = 1 and statistics = 0 group by os_id,app_id,game_creator
    
        UNION ALL
        SELECT
        '{$dayid}' as  date_time,
        os_id,
        app_id,
        game_creator,
        1 as date_type,
        {$sql_str}
        FROM
        zplay_basic_report_daily
        WHERE
        date_time = DATE_SUB('{$dayid}',INTERVAL 1 DAY) and flow_type = 1 and statistics = 0 group by os_id,app_id,game_creator
        UNION ALL
        SELECT
        '{$dayid}' as  date_time,
        os_id,
        app_id,
        game_creator,
        2 as date_type,
        {$sql_str}
        FROM
        zplay_basic_report_daily
        WHERE
        date_time = DATE_SUB('{$dayid}',INTERVAL 2 DAY) and flow_type = 1 and statistics = 0 group by os_id,app_id,game_creator
        UNION ALL
        SELECT
        '{$dayid}' as  date_time,
        os_id,
        app_id,
        game_creator,
        3 as date_type,
        {$sql_str}
        FROM
        zplay_basic_report_daily
        WHERE
        date_time = DATE_SUB('{$dayid}',INTERVAL 3 DAY) and flow_type = 1 and statistics = 0 group by os_id,app_id,game_creator


        UNION ALL
        SELECT
        '{$dayid}' as  date_time,
        os_id,
        app_id,
        game_creator,
        7 as date_type,
        {$sql_str}
        FROM
        zplay_basic_report_daily
        WHERE
        date_time > DATE_SUB('{$dayid}',INTERVAL 7 DAY) AND date_time <= '{$dayid}'  and flow_type = 1 and statistics = 0 group by os_id,app_id,game_creator
        UNION ALL
        SELECT
        '{$dayid}' as  date_time,
        os_id,
        app_id,
        game_creator,
        14 as date_type,
        {$sql_str}
        FROM
        zplay_basic_report_daily
        WHERE
        date_time > DATE_SUB('{$dayid}',INTERVAL 14 DAY) AND date_time <= DATE_SUB('{$dayid}',INTERVAL 7 DAY) and flow_type = 1 and statistics = 0 group by os_id,app_id,game_creator

        UNION ALL
        SELECT
        '{$dayid}' as  date_time,
        os_id,
        app_id,
        game_creator,
        30 as date_type,
        {$sql_str}
        FROM
        zplay_basic_report_daily
        WHERE
        date_time >= '{$month_begin}'  AND date_time <= '{$dayid}' and flow_type = 1 and statistics = 0 group by os_id,app_id,game_creator
        UNION ALL
        SELECT
        '{$dayid}' as  date_time,
        os_id,
        app_id,
        game_creator,
        60 as date_type,
        {$sql_str}
        FROM
        zplay_basic_report_daily
        WHERE
        date_time >= DATE_SUB('{$month_begin}',INTERVAL 1 month) AND date_time <= '{$month_end}' and flow_type = 1 and statistics = 0 group by os_id,app_id,game_creator
        UNION ALL
        SELECT
        '{$dayid}' as  date_time,
        os_id,
        app_id,
        game_creator,
        90 as date_type,
        {$sql_str}
        FROM
        zplay_basic_report_daily
        WHERE
        date_time >= DATE_SUB('{$month_begin}',INTERVAL  2 month) AND date_time <= '$month_end1' and flow_type = 1 and statistics = 0 group by os_id,app_id,game_creator

        
        ";
        /* -- UNION ALL
         --SELECT
         --'{$dayid}' as  date_time,
         --os_id,
         --app_id,
         --game_creator,
         --'-5' as date_type,
          {$sql_str}
         --FROM
         --zplay_basic_report_daily
         -- where date_time <= '$dayid' and flow_type = 1 and statistics = 0
         group by os_id,app_id,game_creator*/
        $info = DB::select($sql);
        $info = Service::data($info);
        // 数据补丁  91  开发者分成成本。    93 总成本    92总利润
        $developer_sql = "
        SELECT
        '{$dayid}' as date_time,
        os_id,
         app_id,
        game_creator,
        0 as date_type,
        round(sum(develop_cost_taxAfter),2) as develop_cost ,
        round(sum(tg_cost) + (case when sum(develop_cost_taxAfter)<0  then 0 else sum(develop_cost_taxAfter) end),2)  as total_cost ,
        round(sum(ff_income_taxAfter) + sum(ad_income_taxAfter) - (sum(tg_cost) + (case when sum(develop_cost_taxAfter)<0  then 0 else sum(develop_cost_taxAfter) end)),2)as total_profit
        FROM
        zplay_divide_develop
        WHERE
        date ='{$dayid}' group by os_id,app_id,game_creator
    
        UNION ALL
        SELECT
        '{$dayid}' as  date_time,
        os_id,
         app_id,
        game_creator,
        1 as date_type,
        round(sum(develop_cost_taxAfter),2) as develop_cost ,
        round(sum(tg_cost) + (case when sum(develop_cost_taxAfter)<0  then 0 else sum(develop_cost_taxAfter) end),2)  as total_cost ,
        round(sum(ff_income_taxAfter) + sum(ad_income_taxAfter) - (sum(tg_cost) + (case when sum(develop_cost_taxAfter)<0  then 0 else sum(develop_cost_taxAfter) end)),2)as total_profit
        FROM
        zplay_divide_develop
        WHERE
        date = DATE_SUB('{$dayid}',INTERVAL 1 DAY)  group by os_id,app_id,game_creator
        UNION ALL
        SELECT
        '{$dayid}' as  date_time,
        os_id,
         app_id,
        game_creator,
        2 as date_type,
        round(sum(develop_cost_taxAfter),2) as develop_cost ,
        round(sum(tg_cost) + (case when sum(develop_cost_taxAfter)<0  then 0 else sum(develop_cost_taxAfter) end),2)  as total_cost ,
        round(sum(ff_income_taxAfter) + sum(ad_income_taxAfter) - (sum(tg_cost) + (case when sum(develop_cost_taxAfter)<0  then 0 else sum(develop_cost_taxAfter) end)),2)as total_profit
        FROM
        zplay_divide_develop
        WHERE
        date = DATE_SUB('{$dayid}',INTERVAL 2 DAY)  group by os_id,app_id,game_creator
        UNION ALL
        SELECT
        '{$dayid}' as  date_time,
        os_id,
         app_id,
        game_creator,
        3 as date_type,
        round(sum(develop_cost_taxAfter),2) as develop_cost ,
        round(sum(tg_cost) + (case when sum(develop_cost_taxAfter)<0  then 0 else sum(develop_cost_taxAfter) end),2)  as total_cost ,
        round(sum(ff_income_taxAfter) + sum(ad_income_taxAfter) - (sum(tg_cost) + (case when sum(develop_cost_taxAfter)<0  then 0 else sum(develop_cost_taxAfter) end)),2)as total_profit
        FROM
        zplay_divide_develop
        WHERE
        date = DATE_SUB('{$dayid}',INTERVAL 3 DAY) group by os_id,app_id,game_creator


        UNION ALL
        SELECT
        '{$dayid}' as  date_time,
        os_id,
         app_id,
        game_creator,
        7 as date_type,
        round(sum(develop_cost_taxAfter),2) as develop_cost ,
        round(sum(tg_cost) + (case when sum(develop_cost_taxAfter)<0  then 0 else sum(develop_cost_taxAfter) end),2)  as total_cost ,
        round(sum(ff_income_taxAfter) + sum(ad_income_taxAfter) - (sum(tg_cost) + (case when sum(develop_cost_taxAfter)<0  then 0 else sum(develop_cost_taxAfter) end)),2)as total_profit
        FROM
        zplay_divide_develop
        WHERE
        date > DATE_SUB('{$dayid}',INTERVAL 7 DAY) AND date <= '{$dayid}'   group by os_id,app_id,game_creator
        UNION ALL
        SELECT
        '{$dayid}' as  date_time,
        os_id,
         app_id,
        game_creator,
        14 as date_type,
        round(sum(develop_cost_taxAfter),2) as develop_cost ,
        round(sum(tg_cost) + (case when sum(develop_cost_taxAfter)<0  then 0 else sum(develop_cost_taxAfter) end),2)  as total_cost ,
        round(sum(ff_income_taxAfter) + sum(ad_income_taxAfter) - (sum(tg_cost) + (case when sum(develop_cost_taxAfter)<0  then 0 else sum(develop_cost_taxAfter) end)),2)as total_profit
        FROM
        zplay_divide_develop
        WHERE
        date > DATE_SUB('{$dayid}',INTERVAL 14 DAY) AND date <= DATE_SUB('{$dayid}',INTERVAL 7 DAY)  group by os_id,app_id,game_creator

        UNION ALL
        SELECT
        '{$dayid}' as  date_time,
        os_id,
         app_id,
        game_creator,
        30 as date_type,
        round(sum(develop_cost_taxAfter),2) as develop_cost ,
        round(sum(tg_cost) + (case when sum(develop_cost_taxAfter)<0  then 0 else sum(develop_cost_taxAfter) end),2)  as total_cost ,
        round(sum(ff_income_taxAfter) + sum(ad_income_taxAfter) - (sum(tg_cost) + (case when sum(develop_cost_taxAfter)<0  then 0 else sum(develop_cost_taxAfter) end)),2)as total_profit
        FROM
        zplay_divide_develop
        WHERE
        date >= '{$month_begin}'  AND date <= '{$dayid}'  group by os_id,app_id,game_creator
        UNION ALL
        SELECT
        '{$dayid}' as  date_time,
        os_id,
         app_id,
        game_creator,
        60 as date_type,
        round(sum(develop_cost_taxAfter),2) as develop_cost ,
        round(sum(tg_cost) + (case when sum(develop_cost_taxAfter)<0  then 0 else sum(develop_cost_taxAfter) end),2)  as total_cost ,
        round(sum(ff_income_taxAfter) + sum(ad_income_taxAfter) - (sum(tg_cost) + (case when sum(develop_cost_taxAfter)<0  then 0 else sum(develop_cost_taxAfter) end)),2)as total_profit
        FROM
        zplay_divide_develop
        WHERE
        date >= DATE_SUB('{$month_begin}',INTERVAL 1 month) AND date <= '{$month_end}'  group by os_id,app_id,game_creator
        UNION ALL
        SELECT
        '{$dayid}' as  date_time,
        os_id,
        app_id,
        game_creator,
        90 as date_type,
        round(sum(develop_cost_taxAfter),2) as develop_cost ,
        round(sum(tg_cost) + (case when sum(develop_cost_taxAfter)<0  then 0 else sum(develop_cost_taxAfter) end),2)  as total_cost ,
        round(sum(ff_income_taxAfter) + sum(ad_income_taxAfter) - (sum(tg_cost) + (case when sum(develop_cost_taxAfter)<0  then 0 else sum(develop_cost_taxAfter) end)),2)as total_profit
        FROM
        zplay_divide_develop_cny
        WHERE
        date >= DATE_SUB('{$month_begin}',INTERVAL 2 month) AND date <= '{$month_end1}'  group by os_id,app_id,game_creator


        ";

        /*        -- UNION ALL
       --  SELECT
       --  '{$dayid}' as  date_time,
       --  os_id,
       --   app_id,
       -- game_creator,
       --  '-5' as date_type,
       --  round(sum(develop_cost_taxAfter),2) as develop_cost ,
       --  round(sum(tg_cost) + (case when sum(develop_cost_taxAfter)<0  then 0 else sum(develop_cost_taxAfter) end),2)  as total_cost ,
       --  round(sum(ff_income_taxAfter) + sum(ad_income_taxAfter) - (sum(tg_cost) + (case when sum(develop_cost_taxAfter)<0  then 0 else sum(develop_cost_taxAfter) end)),2)as total_profit
       --  FROM
       --  zplay_divide_develop
       --  where date <= '$dayid'
       --  group by os_id,app_id,game_creator*/
        $developer_info = DB::select($developer_sql);
        $developer_info = Service::data($developer_info);

//     print "insertBasicDataHomePage:",sql
        $array = [];
        $array_append = [];
        foreach($info as $k => $v) {
            foreach($dim_id as $a => $b) {
                if($b['dim_table_id'] == 'ltv')  continue;
                $array['date_type'] =$v['date_type'];
                $array['dim_id'] =$b['dim_id'];
                $array['value'] =$v[$b['dim_table_id']];
                $array['date_time'] =$v['date_time'];
                $array['os_id'] =$v['os_id'];
                $array['app_id'] =$v['app_id'];
                $array['game_creator'] =$v['game_creator'];
                $array['remark'] ="NULL";
                $array['create_time'] =date('Y-m-d',time());
                array_push($array_append,$array);


            }
            # code...
        }
        foreach($developer_info as $k1 => $v1) {
            // 数据补丁  91  开发者分成成本。
            $array['date_type'] =$v1['date_type'];
            $array['dim_id'] = 91;
            $array['value'] =$v1['develop_cost'];
            $array['date_time'] =$v1['date_time'];
            $array['os_id'] =$v1['os_id'];
            $array['app_id'] =$v1['app_id'];
            $array['game_creator'] =$v1['game_creator'];
            $array['remark'] ="NULL";
            $array['create_time'] =date('Y-m-d',time());
            array_push($array_append,$array);
            //93 总成本
            $array['date_type'] =$v1['date_type'];
            $array['dim_id'] = 93;
            $array['value'] =$v1['total_cost'];
            $array['date_time'] =$v1['date_time'];
            $array['os_id'] =$v1['os_id'];
            $array['app_id'] =$v1['app_id'];
            $array['game_creator'] =$v1['game_creator'];
            $array['remark'] ="NULL";
            $array['create_time'] =date('Y-m-d',time());
            array_push($array_append,$array);
            //92总利润
            $array['date_type'] =$v1['date_type'];
            $array['dim_id'] = 92;
            $array['value'] =$v1['total_profit'];
            $array['date_time'] =$v1['date_time'];
            $array['os_id'] =$v1['os_id'];
            $array['app_id'] =$v1['app_id'];
            $array['game_creator'] =$v1['game_creator'];
            $array['remark'] ="NULL";
            $array['create_time'] =date('Y-m-d',time());
            array_push($array_append,$array);
            # code...
        }

        DB::beginTransaction();

        DB::table($mysql_table)->truncate();

        if($array_append){
            //拆分批次
            $step = array();
            $i = 0;
            foreach ($array_append as $kkkk => $insert_data_info) {
                if ($kkkk % 1000 == 0) $i++;
                if ($insert_data_info) {
                    $step[$i][] = $insert_data_info;
                }
            }

            $is_success = [];
            if ($step) {
                foreach ($step as $k => $v) {
                    $result = DataImportLogic::insertAdReportInfo($mysql_table, $v);
                    if (!$result) {
                        DB::rollBack();
                        $is_success[] = $k;
                    }
                }
            }
        }
        $update_sql = "update $mysql_table a,(SELECT
        app_id,
        30 as date_type,
		sum(active_user) as active_user
        FROM
        zplay_user_tj_report_month
        WHERE
        date = '{$month_begin}' and platform_id ='ptj01'  group by app_id
        UNION ALL
        SELECT
        app_id,
        60 as date_type,
		sum(active_user) as active_user
        FROM
        zplay_user_tj_report_month
        WHERE
        date = DATE_SUB('{$month_begin}',INTERVAL 1 month) and platform_id ='ptj01'  group by app_id
        UNION ALL
        SELECT
        app_id,
        90 as date_type,
		sum(active_user) as active_user
        FROM
        zplay_user_tj_report_month
        WHERE
        date = DATE_SUB('{$month_begin}',INTERVAL 2 month) and platform_id ='ptj01' group by app_id) as b 
        set a.value= b.active_user
        where a.date_type= b.date_type and a.app_id = b.app_id and a.date_type in (30,60,90) and a.dim_id = 10";
        DB::UPDATE($update_sql);

        $update_sql = "update $mysql_table a,( select id,app_id from c_app where app_id in ('ga007001','gi018010','ga019001','go015010','ga025004','gi007008','gi014004','ga014001','gi055002','ga028001','wo029004','gg007004','gi008022','gi016003','ga018002','go012003','ga035001','gg042002','gi007011','ga012001','gi021003','ga025001','go019010','gg008008','gi008021','gi015008','ga016001','go007012','wa032001','gg014002','gi007009','ga008002','gi019008','ga021001','go018012','ga042001','gi008020','gi015006','ga015001','gi033002','ga028002','ga135002','gg007005','gi008023')) as b 
        set a.value= 0
        where a.app_id = b.id and a.date_type in (30,60,90) and a.dim_id = 10";
        DB::UPDATE($update_sql);

        DB::commit();

        echo $currency_type .' 结束时间：'.date('Y-m-d H:i:s')."\r\n";

    }



    public function getDimId($tb_name,$field,$value,$currency_type){
        $sql = "select dim_id,dim_table_id,dim_value from {$tb_name} where {$field}='{$value}' and currency_type = {$currency_type}";
        echo "getDimId:".$sql;
        $info = DB::select($sql);
        $info = Service::data($info);

        $dim_id = [];
        foreach ($info as $key => $value) {
            $dim_id[$value['dim_table_id']] = $value;
        }


        return $dim_id;
    }

}
