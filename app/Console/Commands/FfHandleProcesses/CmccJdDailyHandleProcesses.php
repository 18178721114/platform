<?php

namespace App\Console\Commands\FfHandleProcesses;

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

class CmccJdDailyHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'CmccJdDailyHandleProcesses {begin_date?}  {end_date?} ';

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
        $day_begin = $this->argument('begin_date') ? $this->argument('begin_date'):date('Y-m-01',strtotime('-1 month'));
        $day_end = $this->argument('end_date') ? $this->argument('end_date'):date("Y-m-d",strtotime("$day_begin +1 month -1 day"));
        $begin_date = date('Ymd',strtotime($day_begin));
        $end_date = date('Ymd',strtotime($day_end));

        $del_sql = "delete from o_ff_cmcc_jd_daily where pay_user+pay_time+earning<=0  and  TIME >= '$begin_date'  and TIME <= '$end_date'";
        DB::delete($del_sql);

        $del_sql = "update o_ff_cmcc_jd_daily set business_name=rtrim(business_name),business_divide=rtrim(business_divide)  where   TIME >= '$begin_date'  and TIME <= '$end_date'";
         DB::update($del_sql);

        $del_sql = "update o_ff_cmcc_jd_daily  
	set business_name_split = (case   when instr(business_name, '_') >0 then  substr(business_name, 1, instr(business_name, '_')-1) 
	else business_name end),
	business_divide=  (case when   instr(business_name, '_') >0 then substr(business_name,instr(business_name, '_') + 1,length(business_name))
  else  substr(business_name,-3) end), 
	province=(case when  province is not null then province   else 'δ֪' end),
	create_time=NOW() where  TIME >= '$begin_date'  and TIME <= '$end_date'";
        DB::update($del_sql);


        $select_result = DB::select(" select * from o_ff_cmcc_jd_daily where time >= $begin_date and time <= $end_date limit 1");
        $select_result = Service::data($select_result);
        if ($select_result){



            $del_sql = "delete from zplay_ff_report_daily 
	where date between DATE_FORMAT('$begin_date', '%Y-%m-%d') 
	and DATE_FORMAT('$end_date', '%Y%m%d') 
	and platform_id = 'pcr01jd'
	and pay_type = 1;";
            DB::delete($del_sql);



        $insert_sql ="INSERT INTO `zplay_ff_report_daily` (`date`, `app_id`, `channel_id`, `country_id`, `province_id`, `platform_id`, `data_platform_id`, `platform_account`, `publisher_id`, `business_id`, `business_name`, `channel_id_plat`, `channel_name_plat`, `publisher_type`, `channel_type`, `device_type`, `business_divide`, `pay_user`, `pay_time`, `pay_user_fail`, `pay_time_fail`, `pay_user_all`, `pay_time_all`, `earning`, `earning_fix`, `earning_divide_plat`, `earning_divide_plat_z`, `earning_divide_plat_pay`, `earning_divide_publisher`, `earning_divide_channel`, `income_plat`, `income_publisher`, `income_channel`, `income_fix`, `tongji_type`, `times`, `pay_type`, `remark`, `create_time`, `update_time`) 
select 
DATE_FORMAT(t.time, '%Y-%m-%d') AS date,-- 时间
-- c.cp_id,-- cpid
-- g.game_id_z as app_id,-- 老游戏id
app.new_app_id as app_id,-- 新游戏id
-- n.channel_id_z as td_channel_id,-- zyid
chan.channel_id as channel_id,-- zyid
'64' as country_id,-- 国家
p.c_country_id as province_id,
'pcr01jd' as platform_id,-- 平台id
'' as data_platform_id,
'伟恩' as platform_account,
4 as publisher_id,
t.business_id as business_id,
t.business_name as business_name,
t.channel_id_cr as channel_id_plat,
t.channel_name_cr as channel_name_plat,
0 as publisher_type,
0 as channel_type,
'' as device_type,
t.business_divide as business_divide,
t.pay_user as pay_user,
t.pay_time as pay_time,
0 as pay_user_fail,
0 as pay_time_fail,
t.pay_user as pay_user_all,
t.pay_time as pay_time_all,
t.earning as earning,-- 转换收入
t.earning as earning_fix,-- 修正收入
t.earning*b.earning_plat_tr/100 as earning_divide_plat,-- 计费平台分成
t.earning*b.earning_plat_z_tr/100 as earning_divide_plat_z,-- 计费子平台分成
t.earning*b.earning_plat_pay_tr/100 as earning_divide_plat_pay,-- 计费支付平台分成
t.earning*b.earning_cp_tr/100 as earning_divide_publisher,-- cp分成
t.earning*b.earning_channel_tr/100 as earning_divide_channel,-- 渠道分成
0 as income_plat,-- 作为平台收入
t.earning*b.income_cp_tr/100 as income_publisher, -- 作为cp收入
(case when n.channel_own='1' then t.earning*b.earning_channel_tr/100 else 0 end) as income_channel , -- 作为渠道收入
t.earning*b.income_cp_tr/100+(case when n.channel_own='1' then t.earning*b.earning_channel_tr/100 else 0 end) as income_fix, -- 真实收入=cp收入+渠道收入
0 as tongji_type,
'' as times,
'1' as pay_type,
'' as remark,
now() as create_time,
now() as update_time 
from o_ff_cmcc_jd_daily t
left join o_cfg_ff_cr_channel n on n.channel_id_cr=t.channel_id_cr and n.plat_id='pcr01jd'
left join  o_code_dim_cp c on c.create_id=t.cp_id_cr and c.plat_id='pcr01jd'
left join  c_country_corresponding p on p.name = t.province 
left join  o_code_dim_game_zplay g on g.create_name=t.business_name_split and g.plat_id='pcr01jd'
left join application app on app.`gameid` = g.game_id_z
left join  o_cfg_ff_business b on b.business_divide=t.business_divide and c.cp_id=b.cp_id and b.plat_id='pcr01jd'
left join c_channel chan on chan.td_channel_id = n.channel_id_z
where t.time >= $begin_date 
and t.time <= $end_date
and c.cp_id  is not null 
and g.game_id_z is not null  
and n.channel_id_z is not null 
and b.business_divide is not  null 
and p.id is not null 
group by t.time,g.game_id_z,n.channel_id_z,t.province,t.business_id,t.business_name,t.channel_id_cr,t.channel_name_cr,b.cp_own,n.channel_own,t.business_divide";
         DB::insert($insert_sql);

            Artisan::call('FfSummaryProcesses',['begin_date'=>$day_begin,'end_date'=>$day_end,'platform_id'=>'pcr01jd']);
        }


    }
}
