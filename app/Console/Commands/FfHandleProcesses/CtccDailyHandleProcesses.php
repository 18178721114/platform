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

class CtccDailyHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'CtccDailyHandleProcesses {begin_date?}  {end_date?} ';

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

        $del_sql1 = "delete from o_ff_ctcc_daily where pay_user+earning<=0  and  TIME >= '$begin_date'  and TIME <= '$end_date'";
        DB::delete($del_sql1);

        $del_sql2 = "update  o_ff_ctcc_daily   set channel_id_cr ='zy000' WHERE channel_id_cr is null and  TIME >= '$begin_date'  and TIME <= '$end_date'";
        DB::update($del_sql2);

        $del_sql = "update  o_ff_ctcc_daily t 
			set t.cp_name_cr=(case when t.business_name in ('印第安大冒险2','消灭星星2016','消灭星星2017') then '咪咕' else '掌游' end ),-- 修改资质
			t.channel_id_cr=(case when t.channel_name_cr='好盟渠道' then 'hmqd' else t.channel_id_cr end),-- 好萌渠道创建id
			t.business_divide=(case when  t.channel_name_cr='好盟渠道' then '好盟渠道' -- 好萌渠道
			when t.channel_id_cr='10000000' then '爱游戏' -- 电信爱游戏
			when t.channel_id_cr like '41%' then '41X'-- 41开头渠道
			when t.channel_id_cr like '800%' then '合作渠道800' -- 8000开头渠道
			when t.channel_id_cr like '83%' then '合作渠道互联网' -- 83开头渠道
			when t.channel_id_cr like '801%' then '合作渠道互联网' -- 801开头渠道
			when t.channel_id_cr  = 'zy000' then '未知渠道' -- 801开头渠道
			end),
			t.create_time=now()
			WHERE  TIME >= '$begin_date'  and TIME <= '$end_date'";
        DB::update($del_sql);

        $del_sql4 = "delete from zplay_ff_report_daily 
	where date between '$day_begin' 
	and '$day_end'
	and platform_id = 'pcr03'
	and pay_type = 1;";
        DB::delete($del_sql4);



        $insert_sql ="INSERT into  zplay_ff_report_daily 
			SELECT '' as id,
			 a.date_time as date,
			 application.new_app_id  as app_id,
			c_channel.channel_id as channel_id,
			a.country_id as country_id,
			'' as province_id,
			'pcr03' as platform_id,
			'' as  data_platform_id,
			'掌游' as platform_account,
			5 as publisher_id,
			a.business_id as business_id ,
			a.business_name as business_name,
			a.channel_id_cr as channel_id_plat,
			a.channel_name_cr as channel_name_plat,
			0 as publisher_type,
			0 as channel_type,
			'' as device_type,
			a.business_divide as business_divide,
			a.pay_user as pay_user,
			a.pay_time as pay_time,
			0 as pay_user_fail,
			0 as pay_time_fail,
			a.pay_user as pay_user_all,
			a.pay_time as pay_time_all,
			a.earning as earning,
			a.earning as earning_fix,
			a.EARNING_DIVIDE_PLAT as earning_divide_plat,
			a.EARNING_DIVIDE_PLAT_Z as earning_divide_plat_z,
			a.EARNING_DIVIDE_PLAT_PAY as earning_divide_plat_pay,
			a.EARNING_DIVIDE_PLAT_Z as earning_divide_publisher,
			a.EARNING_DIVIDE_CP as earning_divide_channel,
			a.INCOME_PLAT as income_plat,
			a.INCOME_FIX as income_publisher,
			a.INCOME_CHANNEL as income_channel,
			a.INCOME_FIX as income_fix,
			0 as tongji_type,
			'' as times,
			'1' as pay_type ,
			'' as remark,
			now() as create_time,
			now() as update_time

			from 

			(select 
			DATE_FORMAT(t.time, '%Y-%m-%d') AS date_time,-- 时间
			t.business_id,
			t.business_name,
			t.channel_id_cr,
			t.channel_name_cr,
			t.business_divide,
			t.pay_user,
			t.pay_time,
			c.cp_id,-- cpid
			g.game_id_z,-- 游戏id
			n.channel_id_z,-- zyid
			64 as country_id,-- 国家
			'' as version,-- 版本
			t.earning,-- 转换收入
			'pcr03' as plat_id,-- 平台id
			n.channel_own,
			b.cp_own,
			'1' as pay_type,-- 支付模式 
			p.plat_id as plat_id_z ,-- 子平台 
			t.earning*b.earning_plat_tr/100 as EARNING_DIVIDE_PLAT ,-- 计费平台分成
			t.earning*b.earning_plat_z_tr/100 as EARNING_DIVIDE_PLAT_Z,-- 计费子平台分成
			t.earning*b.earning_plat_pay_tr/100 as EARNING_DIVIDE_PLAT_PAY,-- 计费支付平台分成
			t.earning*b.earning_cp_tr/100 as EARNING_DIVIDE_CP,-- cp分成
			t.earning*b.earning_channel_tr/100 as EARNING_DIVIDE_CHANNEL,-- 渠道分成
			0 as INCOME_PLAT,-- 作为平台收入
			t.earning*b.income_cp_tr/100 as INCOME_CP,-- 作为cp收入
			(case when n.channel_own='1' then t.earning*b.earning_channel_tr/100 else 0 end) as INCOME_CHANNEL,-- 作为渠道收入
			t.earning*b.income_cp_tr/100+(case when n.channel_own='1' then t.earning*b.earning_channel_tr/100 else 0 end) as INCOME_FIX-- 真实收入=cp收入+渠道收入         
			from  o_ff_ctcc_daily t
			left join o_cfg_ff_cr_channel n on n.channel_id_cr=t.channel_id_cr and n.plat_id='pcr03'
			left join  o_code_dim_cp c on c.create_name=t.cp_name_cr and c.plat_id='pcr03'
			left join  o_code_dim_game_zplay g on g.create_name=t.business_name and g.plat_id='pcr03'
			left join  o_code_dim_plat p on p.create_name=t.carrier and p.plat_id_z='pcr03'
			left join  o_cfg_ff_business b on b.business_divide=t.business_divide and p.plat_id=b.plat_id_z  and c.cp_id=b.cp_id and b.plat_id='pcr03'
			where
			t.time >= $begin_date 
			and t.time <= $end_date
			and t.earning != 0.00
			and c.cp_id  is not null 
			and g.game_id_z is not null 
			and n.channel_id_z is  not null
			and b.business_divide is NOT  null 
			and p.plat_id is not null) a 
			LEFT JOIN application on a.game_id_z = gameid
			LEFT JOIN c_channel on a.channel_id_z =c_channel.td_channel_id";
         DB::insert($insert_sql);

        Artisan::call('FfSummaryProcesses',['begin_date'=>$day_begin,'end_date'=>$day_end,'platform_id'=>'pcr03']);

    }
}
