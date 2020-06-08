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

class CtccMonthHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'CtccMonthHandleProcesses {begin_date?}  {end_date?} ';

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

        $del_sql2 = "delete from o_ff_ctcc_month where pay_user+total_income<=0 and   TIME >= '$begin_date'  and TIME <= '$end_date'";
        DB::delete($del_sql2);


        $del_sql1 = "update  o_ff_ctcc_month t-- 电信包月按'合作渠道互联网'计算分成
		set t.business_divide='合作渠道互联网',
		t.create_time=now() where  TIME >= '$begin_date'  and TIME <= '$end_date'";
        DB::update($del_sql1);

        $del_sql1 = "update o_ff_ctcc_month t -- 渠道id为空记为'未知'
		set t.channel_id_cr=(case when ( t.channel_id_cr is null or t.channel_id_cr ='') then 'mpay0000' else t.channel_id_cr end),
		t.channel_name_cr=(case when (t.channel_name_cr is null or t.channel_name_cr ='') then '未知渠道' else t.channel_name_cr end)
		where  TIME >= '$begin_date'  and TIME <= '$end_date'";
        DB::update($del_sql1);

        $del_sql1 = "update o_ff_ctcc_month t -- 渠道id为空记为'未知'
		set t.BUSINESS_NAME=(case when t.PRODUCT_NAME ='星星豪华大礼包' and  (t.BUSINESS_NAME is null or t.BUSINESS_NAME ='')  then '消灭星星全新版' else t.BUSINESS_NAME end)
		where  TIME >= '$begin_date'  and TIME <= '$end_date'";
        DB::update($del_sql1);

        $del_sql = "delete from zplay_ff_report_daily 
	where date '$day_begin'
	and '$day_end'
	and platform_id = 'pcr03'
	and pay_type = 2";
        DB::delete($del_sql);

        $insert_sql ="INSERT into `zplay_ff_report_daily` (`date`, `app_id`, `channel_id`, `country_id`, `province_id`, `platform_id`, `data_platform_id`, `platform_account`, `publisher_id`, `business_id`, `business_name`, `channel_id_plat`, `channel_name_plat`, `publisher_type`, `channel_type`, `device_type`, `business_divide`, `pay_user`, `pay_time`, `pay_user_fail`, `pay_time_fail`, `pay_user_all`, `pay_time_all`, `earning`, `earning_fix`, `earning_divide_plat`, `earning_divide_plat_z`, `earning_divide_plat_pay`, `earning_divide_publisher`, `earning_divide_channel`, `income_plat`, `income_publisher`, `income_channel`, `income_fix`, `tongji_type`, `times`, `pay_type`, `remark`, `create_time`, `update_time`) 
			SELECT 
			 a.date_time as date,
			 application.new_app_id  as app_id,
			c_channel.channel_id as channel_id,
			'64' as country_id,
			'' as province_id,
			'pcr03' as platform_id,
			'' as  data_platform_id,
			'掌游' as platform_account,
			5 as publisher_id,
			'' as business_id ,
			a.business_name as business_name,
			a.channel_id_cr as channel_id_plat,
			a.channel_name_cr as channel_name_plat,
			0 as publisher_type,
			0 as channel_type,
			'' as device_type,
			a.business_divide as business_divide,
			a.pay_user as pay_user,
			a.pay_user as pay_time,
			0 as pay_user_fail,
			0 as pay_time_fail,
			a.pay_user as pay_user_all,
			a.pay_user as pay_time_all,
			a.total_income as earning,
			a.total_income as earning_fix,
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
			'2' as pay_type ,
			'' as remark,
			now() as create_time,
			now() as update_time

			from 

			(select 
			DATE_FORMAT(t.time, '%Y-%m-%d') AS date_time,-- 时间
			t.business_name,
			t.product_name,
			t.firstbook_user,
			t.active_user,
			t.unsubscribe_user,
			t.firstbook_pay,
			t.rebook_pay,
			t.total_income,
			t.business_divide,
			t.business_id,
			t.rebook_user,
			t.pay_user,
			t.channel_id_cr,
			t.channel_name_cr,
			c.cp_id,-- cpid
			g.game_id_z,-- 游戏id
			f.channel_id_z,-- zyid
			'pcr03',-- 平台id
			f.channel_own,
			t.total_income*b.earning_plat_tr/100 as EARNING_DIVIDE_PLAT ,-- 计费平台分成
			t.total_income*b.earning_plat_z_tr/100 as EARNING_DIVIDE_PLAT_Z,-- 计费子平台分成
			t.total_income*b.earning_plat_pay_tr/100 as EARNING_DIVIDE_PLAT_PAY,-- 计费支付平台分成
			t.total_income*b.earning_cp_tr/100 as EARNING_DIVIDE_CP,-- cp分成
			t.total_income*b.earning_channel_tr/100 as EARNING_DIVIDE_CHANNEL,-- 渠道分成
			0 as INCOME_PLAT,-- 作为平台收入
			t.total_income*b.income_cp_tr/100 as INCOME_CP,-- 作为cp收入
			0 as INCOME_CHANNEL,-- 作为渠道收入
			t.total_income*b.income_cp_tr/100 as INCOME_FIX-- 真实收入=cp收入+渠道收入         
			from  o_ff_ctcc_month t
			left join o_code_dim_game_zplay g on g.create_name=t.business_name and g.plat_id='pcr03'
			left join  o_cfg_ff_business b on b.business_divide=t.business_divide and b.cp_id='cpzp01'  and b.plat_id_z='pcr03' and b.plat_id='pcr03'
			left join o_cfg_ff_cr_channel f on f.channel_id_cr=t.channel_id_cr and f.plat_id='pcr03' 
			left join o_code_dim_cp c on c.create_name=t.business_name and c.plat_id='pcr03'
			 
			where g.game_id_z is not null and f.channel_id_z is not null and c.cp_id is not null
			and t.time >= '$begin_date'
			and t.time <= '$end_date'
			) a 
			LEFT JOIN application on a.game_id_z = gameid
			LEFT JOIN c_channel on a.channel_id_z =c_channel.td_channel_id";
         DB::insert($insert_sql);

        Artisan::call('FfSummaryProcesses',['begin_date'=>$day_begin,'end_date'=>$day_end,'platform_id'=>'pcr03']);

    }
}
