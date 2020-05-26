<?php
/**
 * 越狱渠道成本分成
 */
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

class JailbreakChannelProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'JailbreakChannelProcesses {begin_date?}  {end_date?}';

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
        $begin_date = $this->argument('begin_date') ? $this->argument('begin_date'):date('Y-m-d',strtotime('-5 day'));
        $end_date = $this->argument('end_date') ? $this->argument('end_date'):date('Y-m-d',strtotime('-1 day'));

        var_dump($begin_date,$end_date);

        DB::beginTransaction();

        // 清空临时表数据
//        DB::table('erm_mm_data_temp')->truncate();
        DB::connection('mysql_channel_1')->table('mm_data_temp')->truncate();
        $curr_time = date('Y-m-d H:i:s');

        // 临时表插入数据

        $select_result = DB::select("select 
            'NUlL' as id,
            DATE_FORMAT(tg.date, '%Y-%m-%d') as day,
            ccha.td_channel_id as channelId,
            '' as mmId,
            (case when app.gameid = 'z01005' then 'PopStar！消灭星星官方正版' else app.app_name end) as gamename, 
            '0' as newuser,
            '0' as activeuser,
            '0' as starttimes,
            '0' as activerate,
            tg.cost_exc as sharing, -- 待定
            '0' as paytimes,
            '0' as payusers,
            tg.cost_exc as cost, -- 待定
            '0' as payrate,
            '0' as day2Retention,
            '0' as day7Retention,
            '0' as sessionlength,
            cha.`channel` as channelnote,
            '1' as ifInsert,
            '{$curr_time}' as inserttime,
            '0' as state,
            'zplay推广' as type
            from zplay_tg_report_daily tg 
            left join `application` app on tg.`app_id` = app.new_app_id 
            left join channel cha on cha.new_channel_id = tg.channel_id
            left join c_channel ccha on ccha.channel_id = tg.channel_id
            where (tg.channel_id is not NUll and tg.channel_id <> '') 
            and tg.date between '{$begin_date}' and '{$end_date}'
            ");
        $select_result = Service::data($select_result);
        if($select_result){

            $insert_result = DB::connection('mysql_channel_1')->table('mm_data_temp')->insert($select_result);
            if(!$insert_result){
                DB::rollBack();
            }

            // 查询临时表日期，删除正是表当前日期的数据
            $sel_result = DB::connection('mysql_channel_1')->select("select day from mm_data_temp group by day");
            $sel_result = Service::data($sel_result);

            if ($sel_result){
                // 如果有数据
                $del_result = DB::connection('mysql_channel_1')->delete(" delete from mm_data where day in ( select day from mm_data_temp group by day) ");
                if(!$del_result){
                    DB::rollBack();
                }
                // 插入数据
                $insert_pro_result = DB::connection('mysql_channel_1')->insert("
              insert into `mm_data` (`day`, `channelId`, `mmId`, `gamename`, `newuser`, `activeuser`, `starttimes`, `activerate`, `sharing`, `paytimes`, `payusers`, `cost`, `payrate`, `day2Retention`, `day7Retention`, `sessionlength`, `channelnote`, `ifInsert`, `inserttime`, `state`, `type`)
              select `day`, `channelId`, `mmId`, `gamename`, `newuser`, `activeuser`, `starttimes`, `activerate`, `sharing`, `paytimes`, `payusers`, `cost`, `payrate`, `day2Retention`, `day7Retention`, `sessionlength`, `channelnote`, `ifInsert`, `inserttime`, `state`, `type`
              from mm_data_temp ");
                if(!$insert_pro_result){
                    DB::rollBack();
                }
            }
        }


        DB::commit();

    }
}