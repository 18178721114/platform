<?php

namespace App\Console\Commands;

use App\BusinessImp\DataSearchImp;
use App\BusinessLogic\AdReportLogic;
use App\Common\CurlRequest;
use App\Common\Service;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use PHPMailer\PHPMailer\PHPMailer;

class SendReportMailCommond extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'SendReportMailCommond';

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

        $MailInfo = DB::table('s_user_mail')->select(["userid","mailInfo","game_creator"])->get();
        $MailInfo = Service::data($MailInfo);
        if (!$MailInfo) return;
        foreach ($MailInfo as $MailData){
            $MailUserId = $MailData['userid'];
            $MailUserInfo = DB::table('user')->select(['id','name','user_account'])->where('id',$MailUserId)->first();
            $MailUserInfo = Service::data($MailUserInfo);
            if (!$MailUserInfo) continue;
            $user_account = $MailUserInfo['user_account'];
            $MailDataInfo = json_decode($MailData['mailInfo'],true);
            $hour = date('H');

            foreach ($MailDataInfo as $Info){
                if($Info['send_time'] == $hour){
                    $searchId = $Info['custom_id'];
                    $searchInfo = DB::table('s_search_custom')->select(['search_name','user_id','search_condition'])->where('id',$searchId)->first();
                    $searchInfo = Service::data($searchInfo);
                    $searchArr = json_decode($searchInfo['search_condition'],true);
                    $shortcut_id = isset($searchArr['shortcut_id']) ? $searchArr['shortcut_id'] : '';
                    if($shortcut_id){ //当为true时为快捷选择
                        if ($shortcut_id == 1){ // 今天
                            $star_time =  date('Y-m-d');
                            $end_time =  date('Y-m-d');
                        }elseif($shortcut_id == 2){ // 昨天
                            $star_time =  date('Y-m-d', strtotime('-1 day'));
                            $end_time =  date('Y-m-d', strtotime('-1 day'));
                        }elseif($shortcut_id == 3){ // 本月
                            $star_time =  date('Y-m-01', strtotime(date("Y-m-d")));
                            $end_time =  date('Y-m-d');
                        }elseif($shortcut_id == 4){ // 上月
                            $star_time =  date('Y-m-01', strtotime('-1 month'));
                            $end_time =  date('Y-m-t', strtotime('-1 month'));
                        }elseif($shortcut_id == 5){ // 最近7天
                            $star_time =  date('Y-m-d', strtotime('-7 days'));
                            $end_time =  date('Y-m-d');
                        }elseif($shortcut_id == 6){ // 最近30天
                            $star_time =  date('Y-m-d', strtotime('-30 days'));
                            $end_time =  date('Y-m-d');
                        }elseif($shortcut_id == 7){ // 最近90天
                            $star_time =  date('Y-m-d', strtotime('-90 days'));
                            $end_time =  date('Y-m-d');
                        }
                        $searchArr['end_time'] = $end_time;
                        $searchArr['start_time'] = $star_time;
                    }
                    $searchArr['guid'] = $MailUserId;
                    $searchArr['is_mail'] = 1;
                    $searchArr['search_name'] = $searchInfo['search_name'];
                    $searchArr['user_account'] = $user_account;
                    DataSearchImp::getSearchData($searchArr);
                }
            }
        }
    }

}
