<?php

namespace App\Console\Commands\TjHandleProcesses;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\BusinessLogic\DataImportLogic;
use App\Common\CommonFunction;
use App\Common\CurlRequest;
use App\Common\ParseDayid;
use App\Common\Service;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Artisan;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\ApplicationLogic;
use App\BusinessLogic\CommonLogic;
use App\BusinessImp\PlatformImp;
use App\BusinessLogic\PlatformLogic;

// talkingdata用户统计
class TdUserTjHandleProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'TdUserTjHandleProcesses {dayid?} {data_account?}';

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
//        define('MYSQL_TABLE_NAME','zplay_user_tj_report_daily');
        $source_id = 'ptj02';
        $source_name = 'TalkingData国内用户';

        $dayid = $this->argument('dayid') ? $this->argument('dayid'):date('Y-m-d',strtotime('-1 day'));

        try {

//            $error_msg = $dayid . '号，' . $source_name . '统计省份数据处理程序开始时间：' . date('Y-m-d H:i:s');
//            DataImportImp::saveDataErrorLog(2, $source_id, $source_name, 1, $error_msg);

            $talkingdata_sql = "update talkingdata_china_session set channel_name = 'zy246' where channel_name = '2200109612' and day = '{$dayid}'";
            DB::update($talkingdata_sql);
            $talkingdata_sql = "update talkingdata_china_session set channel_name = 'zy210' where channel_name = '2200131904' and day = '{$dayid}'";
            DB::update($talkingdata_sql);

            $talkingdata_sql = "update talkingdata_china_user set channel_name = 'zy246' where channel_name = '2200109612' and tjdate = '{$dayid}'";
            DB::update($talkingdata_sql);
            $talkingdata_sql = "update talkingdata_china_user set channel_name = 'zy210' where channel_name = '2200131904' and tjdate = '{$dayid}'";
            DB::update($talkingdata_sql);

            $talkingdata_sql = "update talkingdata_foreign_user set channel_name = 'zy246' where channel_name = '2200109612' and day = '{$dayid}'";
            DB::update($talkingdata_sql);
            $talkingdata_sql = "update talkingdata_foreign_user set channel_name = 'zy210' where channel_name = '2200131904' and day = '{$dayid}'";
            DB::update($talkingdata_sql);

            //查询pgsql 的数据
            $talkingdata_sql = "select * from talkingdata_china_user where tjdate = '$dayid'";
            $info = DB::select($talkingdata_sql);
            $info = Service::data($info);

            if (!$info) {
//            $error_msg = $dayid.'号，'.$source_name.'统计省份数据处理程序获取原始数据为空';
//            DataImportImp::saveDataErrorLog(2,$source_id,$source_name,1,$error_msg);
                exit;
            }

            //获取匹配应用的数据
            $sql = "SELECT DISTINCT
        c_app.id,
        c_app.app_id,
        c_app.os_id,
        if(c_app.os_id = 1,2,1) as td_os_id,
        c_app_statistic.td_app_id,
        c_app_statistic.api_key,
        c_app_statistic_version.statistic_app_name,
        c_app_statistic_version.app_version,
        c_app_statistic_version.statistic_version,
        c_app_statistic_version.ad_status,
        c_app_statistic_version.channel_id,
        c_channel.td_channel_id,
        c_channel.channel_id as c_channel_id
        FROM
        c_app
        LEFT JOIN c_app_statistic ON c_app.id = c_app_statistic.app_id
        LEFT JOIN c_app_statistic_version ON c_app_statistic.id = c_app_statistic_version.app_statistic_id
        LEFT JOIN c_channel ON c_channel.id = c_app_statistic_version.channel_id
        WHERE
        c_app_statistic.statistic_type = 2
        AND c_app_statistic_version.ad_status != 2
        AND c_app.`release_region_id` in (1,3)";

            $app_list = DB::select($sql);
            $app_list = Service::data($app_list);
            if (!$app_list) {
                $error_msg = $source_name . '统计省份数据处理程序应用数据查询为空';
                DataImportImp::saveDataErrorLog(2, $source_id, $source_name, 1, $error_msg);
                exit;
            }

            //获取对照表国家信息
            $country_map = [];
            $country_info = CommonLogic::getCountryList($country_map)->get();
            $country_info = Service::data($country_info);
            if (!$country_info) {
                $error_msg = $source_name . '统计省份数据处理程序国家信息数据查询为空';
                DataImportImp::saveDataErrorLog(2, $source_id, $source_name, 1, $error_msg);
                exit;
            }


            $array = [];
            $num = 0;
            $num_country = 0;
            $error_log_arr = [];
            $error_detail_arr = [];
            foreach ($info as $k => $v) {

                foreach ($app_list as $app_k => $app_v) {
                    if (($v['app_id'] == $app_v['td_app_id']) && ($v['version_id'] == $app_v['statistic_version']) && ($v['platform_id'] == $app_v['td_os_id']) && (($v['channel_name'] == $app_v['td_channel_id']) || ($v['channel_name'] == $app_v['channel_id']) || ($v['channel_name'] == $app_v['c_channel_id']))) {
                        $array[$k]['app_id'] = $app_v['app_id'];
                        $array[$k]['ad_status'] = $app_v['ad_status'];
                        $array[$k]['channel_id'] = $app_v['channel_id'];
                        $num = 0;
                        break;
                    } else {
                        //广告位配置未配置
                        $num++;
                    }
                }

                if ($num) {
                    //var_dump($json_info['campaign_id']);
                    $error_log_arr['campaign_id'][] = $v['app_name'] . '#' . $v['channel_name'] . '#' . $v['version_id'];
                }

                // todo 匹配国家用
                $array[$k]['country_id'] = 335;
                foreach ($country_info as $country_k => $country_v) {
                    if ((isset($v['country_name']) && ($v['country_name'] == $country_v['name']))) {
                        $array[$k]['country_id'] = $country_v['c_country_id'];
                        $num_country = 0;
                        break;
                    } else {
                        //国家配置失败
                        $num_country++;
                    }
                }

                if ($num_country) {
                    $error_log_arr['country'][] = isset($v['country_id']) ? $v['country_id'] : 'Unknown Region';
                }
                if (($num + $num_country) > 0) {
                    $error_detail_arr[$k]['platform_id'] = $source_id;
                    $error_detail_arr[$k]['platform_name'] = $source_name;
                    $error_detail_arr[$k]['platform_type'] = 1;
                    $error_detail_arr[$k]['err_date'] = $dayid;
                    $error_detail_arr[$k]['first_level_id'] = $v['app_id'];
                    $error_detail_arr[$k]['first_level_name'] = $v['app_name'];
                    $error_detail_arr[$k]['second_level_id'] = $v['version_id'];
                    $error_detail_arr[$k]['second_level_name'] = $v['channel_name'];
                    $error_detail_arr[$k]['money'] = $v['newuser']; // 流水原币
                    $error_detail_arr[$k]['account'] = 'weibo@zplay.cn';
                    $error_detail_arr[$k]['create_time'] = date('Y-m-d H:i:s');
                    $error_detail_arr[$k]['td_err_type'] = 2;
                    unset($array[$k]);
                    //插入错误数据
                    continue;
                }


                //获取每个渠道的平均启动时长
                $session_map = [];
                $session_map['day'] = $dayid;
//            $session_map['game_name'] = $v['app_name'];
                $session_map['appid'] = $v['app_id'];
                $session_map['channel_name'] = $v['channel_name'];
                $fields = ['day', 'game_name', 'channel_name', 'sessionlength'];
                $session_info = CommonLogic::getChannelAvgSessionLength($session_map, $fields)->first();
                $session_info = Service::data($session_info);
                if ($session_info) {
                    $avg_session_lenght = $session_info['sessionlength'];
                } else {
                    $avg_session_lenght = 0;
                }

                $array[$k]['account'] = 'weibo@zplay.cn';
                $array[$k]['td_app_id'] = isset($app_v['td_app_id']) ? addslashes($app_v['td_app_id']) : '';
                $array[$k]['date'] = $dayid;
                $array[$k]['channel_plat_name'] = $v['channel_name'];
                $array[$k]['version_id'] = isset($app_v['app_version']) ? addslashes($app_v['app_version']) : '';
                $array[$k]['new_user'] = $v['newuser'];
                $array[$k]['active_user'] = isset($v['activeuser']) ? addslashes($v['activeuser']) : '';
                $array[$k]['session_time'] = isset($v['session']) ? addslashes($v['session']) : '';
                $array[$k]['session_length'] = $avg_session_lenght ? round($v['session'] * $avg_session_lenght) : 0;
                $array[$k]['flow_type'] = 0;
                $array[$k]['type'] = 1;
                $array[$k]['platform_id'] = $source_id;
                $array[$k]['create_time'] = date('Y-m-d H:i:s');
            }

            // 保存错误信息
            if ($error_log_arr) {
                $error_msg_array = [];
                $error_msg_mail = [];
                $error_log_arr = Service::shield_error($source_id,$error_log_arr);

                if (isset($error_log_arr['campaign_id']) && !empty($error_log_arr['campaign_id'])) {
                    sort($error_log_arr['campaign_id']);
                    $campaign_id = implode(',', array_unique($error_log_arr['campaign_id']));
                    $error_msg_array[] = '应用ID匹配失败,ID为:' . $campaign_id;
                    $error_msg_mail[] = '应用ID匹配失败，ID为：' . $campaign_id;
                }
                if (isset($error_log_arr['country']) && !empty($error_log_arr['country'])) {
                    sort($error_log_arr['country']);
                    $country = implode(',', array_unique($error_log_arr['country']));
                    $error_msg_array[] = '国家匹配失败,ID为:' . $country;
                    $error_msg_mail[] = '国家匹配失败，ID为：' . $country;
                }

                if(!empty($error_msg_array)) {
                    DataImportImp::saveDataErrorLog(2, $source_id, $source_name, 1, implode(';', $error_msg_array));
                    // 发送邮件
//                    CommonFunction::sendMail($error_msg_mail, $source_name . '统计省份数据处理error');
                }
                DataImportImp::saveDataErrorMoneyLog($source_id, $dayid, $error_detail_arr, 1);
            }

            // 保存正确数据
            if ($array) {
                DB::beginTransaction();
                $map_delete = [];
                $map_delete['platform_id'] = $source_id;
                $map_delete['date'] = $dayid;
                $map_delete['type'] = 1;
                DataImportLogic::deleteMysqlHistoryData('zplay_user_tj_report_daily', $map_delete);
                //拆分批次
                $step = array();
                $i = 0;
                foreach ($array as $kkkk => $insert_data_info) {
                    if ($kkkk % 500 == 0) $i++;
                    if ($insert_data_info) {
                        $step[$i][] = $insert_data_info;
                    }
                }

                $is_success = [];
                if ($step) {
                    foreach ($step as $k => $v) {
                        $result = DataImportLogic::insertAdReportInfo('zplay_user_tj_report_daily', $v);
                        if (!$result) {
                            DB::rollBack();
                            $is_success[] = $k;
                        }
                    }
                }
                DB::commit();
                // 处理未知省份数据
                sleep(10);
                self::getUnknowProvince($dayid, $source_id);

                // todo 删除星星部分版本数据
                DB::beginTransaction();

                $star_map_delete = [];
                $star_map_delete['platform_id'] = $source_id;
                $star_map_delete['date'] = $dayid;
                $star_map_delete['type'] = 1;
                $star_map_delete['app_id'] = 'gi007008';
                $star_map_delete['in'] = ['version_id',[5342,5332,5321,530,5291]];
                $star_result = DataImportLogic::deleteMysqlHistoryData('zplay_user_tj_report_daily', $star_map_delete);
                if (!$star_result){
                    DB::rollBack();
                }
                DB::commit();

                // 调用存储过程更新总表数据
                //DB::update("call tj_summary('$source_id')");
//            Artisan::call('TjSummaryProcesses',['platform_id'=>$source_id]);
                // 查询广告数据
                $report_map = [];
                $report_map['platform_id'] = $source_id;
                $report_map['date'] = $dayid;
                // $report_map['type'] = 1;
                $group_by = ['platform_id', 'date', 'account'];
                $report_list = PlatformLogic::getAdReportSum('zplay_user_tj_report_daily', $report_map)->select(DB::raw("sum(new_user) as cost"), 'platform_id', 'date', 'account')->groupBy($group_by)->get();
                $report_list = Service::data($report_list);
                if ($report_list) {
                    // 保存广告平台
                    foreach ($report_list as $value) {
                        PlatformImp::add_platform_status($source_id, $value['account'], $value['cost'], $dayid);
                    }

//                    $error_msg = $dayid . '号，' . $source_name . '统计省份数据处理程序结束时间：' . date('Y-m-d H:i:s');
//                    DataImportImp::saveDataErrorLog(2, $source_id, $source_name, 1, $error_msg);

                }
            } else {
                echo '暂无匹配成功数据';
            }
        }catch (\Exception $e) {
            // 异常报错
            $message = "{$dayid}号, " . $source_name . "统计平台程序报错,报错原因:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, $source_id, $source_name, 1, $message);
            $error_msg_arr[] = $message;
//            CommonFunction::sendMail($error_msg_arr, '统计平台程序error');
            exit;
        }
    }


    // 计算未知省份数据
    private static function getUnknowProvince($dayid,$source_id){

        // 获取分渠道总数
        $pro_user_channel_sql = " select
                    b.`app_name`,
                    a.`app_id`,
                    a.`td_app_id`,
                    a.`account`,
                    a.`date`,
                    a.`channel_plat_name`,
                    a.`channel_id`,
                    a.`country_id`,
                    a.`platform_id`,
                    sum(a.`new_user`) as `new_user`,
                    sum(a.`active_user`) as `active_user`,
                    sum(a.`session_time`) as `session_time` ,
                    a.`type` from zplay_user_tj_report_daily a
                    left join c_app b on a.app_id = b.app_id
                    where date = '{$dayid}' and platform_id = '{$source_id}' and type = 1 group by app_id,`channel_plat_name`";
        $pro_user_channel_list = DB::select($pro_user_channel_sql);
        $pro_user_channel_list = Service::data($pro_user_channel_list);
//        var_dump(count($pro_user_channel_list));

        // 国家获取分渠道总数
        $pro_country_channel_sql = " select
                    b.`app_name`,
                    a.`app_id`,
                    a.`td_app_id`,
                    a.`account`,
                    a.`date`,
                    a.`channel_plat_name`,
                    a.`channel_id`,
                    a.`country_id`,
                    a.`platform_id`,
                    sum(a.`new_user`) as `new_user`,
                    sum(a.`active_user`) as `active_user`,
                    sum(a.`session_time`) as `session_time` ,
                    a.`type` from zplay_user_tj_report_daily a
                    left join c_app b on a.app_id = b.app_id
                    where date = '{$dayid}' and platform_id = '{$source_id}' and type = 2  group by td_app_id,`channel_plat_name`";
        $pro_country_channel_list = DB::select($pro_country_channel_sql);
        $pro_country_channel_list = Service::data($pro_country_channel_list);
//        var_dump(count($pro_country_channel_list));

        // 国内用户总数
//        $china_total_user = DB::select("select c.app_name,c.app_id,b.td_app_id,day as date, a.channel_name as channel_plat_name,d.id as channel_id,  `new_user`, `active_user`, `session` as session_time,`sessionlength`, (`session` * `sessionlength`) as session_length
//                    from talkingdata_china_session a
//                    left join (select DISTINCT app_id,`td_app_id` from c_app_statistic) b on a.appid = b.td_app_id
//                    left join c_app c on c.id = b.app_id
//                    left join c_channel d on d.`td_channel_id` = a.channel_name
//                    where day = '{$dayid}' and c.`release_region_id` = 3 group by a.appid,a.channel_name");

//        $china_total_user = DB::select("select c.tmp_app_name as app_name,c.tmp_app_id as app_id,a.appid as td_app_id,day as date, a.channel_name as channel_plat_name,if(d.id is null,224,d.id) as channel_id,  `new_user`, `active_user`, `session` as session_time,`sessionlength`, (`session` * `sessionlength`) as session_length from talkingdata_china_session a  left join ( select DISTINCT b.app_id as bapp_id,b.td_app_id as btd_app_id ,tmp.platform_id as tmp_platform_id,tmp.id as tmp_id,tmp.app_id as tmp_app_id,tmp.app_name as tmp_app_name from c_app_statistic b  left join ( select if(c_app.os_id = 1,2,1) as platform_id,id,app_id,app_name from c_app where `release_region_id` in (1,3) ) tmp on tmp.id = b.app_id  ) c on c.btd_app_id = a.appid and a.platformid = c.tmp_platform_id left join c_channel d on d.`td_channel_id` = a.channel_name where day = '{$dayid}'  and c.tmp_app_id is not NULL and c.tmp_app_id  <> '' group by a.appid,a.channel_name");
        $china_total_user = DB::select("select c.tmp_app_name as app_name,c.tmp_app_id as app_id,a.appid as td_app_id,day as date, a.channel_name as channel_plat_name,if(d.id is null,224,d.id) as channel_id,  `new_user`, `active_user`, `session` as session_time,`sessionlength`, (`session` * `sessionlength`) as session_length from talkingdata_china_session a  left join ( select DISTINCT b.app_id as bapp_id,b.td_app_id as btd_app_id ,tmp.platform_id as tmp_platform_id,tmp.id as tmp_id,tmp.app_id as tmp_app_id,tmp.app_name as tmp_app_name from c_app_statistic b  left join ( select if(c_app.os_id = 1,2,1) as platform_id,id,app_id,app_name from c_app where `release_region_id` in (1,3) ) tmp on tmp.id = b.app_id  ) c on c.btd_app_id = a.appid and a.platformid = c.tmp_platform_id left join c_channel d on ( d.`td_channel_id` = a.channel_name  or  d.`channel_id` = a.channel_name ) where day = '{$dayid}'  and c.tmp_app_id is not NULL and c.tmp_app_id  <> '' group by a.appid,a.channel_name");
        $china_total_user = Service::data($china_total_user);
//        var_dump(count($china_total_user));

        // 计算相同应用相同渠道相同日志 未知省份新增 活跃 启动次数
        $new_unkown_province = [];
        $total_new_unkown_province = [];
        if ($china_total_user){
            foreach ($china_total_user as $ctuk => $ctuv){
                if ($pro_user_channel_list){
                    foreach ($pro_user_channel_list as $puclk => $puclv){
                        if (($ctuv['td_app_id'] == $puclv['td_app_id']) && ($ctuv['channel_plat_name'] == $puclv['channel_plat_name']) && ($ctuv['date'] == $puclv['date'])){
                            $new_user = $ctuv['new_user'] - $puclv['new_user'];
                            $active_user = $ctuv['active_user'] - $puclv['active_user'];
                            $session_time = $ctuv['session_time'] - $puclv['session_time'];
                            $new_user = $new_user > 0 ? $new_user : 0;
                            $active_user = $active_user > 0 ? $active_user : 0;
                            $session_time = $session_time > 0 ? $session_time : 0;
                            unset($china_total_user[$ctuk]);
                            if ($new_user <= 0 && $active_user <= 0 && $session_time <= 0) continue;

                            $puclv['new_user'] = $new_user;
                            $puclv['active_user'] = $active_user;
                            $puclv['session_time'] = $session_time;
                            $puclv['session_length'] = $ctuv['sessionlength'];
                            $puclv['ad_status'] = 0;
                            $puclv['version_id'] = '';
                            $puclv['country_id'] = 335;
                            $puclv['create_time'] = date('Y-m-d H:i:s');
                            $new_unkown_province[] = $puclv;
                        }
                    }
                }
            }

            foreach ($china_total_user as $new_k => $new_v){
                $new_v['account'] = 'weibo@zplay.cn';
                $new_v['type'] = 1;
                $new_v['platform_id'] = 'ptj02';
                $new_v['country_id'] = 335;
                $new_v['version_id'] = '';
                $new_v['create_time'] = date('Y-m-d H:i:s');
                $new_v['ad_status'] = 0;
                unset($new_v['sessionlength']);
                $new_unkown_province[] = $new_v;
            }
            foreach ($new_unkown_province as $ctuk => $ctuv){
                if ($pro_country_channel_list){
                    foreach ($pro_country_channel_list as $puclk => $puclv){
                        if (($ctuv['td_app_id'] == $puclv['td_app_id']) && ($ctuv['channel_plat_name'] == $puclv['channel_plat_name']) && ($ctuv['date'] == $puclv['date'])){
                            $new_user = $ctuv['new_user'] - $puclv['new_user'];
                            $active_user = $ctuv['active_user'] - $puclv['active_user'];
                            $new_user = $new_user > 0 ? $new_user : 0;
                            $active_user = $active_user > 0 ? $active_user : 0;
                            $session_time = $ctuv['session_time'] - $puclv['session_time'];
                            $session_time = $session_time > 0 ? $session_time : 0;
                            unset($new_unkown_province[$ctuk]);
                            if ($new_user <= 0 && $active_user <= 0 && $session_time <= 0) continue;

                            $puclv['new_user'] = $new_user;
                            $puclv['active_user'] = $active_user;
                            $puclv['session_time'] = $session_time;

                            $puclv['session_length'] = $session_time * $ctuv['session_length'];
                            $puclv['ad_status'] = 0;
                            $puclv['version_id'] = '';
                            $puclv['type'] = 1;
                            $puclv['country_id'] = 335;
                            $puclv['create_time'] = date('Y-m-d H:i:s');
                            $total_new_unkown_province[] = $puclv;
                        }
                    }
                }
            }

            foreach ($new_unkown_province as $ctuk => $ctuv){
                $total_new_unkown_province[] = $ctuv;
            }

        }

        // 更新基础信息表
        if ($total_new_unkown_province){
            DB::beginTransaction();
            foreach ($total_new_unkown_province as $unpdk => $unpdv){
                unset($unpdv['app_name']);
                $total_new_unkown_province[$unpdk] = $unpdv;
            }

            //拆分批次
            $step_n = array();
            $ii = 0;
            foreach ($total_new_unkown_province as $kk_kk => $insert_data_info_new) {
                if ($kk_kk % 500 == 0) $ii++;
                if ($insert_data_info_new) {
                    $step_n[$ii][] = $insert_data_info_new;
                }
            }

            $is_success = [];
            if ($step_n) {
                foreach ($step_n as $kk => $vv) {
                    $result = DB::table('zplay_user_tj_report_daily')->insert($vv);
                    if (!$result) {
                        DB::rollBack();
                    }
                }
            }
            DB::commit();

        }
    }

    // 保存日志
    private static function saveLog($platform_name = '未知', $message = ''){

        $fileName = date('Y-m-d',time());
        $dir = './storage/adDataLogs';

        if (!is_dir($dir)) {
            mkdir($dir,0777,true);
        }
        $logFilename = $dir.'/'.$platform_name.'_ad'.'.log';
        //生成日志
        file_put_contents( $logFilename,$message . "\n\n",FILE_APPEND);
    }
}