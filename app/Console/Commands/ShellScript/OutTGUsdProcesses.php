<?php

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

class OutTGUsdProcesses extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'OutTGUsdProcesses {begin_date?} {end_date?}';

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
        $begin_date = $this->argument('begin_date') ? $this->argument('begin_date') : date('Y-m-d',strtotime('-8 day'));
        $end_date = $this->argument('end_date') ? $this->argument('end_date') : date('Y-m-d');

        try {
            $bengin_time = time();

            DB::beginTransaction();
            $sel_sql = "select count(1) as count  FROM
        zplay_basic_tg_report_total_usd
        WHERE
         date >= '$begin_date'  and   date <= '$end_date' ";
            $sel_info = DB::select($sel_sql);
            $sel_info = Service::data($sel_info);
            if ($sel_info[0]['count'] != 0) {
                $del_sql = "         DELETE
            FROM
                zplay_basic_tg_report_total_usd
            WHERE
                date BETWEEN '$begin_date'
            AND '$end_date';";
                $update_info = DB::delete($del_sql);

                if (!$update_info) {
                    DB::rollBack();
                }
            }

            $insert_sql = "
        INSERT INTO zplay_basic_tg_report_total_usd (
                date,
                app_id,
                os_id,
                country_id,
                new,
                active,
                earning_all,
                earning_all_cny,
                new_appsflyer,
                download,
                new_nature,
                new_nonature,
                cost,
                cost_cny,
                new_tg,
                cost_tg,
                new_fake,
                cost_fake,
                cost_optimize,
                new_appsflyer_phone,
                download_phone,
                new_nature_phone,
                new_nonature_phone,
                cost_phone,
                new_tg_phone,
                cost_tg_phone,
                new_fake_phone,
                cost_fake_phone,
                cost_optimize_phone,
                new_appsflyer_pad,
                download_pad,
                new_nature_pad,
                new_nonature_pad,
                cost_pad,
                new_tg_pad,
                cost_tg_pad,
                new_fake_pad,
                cost_fake_pad,
                cost_optimize_pad,
                cost_dev,
                create_time,
company_id
            ) SELECT
                m.date,
                a.id,
                a.os_id,
                m.country_id,
                -- ??????????????????
                sum(m.new) AS new,
                sum(m.active) AS active,
                -- ?????????
                (
                    sum(m.earning_ad + m.earning_ff)
                ) AS earning_all,
                (
                    sum(m.earning_ad + m.earning_ff)
                ) AS earning_all_cny,
                -- ??????????????????
                sum(m.new_appsflyer) AS new_appsflyer,
                -- appflyer??????
                sum(m.download) AS download,
                -- ?????????
                sum(m.new_nature) - sum(m.new_fake) AS new_nature,
                -- appflyer?????????
        -- sum(m.new_tg + m.new_fake) AS new_nonature,
                sum(m.new_tg ) AS new_nonature,
                -- ????????????
                (
                    sum(
                        m.cost_tg  + m.cost_optimize
                    )
                ) AS cost,
                (
                    sum(
                        m.cost_tg  + m.cost_optimize
                    )
                ) AS cost_cny,
                -- ???????????????
                sum(m.new_tg) AS new_tg,
                -- ???????????????
                (sum(m.cost_tg)) AS cost_tg,
                -- ??????????????????
                sum(m.new_fake) AS new_fake,
                -- ????????????
                (sum(m.cost_fake)) AS cost_fake,
                -- ??????????????????
                (sum(m.cost_optimize)) AS cost_optimize,
                -- aso??????
                -- ipnone??????
                (
                    sum(m.new_nature) * sum(download_phone) / sum(download)
                ) + sum(m.new_tg_phone) AS new_appsflyer_phone,
                -- appflyer??????
                sum(m.download_phone) AS download_phone,
                -- ?????????
                (
                    sum(m.new_nature) * sum(download_phone) / sum(download)
                ) - sum(m.new_fake_phone) AS new_nature_phone,
                -- appflyer?????????
                sum(
                    m.new_tg_phone + m.new_fake_phone
                ) AS new_nonature_phone,
                -- ????????????
                (
                    sum(
                        m.cost_tg_phone + m.cost_fake_phone
                    )
                ) AS cost_phone,
                -- ?????????
                sum(m.new_tg_phone) AS new_tg_phone,
                -- ???????????????
                (sum(m.cost_tg_phone)) AS cost_tg_phone,
                -- ??????????????????
                sum(m.new_fake_phone) AS new_fake_phone,
                -- ????????????
                (sum(m.cost_fake_phone)) AS cost_fake_phone,
                -- ??????????????????
                (
                    sum(m.cost_optimize_phone)
                ) AS cost_optimize_phone,
                -- aso??????
                -- ipad??????
                (
                    sum(m.new_nature) * sum(download_pad) / sum(download)
                ) + sum(m.new_tg_pad) AS new_appsflyer_pad,
                -- appflyer??????
                sum(m.download_pad) AS download_pad,
                -- ?????????
                (
                    sum(m.new_nature) * sum(download_pad) / sum(download)
                ) - sum(m.new_fake_pad) AS new_nature_pad,
                -- appflyer?????????
                sum(
                    m.new_tg_pad + m.new_fake_pad
                ) AS new_nonature_pad,
                -- ????????????
                (
                    sum(
                        m.cost_tg_pad + m.cost_fake_pad
                    )
                ) AS cost_pad,
                -- ?????????
                sum(m.new_tg_pad) AS new_tg_pad,
                -- ???????????????
                (sum(m.cost_tg_pad)) AS cost_tg_pad,
                -- ??????????????????
                sum(m.new_fake_pad) AS new_fake_pad,
                -- ????????????
                (sum(m.cost_fake_pad)) AS cost_fake_pad,
                -- ??????????????????
                (sum(m.cost_optimize_pad)) AS cost_optimize_pad,
                -- aso??????
                -- round((case when p.ad_divide_tr is null or p.game_id like '%53' then 0 else sum(earning_ad+earning_ff-(m.cost_tg+m.cost_fake+m.cost_optimize))*p.ad_divide_tr/100 end),0)
                0 AS cost_dev,
                -- ???????????????
                NOW(),
                a.company_id
            FROM
                (
                    SELECT
                        t.date,
                        t.app_id,
                        t.country_id,
                        0 AS new,
                        0 AS active,
                        t.earning_fix AS earning_ad,
                        -- ????????????
                        0 AS earning_ff,
                        -- appflyer??????????????????
                        0 AS new_appsflyer,
                        -- appflyer??????
                        0 AS new_nature,
                        -- appflyer?????????
                        0 AS new_tg,
                        -- appflyer?????????
                        0 AS cost_tg,
                        -- ??????????????????
                        0 AS new_fake,
                        -- ????????????
                        0 AS cost_fake,
                        -- ??????????????????
                        0 AS cost_optimize,
                        -- ??????aso??????
                        -- appflyer??????????????????phone
                        0 AS new_tg_phone,
                        -- appflyer?????????phone
                        0 AS cost_tg_phone,
                        -- ??????????????????ponhe
                        0 AS new_fake_phone,
                        -- ????????????ponhe
                        0 AS cost_fake_phone,
                        -- ??????????????????ponhe
                        0 AS cost_optimize_phone,
                        --
                        -- appflyer??????????????????pad
                        0 AS new_tg_pad,
                        -- appflyer?????????pad
                        0 AS cost_tg_pad,
                        -- ??????????????????pad
                        0 AS new_fake_pad,
                        -- ????????????pad
                        0 AS cost_fake_pad,
                        -- ??????????????????pad
                        0 AS cost_optimize_pad,
                        -- ios?????????
                        '0' AS download,
                        '0' AS download_phone,
                        '0' AS download_pad
                    FROM
                        zplay_ad_report_daily t
                    WHERE
                        t.date BETWEEN '$begin_date'
                    AND '$end_date'
                    AND t.statistics = 0 and t.flow_type = 1
                    UNION ALL
                        SELECT
                            t.date,
                            t.app_id,
                            if(t.type = 1,64,t.country_id) as country_id,
                            t.new_user AS new,
                            t.active_user AS active,
                            0 AS earning_ad,
                            -- ????????????
                            0 AS earning_ff,
                            -- appflyer??????????????????
                            0 AS new_appsflyer,
                            -- appflyer??????
                            0 AS new_nature,
                            -- appflyer?????????
                            0 AS new_tg,
                            -- appflyer?????????
                            0 AS cost_tg,
                            -- ??????????????????
                            0 AS new_fake,
                            -- ????????????
                            0 AS cost_fake,
                            -- ??????????????????
                            0 AS cost_optimize,
                            -- ??????aso??????
                            -- appflyer??????????????????phone
                            0 AS new_tg_phone,
                            -- appflyer?????????phone
                            0 AS cost_tg_phone,
                            -- ??????????????????ponhe
                            0 AS new_fake_phone,
                            -- ????????????ponhe
                            0 AS cost_fake_phone,
                            -- ??????????????????ponhe
                            0 AS cost_optimize_phone,
                            --
                            -- appflyer??????????????????pad
                            0 AS new_tg_pad,
                            -- appflyer?????????pad
                            0 AS cost_tg_pad,
                            -- ??????????????????pad
                            0 AS new_fake_pad,
                            -- ????????????pad
                            0 AS cost_fake_pad,
                            -- ??????????????????pad
                            0 AS cost_optimize_pad,
                            -- ios?????????
                            '0' AS download,
                            '0' AS download_phone,
                            '0' AS download_pad
                        FROM
                            zplay_user_tj_report_daily t
                        WHERE
                            t.date BETWEEN '$begin_date'
                        AND '$end_date'
                        -- AND ad_status = 1
                        UNION ALL
                            SELECT
                                t.date,
                                t.app_id,
                                t.country_id,
                                0,
                                0,
                                0,
                                t.income_fix AS earning_ff,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                0,
                                '0' AS download,
                                '0' AS download_phone,
                                '0' AS download_pad
                            FROM
                                zplay_ff_report_daily t
                                
                            WHERE
                                t.date BETWEEN '$begin_date'
                            AND '$end_date'
                            AND t.tongji_type = 0
                            UNION ALL
                                SELECT
                                    a.date,
                                    a.app_id,
                                    a.country_id,
                                    0,
                                    0,
                                    0,
                                    0,
                                    -- ??????
                                    (
                                        CASE
                                        WHEN a.tongji_type = '2' THEN
                                            a.new
                                        ELSE
                                            0
                                        END
                                    ) AS new_appsflyer,
                                    (
                                        CASE
                                        WHEN a.tongji_type = '2'
                                        AND a.platform_id = 'ptg31' THEN
                                            a.new
                                        ELSE
                                            0
                                        END
                                    ) AS new_nature,
                                    (
                                        CASE
                                        WHEN a.tongji_type = '0'
                                        AND (a.type = '1' OR a.type = '2')
                                        AND a.platform_id <> 'ptg31' THEN
                                            a.new
                                        ELSE
                                            0
                                        END
                                    ) AS new_tg,
                                    (
                                        CASE
                                        WHEN a.tongji_type = '0'
                                        AND (a.type = '1' OR a.type = '2') THEN
                                            a.cost_exc
                                        ELSE
                                            0
                                        END
                                    ) AS cost_tg,
                                    (
                                        CASE
                                        WHEN a.tongji_type = '0' AND a.type = '2' THEN
                                            a.new
                                        ELSE
                                            0
                                        END
                                    ) AS new_fake,
                                    (
                                        CASE
                                        WHEN a.tongji_type = '0'
                                        AND a.type = '2' THEN
                                            a.cost_exc
                                        ELSE
                                            0
                                        END
                                    ) AS cost_fake,
                                    (
                                        CASE
                                        WHEN a.type = '3'
                                        OR a.type = '5' THEN
                                            a.cost_exc
                                        ELSE
                                            0
                                        END
                                    ) AS cost_optimize,
                                    -- phone
                                    (
                                        CASE
                                        WHEN a.tongji_type = '0'
                                        AND (a.type = '1' OR a.type = '2') THEN
                                            a.new_phone
                                        ELSE
                                            0
                                        END
                                    ) AS new_tg_phone,
                                    (
                                        CASE 
                                        WHEN a.tongji_type = '0'
                                        AND (a.type = '1' OR a.type = '2')
                                        AND a.new_phone > 0 THEN
                                                a.new_phone * a.cost_exc / a.new
                                        ELSE
                                            0
                                        END
                                    ) AS cost_tg_phone,
                                    (
                                        CASE
                                        WHEN a.tongji_type = '0'
                                        AND a.type = '2' THEN
                                            a.new_phone
                                        ELSE
                                            0
                                        END
                                    ) AS new_fake_phone,
                                    (
                                        CASE
                                        WHEN a.tongji_type = '0'
                                        AND a.type = '2'
                                        AND a.new_phone > 0 THEN
                                                a.new_phone * a.cost_exc / a.new
                                        ELSE
                                            0
                                        END
                                    ) AS cost_fake_phone,
                                    (
                                        CASE
                                        WHEN (a.type = '3' OR a.type = '5')
                                        AND a.device_type = '1' THEN
                                            a.cost_exc
                                        ELSE
                                            0
                                        END
                                    ) AS cost_optimize_phone,
                                    -- pad
                                    (
                                        CASE
                                        WHEN a.tongji_type = '0'
                                        AND (a.type = '1' OR a.type = '2') THEN
                                            a.new_pad
                                        ELSE
                                            0
                                        END
                                    ) AS new_tg_pad,
                                    (
                                        CASE
                                        WHEN a.tongji_type = '0'
                                        AND (a.type = '1' OR a.type = '2')
                                        AND a.new_pad > 0 THEN
                                            a.new_pad * a.cost_exc / a.new
                                        ELSE
                                            0
                                        END
                                    ) AS cost_tg_pad,
                                    (
                                        CASE
                                        WHEN a.tongji_type = '0'
                                        AND a.type = '2' THEN
                                            a.new_pad
                                        ELSE
                                            0
                                        END
                                    ) AS new_fake_pad,
                                    (
                                        CASE
                                        WHEN a.tongji_type = '0'
                                        AND a.type = '2'
                                        AND a.new_pad > 0 THEN
                                            a.new_pad * a.cost_exc / a.new
                                        ELSE
                                            0
                                        END
                                    ) AS cost_fake_pad,
                                    (
                                        CASE
                                        WHEN (a.type = '3' OR a.type = '5')
                                        AND a.device_type = '2' THEN
                                            a.cost_exc
                                        ELSE
                                            0
                                        END
                                    ) AS cost_optimize_pad,
                                    '0' AS download,
                                    '0' AS download_phone,
                                    '0' AS download_pad
                                FROM
                                    zplay_tg_report_daily a
                                WHERE
                                    a.type != 4
                                AND a.platform_id <> 'ptg279'
                                AND a.date BETWEEN '$begin_date'
                                AND '$end_date'
                                UNION ALL
                                    --  ios ?????????iPhone ???ipad??????
                                    SELECT
                                        date,
                                        t.app_id,
                                        t.country_id,
                                        0,
                                        0,
                                        0,
                                        0,
                                        0,
                                        0,
                                        0,
                                        0,
                                        0,
                                        0,
                                        0,
                                        0,
                                        0,
                                        0,
                                        0,
                                        0,
                                        0,
                                        0,
                                        0,
                                        0,
                                        0,
                                        t.pay_user,
                                        (
                                            CASE
                                            WHEN t.device_type = 'iPhone' THEN
                                                t.pay_user
                                            ELSE
                                                '0'
                                            END
                                        ) AS units_iphone,
                                        (
                                            CASE
                                            WHEN t.device_type = 'iPad' THEN
                                                t.pay_user
                                            ELSE
                                                '0'
                                            END
                                        ) AS units_iphone
                                    FROM
                                        zplay_ff_report_daily t
                                    WHERE
                                        t.date BETWEEN '$begin_date'
                                    AND '$end_date'
                                    AND t.times = '1F'
                                    AND platform_id = 'pff03'
                ) m
            LEFT JOIN c_app a ON a.app_id = m.app_id
            where  m.app_id not in (select app_id from  c_app_show where promotion_on = 0)
            GROUP BY
                m.date,
                m.app_id,
                a.os_id,
                m.country_id
            ORDER BY
                m.date,
                m.app_id,
                a.os_id,
                m.country_id ASC

        ";
            $insert_info_1 = DB::insert($insert_sql);

            if (!$insert_info_1) {
                //var_dump(3);
                DB::rollBack();
            }
            $update_sql = "UPDATE zplay_basic_tg_report_total_usd usd,
            c_currency_ex cur 
            SET usd.cost = usd.cost_cny / cur.currency_ex,usd.earning_all = usd.earning_all_cny / cur.currency_ex 
            WHERE
            cur.`effective_time` = date_format( usd.date, '%Y%m' ) 
            AND cur.`currency_id` = 60  
            AND usd.date BETWEEN '$begin_date'
            AND '$end_date'";
            $update_info = DB::update($update_sql);
//            if (!$update_info){
//                DB::rollBack();
//            }

            DB::commit();
            $end_time = time();
            var_dump($bengin_time - $end_time);
        }catch (\Exception $e) {
            // ????????????
            $message = date("Y-m-d")."???,????????????????????????,????????????:".$e->getMessage();
            DataImportImp::saveDataErrorLog(5, 'ptg-003', '????????????', 4, $message);
            $error_msg_arr[] = $message;
//            CommonFunction::sendMail($error_msg_arr, '??????????????????error');
            exit;
        }
    }
}