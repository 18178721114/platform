<?php

namespace App\Console;

use App\Console\Commands\AppsflyerDeviceData\AppsflyerAppID;
use App\Console\Commands\ManuallyCheckDataProcess\HandWorkExcelDataProcesses;
use Illuminate\Console\Scheduling\Schedule;
use Illuminate\Foundation\Console\Kernel as ConsoleKernel;

class Kernel extends ConsoleKernel
{
    /**
     * The Artisan commands provided by your application.
     *
     * @var array
     */
    protected $commands = [
        Commands\ZplayAdsReportCommond::class,
        Commands\SendReportMailCommond::class,
        Commands\AdDataProcesses\VungleCommond::class,
        Commands\AdDataProcesses\SmaatoCommond::class,
        Commands\TgDataProcesses\GdtTgReportCommond::class,
        Commands\AdDataProcesses\IronSourceReportCommond::class,
        Commands\AdDataProcesses\UnityReportCommond::class,
        Commands\AdDataProcesses\HeyzapReportCommond::class,
        Commands\AdDataProcesses\ApplovinReportCommond::class,
        Commands\AdDataProcesses\InmobiReportCommond::class,
        Commands\AdDataProcesses\GuangdiantongReportCommond::class,
        Commands\AdDataProcesses\ChartboostReportCommond::class,
        Commands\AdDataProcesses\FacebookAdBiddingCommond::class,
        Commands\AdDataProcesses\MobvistaReportCommond::class,
        Commands\AdDataProcesses\OnewayReportCommond::class,
        Commands\AdDataProcesses\KedaxunfeiReportCommond::class,
        Commands\AdDataProcesses\ChangsiReportCommond::class,
        Commands\AdDataProcesses\TapjoyReportCommond::class,
        Commands\AdDataProcesses\DiankaiReportCommond::class,
        Commands\TgDataProcesses\ApplovinTgReportCommond::class,
        Commands\TgDataProcesses\UnityTgReportCommond::class,
        Commands\TgDataProcesses\ChartboostTgReportCommond::class,
        Commands\TgDataProcesses\VungleTgReportCommond::class,
        Commands\TjDataProcesses\AppflurryTjReportCommond::class,
        Commands\AdDataProcesses\KewanReportCommond::class,
        Commands\TgDataProcesses\KewanTgReportCommond::class,
        Commands\TgDataProcesses\SnapchatTgCommond::class,
        Commands\TgDataProcesses\ToutiaoTgReportCommond::class,
        Commands\TgDataProcesses\AdcolonyTgReportCommond::class,
        Commands\TgDataProcesses\TiktokTgReportCommond::class,
//        Commands\TgDataProcesses\KuaishouTgReportCommond::class,
        Commands\AdHandleProcesses\IronsourceHandleProcesses::class,
        Commands\TjDataProcesses\TalkdataChinaUserCommand::class,
        Commands\TjDataProcesses\TalkdataChinaKeepUserCommond::class,
        Commands\TjDataProcesses\TalkdataForeignUserRetentionTjCommond::class,
        Commands\TjDataProcesses\TalkdataForeignUserSessionTjCommond::class,
        Commands\TjDataProcesses\TalkdataForeignUserTjReportCommond::class,
        Commands\TjDataProcesses\TalkdataForeignTotalTjReportCommond::class,
        Commands\TjDataProcesses\TalkdataForeignTotalSessionTjCommond::class,
        Commands\TjDataProcesses\TalkdataChinaSessionCommand::class,
        Commands\TjDataProcesses\TalkdataChinaSessionCommand::class,
        Commands\CrontabMysqlHome\HomeCommond::class,
        Commands\CrontabMysqlHome\HomeUsdCommond::class,
        Commands\TgDataProcesses\AppsflyerTgReportCommond::class,
        Commands\TjDataProcesses\TalkdataChinaUserNewCommand::class,
        Commands\TjDataProcesses\TalkdataChinaSessionNewCommand::class,
        Commands\TjDataProcesses\TalkdataForeignUserNewCommond::class,
        Commands\TjDataProcesses\TalkdataForeignSessionNewCommond::class,
        Commands\TgHandleProcesses\BreakHandleProcesses::class,
        Commands\AdDataProcesses\FyberReportCommond::class,
        Commands\AdDataProcesses\YumiPolymerizationCommond::class,
        Commands\DeveloperPushOldprocesses\DeveloperDayProcesses::class,
        Commands\ShellScript\RedisAdProcesses::class,
        Commands\ManuallyCheckDataProcess\HandWorkExcelDataProcesses::class,
        Commands\AppsflyerHandleProcesses\AfAnalysisIdfaCommond::class,
        Commands\AppsflyerDeviceData\AppsflyerTgDetailsReportCommond::class,
        Commands\AppsflyerDeviceData\AppsflyerAppID::class,
        Commands\DivideHandleProcesses\ChannelPushShowHandleProcesses::class,


    ];

    /**
     * Define the application's command schedule.
     *
     * @param  \Illuminate\Console\Scheduling\Schedule  $schedule
     * @return void
     */
    protected function schedule(Schedule $schedule)
    {
        // $schedule->command('inspire')
        //          ->hourly();
    }

    /**
     * Register the commands for the application.
     *
     * @return void
     */
    protected function commands()
    {
        $this->load(__DIR__.'/Commands');

        require base_path('routes/console.php');
    }
}
