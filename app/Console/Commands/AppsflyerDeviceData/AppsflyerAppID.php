<?php

namespace App\Console\Commands\AppsflyerDeviceData;

use App\BusinessImp\DataImportImp;
use App\BusinessLogic\AdReportLogic;
use App\Common\CurlRequest;
use App\Models\AdReportData;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\DataImportLogic;
use Illuminate\Support\Facades\Artisan;
use App\Common\CommonFunction;
use App\Common\Service;

class AppsflyerAppID extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'AppsflyerAppID';

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

//        $gameDataSql = "SELECT  DISTINCT a.application_id as appid  from (select appid as application_id from appsflyer_conf UNION all SELECT application_id from c_generalize  WHERE platform_id = 'ptg02') as a";
        $gameDataSql = "SELECT distinct application_id as appid from c_generalize  WHERE platform_id = 'ptg02'";
        $app_list = DB::select($gameDataSql);
        $app_list = Service::data($app_list);
        if ($app_list){
            file_put_contents('./appsflyer_appid.txt', '');
            foreach($app_list as $row) {
                file_put_contents('./appsflyer_appid.txt', $row['appid']."\n",FILE_APPEND);
            }
        }
    }


}
