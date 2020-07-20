<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * 
 */
namespace App\Http\Controllers\DataPlatform;
use App\BusinessImp\CommonImp;
use App\BusinessImp\DataImportImp;
use App\BusinessImp\UserImp;
use App\BusinessImp\PlatformImp;
use App\Http\Controllers\Controller as Controller;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Artisan;

class DataProcessController extends Controller
{

    /**
     * admob 数据处理过程
     * @param $params array 请求数据
     */
    public function admobProcess()
    {
        $dayid = isset($_GET['dayid']) ? $_GET['dayid'] : '';

        if ($dayid){
            Artisan::call('AdmobHandleProcesses',['dayid' => $dayid]);
        }
    }

    /**
     * adwords tg process 数据处理过程
     * @param $params array 请求数据
     */
    public function adwordsTgProcess()
    {
        $dayid = isset($_GET['dayid']) ? $_GET['dayid'] : '';

        if ($dayid){
            $dayid = date('Y-m-d',strtotime($dayid));
            Artisan::call('AdwodsHandleProcesses',['dayid' => $dayid]);
        }
    }


    /**
     * facebook tg process 数据处理过程
     * @param $params array 请求数据
     */
    public function facebookTgProcess()
    {
        $dayid = isset($_GET['dayid']) ? $_GET['dayid'] : '';

        if ($dayid){
            Artisan::call('FacebookTgHandleProcesses',['dayid' => $dayid]);
        }
    }

    /**
     * mintegral tg process 数据处理过程
     * @param $params array 请求数据
     */
    public function mintegralTgProcess()
    {
        $dayid = isset($_GET['dayid']) ? $_GET['dayid'] : '';

        if ($dayid){
            Artisan::call('MintegralTgHandleProcesses',['dayid' => $dayid]);
        }
    }

    /**
     * baidu tg process 数据处理过程
     * @param $params array 请求数据
     */
    public function baiduTgProcess()
    {
        $dayid = isset($_GET['dayid']) ? $_GET['dayid'] : '';

        if ($dayid){
            Artisan::call('BaiduTgHandleProcesses',['dayid' => $dayid]);
        }
    }

    /**
     * baidu ad process 数据处理过程
     * @param $params array 请求数据
     */
    public function baiduAdProcess()
    {
        $dayid = isset($_GET['dayid']) ? $_GET['dayid'] : '';
        if ($dayid){
            Artisan::call('BaiduHandleProcesses',['dayid' => $dayid]);
        }
    }

    /**
     * 添加平台取数状态
     * @param $params array 请求数据
     */
    public function platformAddStatus()
    {
         $platform_id = isset($_GET['platform_id']) ? $_GET['platform_id'] : '';
         $account = isset($_GET['account']) ? $_GET['account'] : '';
         $account_total = isset($_GET['account_total']) ? $_GET['account_total'] : '';
         $dayid = isset($_GET['dayid']) ? $_GET['dayid'] : '';
         if(!$platform_id && !$account  && !$account_total && !$dayid){
            PlatformImp::add_platform_status($platform_id,$account,$account_total,$dayid);
         }
    }

        /**
     * baidu tg process 数据处理过程
     * @param $params array 请求数据
     */
    public function UnityAdProcess()
    {
        $dayid = isset($_GET['dayid']) ? $_GET['dayid'] : '';

        if ($dayid){
            Artisan::call('UnityHandleProcesses',['dayid' => $dayid]);
        }
    }
        /**
     * baidu tg process 数据处理过程
     * @param $params array 请求数据
     */
    public function snapchatTgProcess()
    {
        $dayid = isset($_GET['dayid']) ? $_GET['dayid'] : '';

        if ($dayid){
            Artisan::call('SnapchatTgHandleProcesses',['dayid' => $dayid]);
        }
    }

    /**
     * vungle tg process 数据处理过程
     * @param $params array 请求数据
     */
    public function vungleAdProcess()
    {
        $dayid = isset($_GET['dayid']) ? $_GET['dayid'] : '';

        if ($dayid){
            Artisan::call('VungleHandleProcesses',['dayid' => $dayid]);
        }
    }

    /**
     * GoolePlay ff process 数据处理过程
     * @param $params array 请求数据
     */
    public function GoolePlayFfProcess()
    {
        $dayid = isset($_GET['dayid']) ? $_GET['dayid'] : '';
        if ($dayid){
            Artisan::call('GoolePlayHandleProcesses',['dayid' => $dayid]);
        }
    }

    /**
     * AppStore ff process 数据处理过程
     * @param $params array 请求数据
     */
    public function AppStoreFfProcess()
    {
        $dayid = isset($_GET['dayid']) ? $_GET['dayid'] : '';
        if ($dayid){
            Artisan::call('AppStoreHandleProcesses',['dayid' => $dayid]);
        }
    }

    /**
     * tiktok 保存数据
     * @param $params array 请求数据
     */
    public function tiktokTgData()
    {
        $params = $this->params;
        $data = isset($params['data']) ? $params['data'] : '';
        $dayid = isset($params['dayid']) ? $params['dayid'] : '';
        $data_account = isset($params['data_account']) ? $params['data_account'] : '';

        if ($dayid && $data && $data_account){
            Artisan::call('TiktokTgReportCommond',['data' => $data, 'dayid' => $dayid, 'data_account' => $data_account]);
        }
    }

    /**
     * todo tiktok 处理过程
     * @param $params array 请求数据
     */
    public function tiktokTgProcess()
    {
        $dayid = isset($_GET['dayid']) ? $_GET['dayid'] : '';

        if ($dayid){
            Artisan::call('VungleHandleProcesses',['dayid' => $dayid]);
        }
    }

    /**
     * toutiao 保存数据
     * @param $params array 请求数据
     */
    public function toutiaoTgData()
    {
        $params = $this->params;
        $data = isset($params['data']) ? $params['data'] : '';
        $dayid = isset($params['dayid']) ? $params['dayid'] : '';
        $data_account = isset($params['data_account']) ? $params['data_account'] : '';

        if ($dayid && $data && $data_account){
            Artisan::call('ToutiaoTgReportCommond',['data' => $data, 'dayid' => $dayid, 'data_account' => $data_account]);
        }
    }

    /**
     * todo toutiao 处理过程
     * @param $params array 请求数据
     */
    public function toutiaoTgProcess()
    {
        $dayid = isset($_GET['dayid']) ? $_GET['dayid'] : '';

        if ($dayid){
            Artisan::call('JinritoutiaoTgHandleProcesses',['dayid' => $dayid]);
        }
    }

    /**
     * tuia ad 保存数据
     * @param $params array 请求数据
     */
    public function tuiaAdProcess()
    {
        $params = $this->params;
        $start_date = isset($params['start_date']) ? $params['start_date'] : '';
        $end_date = isset($params['end_date']) ? $params['end_date'] : '';

        if ($start_date && $end_date){
            Artisan::call('TuiaAdHandleProcesses',['start_date' => $start_date, 'end_date' => $end_date]);
        }
    }

     /**
     * 渠道手工处理过程 ad 保存数据
     * @param $params array 请求数据
     */
    public function channelProcess()
    {
        $params = $this->params;
        $platform_id = isset($params['platform_id']) ? $params['platform_id'] : '';
        $dayid = isset($params['dayid']) ? $params['dayid'] : '';
        if ($dayid && $platform_id){
            Artisan::call($platform_id,['dayid' => $dayid]);
        }
    }

    /**
     * 渠道手工处理过程 ad 保存数据
     * @param $params array 请求数据
     */
    public function flurryTjProcess()
    {
        $params = $this->params;
        $stime = isset($params['stime']) ? $params['stime'] : '';
        $etime = isset($params['etime']) ? $params['etime'] : '';
        if ($stime && $etime){
            Artisan::call("FlurryKeepTjHandleProcesses",['stime' => $stime,"etime" => $etime]);
        }
    }


    /**
     * 渠道手工处理过程 ad 保存数据
     * @param $params array 请求数据
     */
    public function tdForeignProcess()
    {
        $params = $this->params;
        $day = isset($params['dayid']) ? $params['dayid'] : '';
        if ($day){
            Artisan::call("TdForeignUserTjHandleProcesses",['dayid'=>$day]);
        }
    }

    /**
     * 渠道手工处理过程 ad 保存数据
     * @param $params array 请求数据
     */
    public function tdChinaProcess()
    {
        $params = $this->params;
        $day = isset($params['dayid']) ? $params['dayid'] : '';
        if ($day){
            Artisan::call("TdUserTjHandleProcesses",['dayid'=>$day]);
        }
    }

    /**
     * 渠道手工处理过程 ad 保存数据
     * @param $params array 请求数据
     */
    public function tdKeepUserProcess()
    {
        $params = $this->params;
        $day = isset($params['dayid']) ? $params['dayid'] : '';
        if ($day){
            Artisan::call("TdKeepTjHandleProcesses",['dayid'=>$day]);
        }
    }

    /**
     * 渠道手工处理过程 ad 保存数据
     * @param $params array 请求数据
     */
    public function tdMonthUserProcess()
    {
        $params = $this->params;
        $day = isset($params['dayid']) ? $params['dayid'] : '';
        if ($day){
            Artisan::call("TdUserTjMonthHandleProcesses",['dayid'=>$day]);
        }
    }

    /**
     * applovinAdProcess ad 保存数据
     * @param $params array 请求数据
     */
    public function applovinAdProcess()
    {
        $params = $this->params;
        $day = isset($params['dayid']) ? $params['dayid'] : '';
        if ($day){
            Artisan::call("ApplovinHandleProcesses",['dayid'=>$day]);
        }
    }

    /**
     * 金立 ff 保存数据
     * @param $params array 请求数据
     */
    public function jinliFfProcess()
    {
        $params = $this->params;
        $day = isset($params['dayid']) ? $params['dayid'] : '';
        if ($day){
            Artisan::call("JinliHandleProcesses",['dayid'=>$day]);
        }
    }

    /**
     * 金立 ff 保存数据
     * @param $params array 请求数据
     */
    public function appsflyerPullProcess()
    {
        $params = $this->params;
        $day = isset($params['dayid']) ? $params['dayid'] : '';
        $hours = isset($params['hours']) ? $params['hours'] : '';
        if ($day && $hours){
            Artisan::call("AfAnalysisIdfaCommond",['dayid'=>$day,'hours' => $hours]);
        }
    }
    /**
     * ironSourceAdProcess ad 保存数据
     * @param $params array 请求数据
     */
    public function ironSourceAdProcess()
    {
        $params = $this->params;
        $day = isset($params['dayid']) ? $params['dayid'] : '';
        if ($day){
            Artisan::call("IronSourceReportCommond",['dayid'=>$day]);
        }
    }

    
}