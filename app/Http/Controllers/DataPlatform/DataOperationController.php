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
use App\BusinessImp\DataOperationImp;
use App\BusinessImp\DataSearchImp;
use App\BusinessImp\SysInItImp;
use App\BusinessImp\UserImp;
use App\Http\Controllers\Controller as Controller;
use Illuminate\Http\Request;
class DataOperationController extends Controller
{

    /**
     * 商业化运营数据报表 变现数据
     * @param $params array 请求数据
     */
    public function getRealizationData()
    {
        DataOperationImp::getRealizationData($this->params);
    }

    /**
     * 游戏发行运营数据报表 发行数据
     * @param $params array 请求数据
     */
    public function getPublishData()
    {
        DataOperationImp::getPublishData($this->params);
    }



}