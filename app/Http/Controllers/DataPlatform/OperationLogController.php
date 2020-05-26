<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: 下午4:18
 */
namespace App\Http\Controllers\DataPlatform;

use App\BusinessImp\CommonImp;
use App\BusinessImp\OperationLogImp;
use App\BusinessImp\PlatformImp;
use App\Http\Controllers\Controller as Controller;
use Illuminate\Http\Request;

class OperationLogController extends Controller
{
    /**
     * 操作日志列表
     * @param $params array 请求数据
     */
    public function getOperationLogList(){
        OperationLogImp::getOperationLogList($this->params);
    }

}