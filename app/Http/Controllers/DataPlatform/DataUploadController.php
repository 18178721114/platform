<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: 下午4:18
 */
namespace App\Http\Controllers\DataPlatform;

use App\BusinessImp\CommonImp;
use App\BusinessImp\DataUploadImp;
use App\BusinessImp\PlatformImp;
use App\Http\Controllers\Controller as Controller;
use Illuminate\Http\Request;

class DataUploadController extends Controller
{
    /**
     * 数据核对报表导入
     * @param $params array 请求数据
     */
    public function postOriginalDataUpload(Request $request){
        DataUploadImp::postOriginalDataUpload($request);
    }

    /**
     * 三方平台报表数据导入
     * @param $params array 请求数据
     */
    public function postPlatformDataUpload(Request $request){
        DataUploadImp::postPlatformDataUpload($request);
    }





}
