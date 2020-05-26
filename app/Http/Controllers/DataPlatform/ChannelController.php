<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: 下午4:18
 */
namespace App\Http\Controllers\DataPlatform;

use App\BusinessImp\ChannelImp;
use App\Http\Controllers\Controller as Controller;
use Illuminate\Http\Request;

class ChannelController extends Controller
{
    /**
     * 渠道列表
     * @param $params array 请求数据
     */
    public function getChannelList(){
        ChannelImp::getChannelList($this->params);
    }

    /**
     * 编辑添加渠道
     * @param $params array 请求数据
     */
    public function createChannel(){
        ChannelImp::createChannel($this->params);
    }


}