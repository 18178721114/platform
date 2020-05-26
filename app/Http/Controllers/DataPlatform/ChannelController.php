<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: ����4:18
 */
namespace App\Http\Controllers\DataPlatform;

use App\BusinessImp\ChannelImp;
use App\Http\Controllers\Controller as Controller;
use Illuminate\Http\Request;

class ChannelController extends Controller
{
    /**
     * �����б�
     * @param $params array ��������
     */
    public function getChannelList(){
        ChannelImp::getChannelList($this->params);
    }

    /**
     * �༭�������
     * @param $params array ��������
     */
    public function createChannel(){
        ChannelImp::createChannel($this->params);
    }


}