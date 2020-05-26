<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: ����4:18
 */
namespace App\Http\Controllers\DataPlatform;

use App\BusinessImp\CommonImp;
use App\BusinessImp\DeveloperImp;
use App\BusinessImp\PlatformImp;
use App\Http\Controllers\Controller as Controller;
use Illuminate\Http\Request;

class DeveloperController extends Controller
{
    /**
     * �������б�
     * @param $params array ��������
     */
    public function getDeveloperList(){
        DeveloperImp::getDeveloperList($this->params);
    }


    /**
     * �༭��ӿ�����
     * @param $params array ��������
     */
    public function createDeveloper(){
        DeveloperImp::createDeveloper($this->params);
    }


}