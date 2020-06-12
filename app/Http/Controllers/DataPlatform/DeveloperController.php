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

    /**
     * ������ƽ̨Ӧ�ÿ����б�ӿ�
     * @param $params array ��������
     */
    public function developAppList(){
        DeveloperImp::developAppList($this->params);
    }

    /**
     * ������ƽ̨Ӧ�ÿ���״̬�޸Ľӿ�
     * @param $params array ��������
     */
    public function developAppStatus(){
        DeveloperImp::developAppStatus($this->params);
    }

    /**
     * ������ƽ̨�û�ע��ӿ�
     * @param $params array ��������
     */
    public function developUserRegister(){
        DeveloperImp::developUserRegister($this->params);
    }


}