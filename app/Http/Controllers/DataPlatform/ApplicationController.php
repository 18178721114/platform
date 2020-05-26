<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: ����4:18
 */
namespace App\Http\Controllers\DataPlatform;

use App\BusinessImp\ApplicationImp;
use App\Http\Controllers\Controller as Controller;
use Illuminate\Http\Request;

class ApplicationController extends Controller
{
    /**
     * Ӧ���б�
     */
    public function getAppList(){
        ApplicationImp::getAppList($this->params);
    }


    /**
     * �༭���Ӧ��
     */
    public function createApp(){
        ApplicationImp::addApp($this->params);
    }

    /**
     * ͳ����Ϣ�����б�
     * @param $params array ��������
     */
    public function appStatisticList()
    {
        ApplicationImp::appStatisticList($this->params);
    }

    /**
     * ͳ����Ϣ�������� �޸�
     * @param $params array ��������
     */
    public function createAppStatistic()
    {
        ApplicationImp::createAppStatistic($this->params);
    }

    /**
     * ͳ����Ϣ����ɾ��
     * @param $params array ��������
     */
    public function changeAppStatisticStatus()
    {
        ApplicationImp::changeAppStatisticStatus($this->params);
    }

    /**
     * �����Ϣ�����б�
     */
    public function appAdList()
    {
        ApplicationImp::appAdList($this->params);
    }

    /**
     * �����Ϣ�����˺�����
     */
    public function appAdAccountList()
    {
        ApplicationImp::appAdAccountList($this->params);
    }

    /**
     * �����Ϣ�������� �޸�
     */
    public function createAppAd()
    {
        ApplicationImp::createAppAd($this->params);
    }

    /**
     * �����Ϣ����ɾ��
     */
    public function changeAppAdStatus()
    {
        ApplicationImp::changeAppAdStatus($this->params);
    }

    /**
     * �Ʒ���Ϣ�����б�
     */
    public function appBillingList()
    {
        ApplicationImp::appBillingList($this->params);
    }

    /**
     * �Ʒ���Ϣ�������� �޸�
     */
    public function createAppBilling()
    {
        ApplicationImp::createAppBilling($this->params);
    }

    /**
     * �Ʒ���Ϣ����ɾ��
     */
    public function changeAppBillingStatus()
    {
        ApplicationImp::changeAppBillingStatus($this->params);
    }

        /**
     * �Ʒ���Ϣ����ɾ��
     */
    public function changeAppBillingRebate()
    {
        ApplicationImp::changeAppBillingRebate($this->params);
    }

    /**
     * �ƹ���Ϣ�����б�
     */
    public function appGeneralizeList()
    {
        ApplicationImp::appGeneralizeList($this->params);
    }

    /**
     * �ƹ���Ϣ���ô���ƽ̨����
     */
    public function appGeneralizeAgencyList()
    {
        ApplicationImp::appGeneralizeAgencyList($this->params);
    }

    /**
     * �ƹ���Ϣ�������� �޸�
     */
    public function createAppGeneralize()
    {
        ApplicationImp::createAppGeneralize($this->params);
    }

    /**
     * �ƹ���Ϣ����ɾ��
     */
    public function changeAppGeneralizeStatus()
    {
        ApplicationImp::changeAppGeneralizeStatus($this->params);
    }

    /**
     * ƽ̨��̬���������ӿ�
     */
    public function getPlatformConfiglist()
    {
        ApplicationImp::getPlatformConfiglist($this->params);
    }


}