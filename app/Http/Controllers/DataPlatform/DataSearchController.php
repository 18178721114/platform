<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * 
 */
namespace App\Http\Controllers\DataPlatform;
use App\BusinessImp\CommonImp;
use App\BusinessImp\DataAppTotalImp;
use App\BusinessImp\DataImportImp;
use App\BusinessImp\DataSearchImp;
use App\BusinessImp\SysInItImp;
use App\BusinessImp\UserImp;
use App\Http\Controllers\Controller as Controller;
use Illuminate\Http\Request;
class DataSearchController extends Controller
{

    /**
     * 数据综合查询初始化
     * @param $params array 请求数据
     */
    public function dataIntegratedQuery()
    {
        DataSearchImp::dataIntegratedQuery($this->params);
    }

    /**
     * 数据邮件信息列表
     * @param $params array 请求数据
     */
    public function dataMailList()
    {
        DataSearchImp::dataMailList($this->params);
    }

    /**
     * 数据邮件信息保存
     * @param $params array 请求数据
     */
    public function dataMailSave()
    {
        DataSearchImp::dataMailSave($this->params);
    }

    /**
     * 数据邮件信息保存
     * @param $params array 请求数据
     */
    public function dataCustomQuery()
    {
        DataSearchImp::dataCustomQuery($this->params);
    }

    /**
     * 数据邮件信息保存
     * @param $params array 请求数据
     */
    public function getSearchData()
    {
        DataSearchImp::getSearchData($this->params);
    }

    /**
     * 定制条件保存
     * @param $params array 请求数据
     */
    public function dataCustomSave()
    {
        DataSearchImp::dataCustomSave($this->params);
    }

    /**
     * 首页数据初始化
     * @param $params array 请求数据
     */
    public function dataIndexInit()
    {
        DataSearchImp::dataIndexInit($this->params);
    }

    /**
     * 首页应用列表
     * @param $params array 请求数据
     */
    public function dataAppList()
    {
        DataSearchImp::dataAppList($this->params);
    }

    /**
     * 首页应用列表
     * @param $params array 请求数据
     */
    public function dataGeneralList()
    {
        DataSearchImp::dataGeneralList($this->params);
    }

    /**
     * 趋势分析
     * @param $params array 请求数据
     */
    public function dataAppLine()
    {
        DataSearchImp::dataAppLine($this->params);
    }
    /**
     * 地域分析
     * @param $params array 请求数据
     */
    public function dataAppCountry()
    {
        DataSearchImp::dataAppCountry($this->params);
    }
    /**
     * 构成分析
     * @param $params array 请求数据
     */
    public function dataAppForm()
    {
        DataSearchImp::dataAppForm($this->params);
    }

    /**
     * 定制指标数据
     * @param $params array 请求数据
     */
    public function dataCustomTarget()
    {
        DataSearchImp::dataCustomTarget($this->params);
    }

    /**
     * 推广数据初始化
     * @param $params array 请求数据
     */
    public function dataPromotionInit()
    {
        DataSearchImp::dataPromotionInit($this->params);
    }

    /**
     * 推广数据初始化 总体数据
     * @param $params array 请求数据
     */
    public function dataPromotionPopulation()
    {
//        DataSearchImp::dataPromotionPopulation($this->params);
        DataSearchImp::dataPromotionPopulationPageSize($this->params);
    }

    /**
     * 推广数据初始化 总体数据
     * @param $params array 请求数据
     */
    public function dataPromotionPopulationDetails()
    {
        DataSearchImp::dataPromotionPopulationDetails($this->params);
    }

    /**
     * 推广数据初始化 总体数据 分平台 arpu
     * @param $params array 请求数据
     */
    public function dataPromotionArpuDetails()
    {
        DataSearchImp::dataPromotionArpuDetails($this->params);
    }


    /**
     * 推广数据初始化 设备
     * @param $params array 请求数据
     */
    public function dataPromotionDevice()
    {
        DataSearchImp::dataPromotionDevice($this->params);
    }

    /**
     * 推广数据初始化 平台
     * @param $params array 请求数据
     */
    public function dataPromotionPlatform()
    {
        DataSearchImp::dataPromotionPlatform($this->params);
    }

    /**
     * 推广数据初始化 国家
     * @param $params array 请求数据
     */
    public function dataPromotionCountry()
    {
        DataSearchImp::dataPromotionCountry($this->params);
    }


    /**
     * 总体情况增长量图
     * @param $params array 请求数据
     */
    public function dataPromotionGrowth()
    {
        DataSearchImp::dataPromotionGrowth($this->params);
    }

    /**
     * 总体情况成本收入图
     * @param $params array 请求数据
     */
    public function dataPromotionIncome()
    {
        DataSearchImp::dataPromotionIncome($this->params);
    }

    /**
     * 总体情况CPI
     * @param $params array 请求数据
     */
    public function dataPromotionCpi()
    {
        DataSearchImp::dataPromotionCpi($this->params);
    }

    /**
     * 总体情况ARPU
     * @param $params array 请求数据
     */
    public function dataPromotionArpu()
    {
        DataSearchImp::dataPromotionArpu($this->params);
    }

    /**
     * 总体情况 新增活跃 柱状图
     * @param $params array 请求数据
     */
    public function dataPromotionUser()
    {
        DataSearchImp::dataPromotionUser($this->params);
    }

    /**
     * 国家或地区排名
     * @param $params array 请求数据
     */
    public function dataPromotionRranking()
    {
        DataSearchImp::dataPromotionRranking($this->params);
    }

    /**
     * 三方平台及数据平台初始化
     * @param $params array 请求数据
     */
    public function getCheckInit()
    {
        DataSearchImp::getCheckInit($this->params);
    }

    /**
     * 三方平台及数据平台页面
     * @param $params array 请求数据
     */
    public function getCheckData()
    {
        DataSearchImp::getCheckData($this->params);
    }

    /**
     * 三方平台及数据平台页面 数据更新
     * @param $params array 请求数据
     */
    public function updateCheckData()
    {
        DataSearchImp::updateCheckData($this->params);
    }

    /**
     * 开发者分成数据
     * @param $params array 请求数据
     */
    public function getDeveloperData()
    {
        DataSearchImp::getDeveloperData($this->params);
    }

    /**
     * 开发者分成数据  折线图
     * @param $params array 请求数据
     */
    public function getDeveloperLine()
    {
        DataSearchImp::getDeveloperLine($this->params);
    }

    /**
     * 累计页面 左侧列表接口
     * @param $params array 请求数据
     */
    public function getAppTotalList()
    {
        DataAppTotalImp::getAppTotalList($this->params);
    }

    /**
     * 累计页面 右侧列表接口
     * @param $params array 请求数据
     */
    public function getAppTotalData()
    {
        DataAppTotalImp::getAppTotalData($this->params);
    }


}