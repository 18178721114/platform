<?php

namespace App\BusinessImp;

use App\BusinessLogic\ApplicationLogic;
use App\BusinessLogic\CommonLogic;
use App\BusinessLogic\DataImportLogic;
use App\BusinessLogic\DataSearchLogic;
use App\BusinessLogic\OperationLogLogic;
use App\Common\ApiResponseFactory;
use App\Common\CommonFunction;
use App\Common\Service;
use App\Common\CurlRequest;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\UserLogic;
use Illuminate\Support\Facades\DB;
use App\BusinessLogic\RoleLogic;

class DataSearchImp extends ApiBaseImp
{

    /**
     * get data init
     * @route({"GET", "/*"})
     * @param({"guid","$.path[1]"})
     */
    public static function dataIntegratedQuery($params)
    {
        $userid = $_SESSION['erm_data']['guid'];
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }

        // todo 待确认
        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 58;
        // 判断公司
        $Cwhere = '';
        if($company){
            $Cwhere = " and 1=1 ". DataSearchLogic::getCompanyPowerSql($company,'s_search_custom');
        }
        // 查询当前用户的定制查询条件信息
        $sql = "select search_name,id from s_search_custom where user_id = {$userid} {$Cwhere}";
        $searchC = DB::select($sql);
        $searchC = Service::data($searchC);
        $customBuilt = [];
//        $customBuilt['select_id'] = null;
        if(!empty($searchC)){
            foreach ($searchC as $search){
                $customBuilt[] = array('id'=>$search['id'],'value'=>$search['search_name']);
            }
        }else{
            $customBuilt = [];
        }
        $data['custom_list'] = $customBuilt;

        $basic = DB::table('s_cfg_select_dim')->select(['dim_id','dim_name','dim_type','dim_table','dim_order','dim_value','dim_table_id','dim_cfg','dim_class','base_sel_key','base_sel_name'])->where(['currency_type'=>$currency_type_id])->orderBy('dim_order')->get();
        $basic = Service::data($basic);
        $basicSelect = array();
        $dataSelect = array();
        $popDimension = array();
        $basicSelectId = '';
        $default_selectid_info = DataSearchLogic::get_default_select(0,$currency_type_id);//基础维度
        $default_selectid_info = Service::data($default_selectid_info);
        if(!empty($default_selectid_info)){
            foreach ($default_selectid_info as $basic_select){
                $basicSelectId .=$basic_select['dim_id'].",";
            }
        }
        $basicSelectId = rtrim($basicSelectId,',');
        $basicSelect['select_id'] = $basicSelectId ? $basicSelectId:null;

        $dataSelectId = '';
        $default_selectid_info = DataSearchLogic::get_default_select(1,$currency_type_id);// 数据维度
        $default_selectid_info = Service::data($default_selectid_info);
        if(!empty($default_selectid_info)){
            foreach ($default_selectid_info as $basic_select){
                $dataSelectId .=$basic_select['dim_id'].",";
            }
        }
        $dataSelectId = rtrim($dataSelectId,',');
        $dataSelect['target_id'] = $dataSelectId ? $dataSelectId:null;
        $popDimension['app_id'] = 1;
        //用户权限
        $map1['id'] = $userid;
        //验证用户是否有权限登录
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        //返回用户下用权限列表
        $app_permission = [];
        if($userInfo[0]['app_permission'] != -2){
            $app_permission = explode(',', $userInfo[0]['app_permission']);
        }


        $powerList = $app_permission;
        foreach ($basic as $info){
            if($info['dim_type']==1)
                $dataSelect['data']['sub'][$info['dim_class']][] = array('id'=>$info['dim_id'],'value'=>$info['dim_name']);
            else {
                $basicSelect['list'][] = array('id'=>$info['dim_id'],'value'=>$info['dim_name']);

                if($info['dim_table']=='c_country_standard'){

                    if ($info['dim_name'] == '国家'){

                        $popData = DB::table($info['dim_table'])->select($info['base_sel_name'],$info['base_sel_key'])->where('type',2)->get();
                        $popData = Service::data($popData);

                    }elseif($info['dim_name'] == '省份'){

                        $popData = DB::table($info['dim_table'])->select($info['base_sel_name'],$info['base_sel_key'])->where('type',3)->get();
                        $popData = Service::data($popData);
                    }
                }elseif($info['dim_name'] == '平台' && ($info['dim_table']=='c_platform')){

                    $popData = DB::table($info['dim_table'])->select([$info['base_sel_name'],$info['base_sel_key'],'platform_type_id'])->where(['status' => 1])->groupBy([$info['base_sel_name'],$info['base_sel_key']])->orderBy('platform_type_id')->get();
                    $popData = Service::data($popData);
                    foreach ($popData as $key => $value){
                        if ($value['platform_type_id'] == 1){
                            $value[$info['base_sel_name']] = '统计-'.$value[$info['base_sel_name']].'-'.$value[$info['base_sel_key']];
                            unset($value['platform_type_id']);
                            $popData[$key] = $value;
                        }elseif ($value['platform_type_id'] == 2){
                            $value[$info['base_sel_name']] = '广告-'.$value[$info['base_sel_name']].'-'.$value[$info['base_sel_key']];
                            unset($value['platform_type_id']);
                            $popData[$key] = $value;
                        }elseif ($value['platform_type_id'] == 3){
                            $value[$info['base_sel_name']] = '计费-'.$value[$info['base_sel_name']].'-'.$value[$info['base_sel_key']];
                            unset($value['platform_type_id']);
                            $popData[$key] = $value;
                        }elseif ($value['platform_type_id'] == 4){
                            $value[$info['base_sel_name']] = '推广-'.$value[$info['base_sel_name']].'-'.$value[$info['base_sel_key']];
                            unset($value['platform_type_id']);
                            $popData[$key] = $value;
                        }elseif ($value['platform_type_id'] == 5){
                            unset($popData[$key]);
                        }
                    }
                }elseif($info['dim_name'] == '应用' && ($info['dim_table']=='c_app')){

                    $popData = DB::table($info['dim_table'])->select([$info['base_sel_name'],$info['base_sel_key'],'app_id','release_region_id','os_id']);
                    if ($app_permission){
                        $popData->whereIn($info['base_sel_key'],$app_permission);
                    }
                    if($company == 9){
                        $popData->where('company_id',$company);
                    }elseif($company == 1 ){
                        $popData->whereNotIn('company_id',[9]);
                    }
                    $popData = $popData->groupBy([$info['base_sel_name'],$info['base_sel_key']])->where('status',1)->orderBy('app_full_name')->get();
                    $popData = Service::data($popData);

                    foreach ($popData as $app_k => $app_v){

                        // 发行区域ID(1,全球;2,国外;3,国内;)
                        if ($app_v['release_region_id'] == 1){
                            $release_region_id = '全球-';
                        }elseif ($app_v['release_region_id'] == 2){
                            $release_region_id = '国外-';
                        }elseif ($app_v['release_region_id'] == 3){
                            $release_region_id = '国内-';
                        }else{
                            $release_region_id = '未知区域-';
                        }

                        // 操作系统(1,ios;2,Android;3,h5;4,Amazon;)
                        if ($app_v['os_id'] == 1){
                            $os_id = 'ios-';
                        }elseif ($app_v['os_id'] == 2){
                            $os_id = 'Android-';
                        }elseif ($app_v['os_id'] == 3){
                            $os_id = 'h5-';
                        }elseif ($app_v['os_id'] == 4){
                            $os_id = 'Amazon-';
                        }else{
                            $os_id = '未知系统-';
                        }

                        $app_v[$info['base_sel_name']] = $release_region_id.$os_id.$app_v[$info['base_sel_name']].'-'.$app_v['app_id'];
                        unset($app_v['app_id']);
                        $popData[$app_k] = $app_v;
                    }

                }elseif($info['dim_name'] == '版本'){

                    $popData = DB::table($info['dim_table'])->select(['app_version','app_version as id'])->groupBy('app_version')->get();
                    $popData = Service::data($popData);

                }elseif($info['dim_name'] == '渠道'){

                    $popData = DB::table($info['dim_table'])->select($info['base_sel_name'],$info['base_sel_key'],'channel_id')->orderBy('sort',"desc")->get();
                    $popData = Service::data($popData);
                    foreach ($popData as $channel_k => $channel_v){
                        $channel_v[$info['base_sel_name']] = $channel_v[$info['base_sel_name']].'-'.$channel_v['channel_id'];
                        unset($channel_v['channel_id']);
                        $popData[$channel_k] = $channel_v;
                    }

                }else{
                    $popData = DB::table($info['dim_table'])->select($info['base_sel_name'],$info['base_sel_key'])->get();
                    $popData = Service::data($popData);
                }

                $popDimension['data'][$info['dim_id']] = array('id'=>$info['dim_id'],'value'=>$info['dim_name']);

                foreach ($popData as $pop){
                    if(!empty($powerList) && $info['dim_cfg']==1){
                        if(in_array($pop[$info['base_sel_key']], $powerList))
                            $popDimension['data'][$info['dim_id']]['sub'][] = array('id'=>$pop[$info['base_sel_key']],'value'=>$pop[$info['base_sel_name']]);
                        else {
                            //$popDimension['list'][$info['dim_id']]['sub'][0] = array('id'=>'-1','value'=>'请选择');
                        }
                    }else
                        $popDimension['data'][$info['dim_id']]['sub'][] = array('id'=>$pop[$info['base_sel_key']],'value'=>$pop[$info['base_sel_name']]);
                }
            }
        }

        $dataSelectk = array();
        foreach ($dataSelect['data']['sub'] as $datakey=>$dataSelectSub){
            $dataSelectk[] = array('value'=>$datakey,'sub'=>$dataSelectSub);

        }
        $dataSelect['data'] = $dataSelectk;


        $popDimension['data'] = array_values($popDimension['data']);
        $data['basicSelect'] = $basicSelect;
        $data['target_data'] = $dataSelect;
        $timeGranularity['select_id'] = '3';
        $timeGranularity['data'] = array(array('id'=>'1','value'=>'年'),array('id'=>'2','value'=>'月'),array('id'=>'3','value'=>'日'));
        $data['time_granularity'] = $timeGranularity;
        $timeCutBay['select_id']='1';
        $timeCutBay['data'] = array(array('id'=>'1','value'=>'时间序列'),array('id'=>'2','value'=>'时间段'));
        $data['time_cutbay_id'] = $timeCutBay;
        $data['condition_data'] = $popDimension;
        $popRelate = DB::table('s_cfg_select_compute')->select(['compute_id','compute_name','compute_code'])->get();
        $popRelate = Service::data($popRelate);
        $popRelation = array();
        $popRelation['relation_id'] = null;
        foreach ($popRelate as $popR ){
            $popRelation['data'][] = array('id'=>$popR['compute_id'],'value'=>$popR['compute_name']);
        }
        $data['relation_data'] = $popRelation;
        $data['time'] = array('start_time'=>date('Y-m-d',strtotime('-2 day')),'end_time'=>date('Y-m-d',strtotime('-2 day')));
        //已定制保存的邮件发送信息
        $Cwhere = '';
        if($company){
            $Cwhere = " and 1=1 ".DataSearchLogic::getCompanyPowerSql($company,'s_user_mail');
        }
        $sql = "select userid,mailInfo from s_user_mail where userid = {$userid} {$Cwhere} limit 1";
        $mailInfo = DB::select($sql);
        $mailInfo = Service::data($mailInfo);
        if ($mailInfo){
            $mail = json_decode($mailInfo[0]['mailInfo'],true);
        }else{
            $mail = [];
        }

        if(!empty($mail))
            $data['email'] = $mail;
        else $data['email'] = array();

        ApiResponseFactory::apiResponse($data,[]);
    }

    /**
     * get data init
     * @route({"GET", "/search/*"})
     * @param({"guid","$.path[2]"})
     */
    public static function getSearchData($params){

        // 用户ID
        $userid = isset($params['guid']) ? $params['guid'] : $_SESSION['erm_data']['guid'];
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 58;
        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        // 开始结束时间
        $stime = isset($params['start_time']) ? $params['start_time'] : '';
        $etime = isset($params['end_time']) ? $params['end_time'] : '';
        if(!$stime || !$etime){
            ApiResponseFactory::apiResponse([],[],751);
        }
        // 日 月 年
        $time_granularity_id = isset($params['time_granularity_id']) ? $params['time_granularity_id'] : 3;
        // 时间序列 时间段
        $time_cutbay_id = isset($params['time_cutbay_id']) ? $params['time_cutbay_id'] : 1;
        // 判断是否查询玉米广告数据
        $statistics = isset($params['statistics']) ? $params['statistics'] : 0;

        $search_table = 'zplay_basic_report_daily';
        //        if($time_granularity_id == 2 || $time_granularity_id == 1){//月
        //            $search_table = 'zplay_basic_report_month';
        //        }

        $sql =' where 1=1 ';
        $sql .=" and flow_type = 1 and platform_id != 'ptg31' ";
        // 判断查询哪张表
        if($statistics == 2){
            $search_table = 'zplay_basic_report_daily_channel';
        }else{
            $sql .=" and statistics = 0 ";
        }


        // 公司筛选
        if($company){
            $sql .= DataSearchLogic::getCompanyPowerSql($company,$search_table);
        }

        $selectCondition = $params['condition_list'];
        if (!is_array($selectCondition)){
            $selectCondition = json_decode($selectCondition,true);
        }

        //显示维度字段
        $display_dimension = [];
        $display_dimension[] = 'date_time';
        $join_sql='';
        $prderby='';
        $orderby = '';
        $groupby='';

        // 默认筛选应用维度
        $power_list = DB::table('s_cfg_select_dim')->select(['dim_id','dim_name','dim_type','dim_table','dim_order','dim_value','dim_table_id','dim_cfg','dim_table_id'])->where(['dim_cfg' => 1, 'dim_type' =>0,'currency_type'=>$currency_type_id])->orderBy('dim_order')->get();
        $power_list = Service::data($power_list);

        //返回用户下可查询的应用ID
        $map1['id'] = $userid;
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        $power = []; // 为空 则拥有全部查询权限
        if($userInfo[0]['app_permission'] != -2){
            $power = explode(',', $userInfo[0]['app_permission']);
        }

        // 搜索条件拼接
        $selectDimension = [];
        if($selectCondition){
            foreach ($selectCondition as $condition){
                $selectDimension[] = $condition['dimension_id'];//已选择的数据权限id列表
                $relation = DataSearchLogic::getRelation($condition['relation_id']);
                $relation = Service::data($relation);
                $dimension = DataSearchLogic::getDimension($condition['dimension_id'],$currency_type_id);
                $dimension = Service::data($dimension);

                // 查询条件拼接
                if($condition['dimension_id'] != 1) {
                    $power = [];
                }
                // 查询条件拼接
                if(isset($condition['values_id']) && $condition['values_id'] !== false){
                    if($condition['values_id'] == '-2'){
                        // 选择全部应用
                        if ($relation['compute_connect'] !== '='){
                            if (!empty($power)){
                                $i=1;
                                foreach ($power as $sp){
                                    if($i == 1)
                                        $Searchrelation = "and (";
                                    else $Searchrelation = $relation['compute_connect'];
                                    $sql .= "$Searchrelation {$search_table}.{$dimension['dim_table_id']} {$relation['compute_code']} '{$sp}' ";
                                    $i++;
                                }
                            }

                        }

                    }else{
                        if($condition['values_id'] != '-1'){
                            $valuesId = explode(',', $condition['values_id']);
                            $i=1;
                            foreach ($valuesId as $valueId){
                                if($i==1)
                                    $Searchrelation=" and (";
                                else $Searchrelation = $relation['compute_connect'];

                                $sql.="$Searchrelation {$search_table}.{$dimension['dim_table_id']} {$relation['compute_code']} '{$valueId}' ";
                                $i++;
                            }

                        }
                    }
                }

                // 自定义值 查询条件 拼接
                if(isset($condition['custom_text']) && $condition['custom_text']){
                    if(!empty($power)){
                        $i=1;
                        foreach ($power as $sp){
                            if($i==1)
                                $Searchrelation="and (";
                            else $Searchrelation = $relation['compute_connect'];
                            $sql.="$Searchrelation {$search_table}.{$dimension['dim_table_id']} {$relation['compute_code']} '{$sp}' ";
                            $i++;
                        }
                        $sql.=")";
                    }

                    $sql .= " and {$search_table}.{$dimension['dim_value']} {$relation['compute_code']}  '%{$condition['custom_text']}%' ";

                }else{
                    if($condition['values_id'] == '-2' && !empty($power))
                        $sql .= ")";
                    if($condition['values_id'] != '-2' && empty($power))
                        $sql .= ")";
                    if($condition['values_id'] != '-2' && !empty($power))
                        $sql .= ")";
                }
            }
            // 当搜索条件 未选择应用时 默认查询 当前用户拥有权限的应用
            foreach ($power_list as $slist){
                if(!in_array($slist['dim_id'], $selectDimension)){
                    if(!empty($power)){
                        $power_value_str = implode(',', $power);
                        $sql.=" and {$search_table}.{$slist['dim_table_id']} in ($power_value_str) ";
                    }
                }
            }

            // 查询5省份数据 未选择4国家时 查询省份ID>0 的数据，因为国家数据 省份都是0
            if (in_array(5,$selectDimension) && !in_array(4,$selectDimension)){
                $sql .= " and province_id > 0";
//                var_dump($sql);
            }

        }else{
            // 当没有选择任何 搜索条件时 默认查询 当前用户拥有权限的应用
            foreach ($power_list as $slist){
                //根据用户去查权限值
                if(!empty($power)){

                    $power_value_str = implode(',', $power);
                    $sql.=" and {$search_table}.{$slist['dim_table_id']} in ($power_value_str) ";
                }
            }
        }

        // 查询列
        $searchColumn = ' ';
        if($selectDimension){
            $basicSelect = $selectDimension;
            $basicss = DB::table('s_cfg_select_dim')->select('dim_id')->whereIn("dim_id",$basicSelect)->where(['currency_type'=>$currency_type_id])->orderBy('dim_order')->get();
            $basicss = Service::data($basicss);
            $dimBasicArr = array();
            foreach ($basicss as $basic){
                $dim_value = DataSearchLogic::getDimension($basic['dim_id'],$currency_type_id);
                $dim_value = Service::data($dim_value);
                $dimBasicArr[] = $dim_value['dim_value'];
                $display_dimension[] = $dim_value['dim_value'];
            }
            $dimBasicData = implode(",", $dimBasicArr);
            if(!$groupby)
                $groupby = " group by {$dimBasicData}";
            $searchColumn .= $dimBasicData;
        }

        // select 查询字段
        if(trim($searchColumn))
            $searchColumn .= ",";
        if($params['target_id'] == '-2'){
            $dimList = DataSearchLogic::getDimensionList(1,$currency_type_id);
            $searchColumn .= $dimList['dimList'];
            foreach ($dimList['dimArr'] as $dimArr){
                $display_dimension[] = $dimArr['dim_table_id'];
            }
        }else {
            $dataSelect = explode(',', $params['target_id']);
            $ss = DB::table('s_cfg_select_dim')->select('dim_id')->whereIn("dim_id",$dataSelect)->where(['currency_type'=>$currency_type_id])->orderBy('dim_order')->get();
            $ss = Service::data($ss);
            //	var_dump($ss);
            $dataSelectArr = array();
            foreach ($ss as $dataS){
                $dim_value_s = DataSearchLogic::getDimension($dataS['dim_id'],$currency_type_id);
                $dim_value_s = Service::data($dim_value_s);
                //var_dump($dim_value_s);
                $dataSelectArr[] = $dim_value_s['dim_value']." as ".$dim_value_s['dim_table_id'];
                $display_dimension[] = $dim_value_s['dim_table_id'];
            }
            $dimSelectData = implode(',', $dataSelectArr);
            $searchColumn.=$dimSelectData ;
        }

        $startTime = str_replace('-', '', $params['start_time']);
        $endTime = str_replace('-', '', $params['end_time']);
        $time_sql = " and date_time between '{$startTime}' and '{$endTime}'";


        // 分区查询
        $partition = '';
        $all_month_arr = Service::dateMonthsSections($params['start_time'],$params['end_time']);
        $all_month = [];
        if ($all_month_arr){
            foreach ($all_month_arr as $month_srt){
                $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
            }
            if ($all_month){
                $partition = " partition (".implode(',',$all_month).")";
            }
        }

        $date_time_column = '';
        if($time_cutbay_id == 1){//时间序列
            if($time_granularity_id == 3){//日
                if(!$groupby)
                    $groupby = " group by date_time";
                else $groupby.=" ,date_time";
                $date_time_column='  date_time';
            }
            if($time_granularity_id == 2){//月
                if(!$groupby)
                    $groupby = " group by SUBSTR(date_time, 1,7)";
                else $groupby.=" ,SUBSTR(date_time, 1,7)";
                $date_time_column="   SUBSTR(date_time, 1,7) ";
            }
            if($time_granularity_id == 1){//年
                if(!$groupby)
                    $groupby = " group by SUBSTR(date_time, 1,4 )";
                else $groupby.=" ,SUBSTR(date_time, 1,4 )";
                $date_time_column="  SUBSTR(date_time, 1,4 )  ";
            }
            $date_orderby = "  {$date_time_column},";
        }else {

            if($time_granularity_id == 3){//日
                //	if(!$groupby)
                //			$groupby = " group by '$startTime-$endTime'";
                //		else $groupby.=",'$startTime-$endTime'";
            }
            if($time_granularity_id == 2){//月
                $startTime = date('Ym',strtotime($startTime));
                $endTime = date('Ym',strtotime($endTime));
                //	if(!$groupby)
                //		$groupby = " group by SUBSTR(date_time, 1,6 )";
                //	else $groupby.=" ,SUBSTR(date_time, 1,6 )";

            }
            if($time_granularity_id == 1){//年
                $startTime = date('Y',strtotime($startTime));
                $endTime = date('Y',strtotime($endTime));
                //	if(!$groupby)
                //		$groupby = " group by SUBSTR(date_time, 1,4 )";
                //	else $groupby.=" ,SUBSTR(date_time, 1,4 )";
            }
            $date_orderby = "  $startTime-$endTime,";
            if(!$groupby)
                $groupby = " group by '$startTime-$endTime'";
            else $groupby.=",'$startTime-$endTime'";
            $date_time_column="  '$startTime-$endTime' ";
        }
        $pageSize = isset($params['size']) ? $params['size'] : 99999;
        $p = isset($params['page']) ? $params['page'] : 1;
        $gby="";

        //
        if(isset($params['sort_name']) && $params['sort_name'] && isset($params['sort_type']) && $params['sort_type']){
            $date_orderby = '';
            $sortType = $params['sort_type'];
            $dimSortColumn = DataSearchLogic::getDimensionByTableId($params['sort_name'],$currency_type_id);
            $dimSortColumn = Service::data($dimSortColumn);
            if($dimSortColumn['dim_type']=='1'){
                $orderby=" order by {$dimSortColumn['dim_value']} {$sortType}";
            }else{
                $gby=",{$search_table}.{$params['sort_name']} ";
                if($params['sort_name'] == 'date_time'){
                    $orderby = "  order by {$date_time_column} {$sortType} ";
                    $gby='';
                }else{
                    $orderby = "  order by {$search_table}.{$params['sort_name']} {$sortType}";

                }
            }
        }

        //点击搜索按钮上面的排序时
        if(isset($params['sort_list']) && $params['sort_list']){
            $sortColumn = $params['sort_list'];
            if (!is_array($sortColumn)){
                $sortColumn = json_decode($sortColumn,true);
            }
            if(!empty($sortColumn)){
                $date_orderby = '';
                $orderby=" order by ";
                $by='';
                $g = '';

                foreach ($sortColumn as $sColumn){
                    $dimSortColumn = DataSearchLogic::getDimension($sColumn['id'],$currency_type_id);
                    $dimSortColumn = Service::data($dimSortColumn);
                    if(isset($dimSortColumn['dim_type']) && ($dimSortColumn['dim_type']=='1')){

                        $by .= "{$dimSortColumn['dim_value']} {$sColumn['type']},";

                    }else{
                        if($sColumn['id'] == 'date_time')
                            $by .= "  {$date_time_column} {$sColumn['type']} ,";
                        else{
                            $g.=",{$search_table}.{$dimSortColumn['dim_table_id']} ";
                            $by .= "{$dimSortColumn['dim_value']} {$sColumn['type']},";
                        }
                    }
                }
                $by = rtrim($by,',');
                $orderby .= $by;
            }
        }

//        var_dump($join_sql,$sql,$time_sql,$groupby,$gby,$orderby,$date_orderby);

        if(!$orderby)
            $orderby=" order by ";
        else $orderby.=" ,";
        if(trim($orderby) == trim("order by  ,"))
            $orderby = rtrim($orderby,',');
        if ($date_orderby)
            $date_orderby = rtrim($date_orderby,',');
        if ($orderby)
            $orderby = rtrim($orderby,',');

        $searchSql ="select SQL_CALC_FOUND_ROWS a.* from (select   ".$date_time_column. " as date_time , ".$searchColumn." from {$search_table} {$partition} ".$join_sql.$sql.$time_sql.$groupby.$gby.$orderby.$date_orderby.")a  ";
        $countSql = "select count(*) c from (select $search_table.* from {$search_table} {$partition} ".$join_sql.$sql.$time_sql.$groupby.")a";

        $start = ($p-1) * $pageSize;
        $end = $p * $pageSize;
        $searchSql_p = $searchSql." limit {$start},{$pageSize}";
//        echo $searchSql;
//        echo $searchSql_p;
//        die;

        $tableTitle=array();
        foreach ($display_dimension as $display_column){
            $new_data=array();
            if($display_column == 'date_time'){
                $new_data['label'] = '日期';
                $new_data['chartX'] = true;
                $new_data['is_sort'] = true;
                $new_data['name'] = 'date_time';
                $tableTitle[] = $new_data;
                continue;
            }
            else
                $data = DB::table('s_cfg_select_dim')->select(['dim_name','dim_type','dim_table_id','dim_id'])->orWhere(['dim_table_id'=>$display_column,'dim_value'=>$display_column])->where(['currency_type'=>$currency_type_id])->first();

            $data = Service::data($data);

            if($data['dim_type']=='1'){
                $new_data['is_sort']=true;
            }
            else $new_data['is_sort']=true;

            $new_data['chart_id'] = $data['dim_id'];
            $new_data['label'] = $data['dim_name'];
            $new_data['name'] = $data['dim_table_id'];
            $tableTitle[] = $new_data;
        }

        //数据维度
        $dimDataArrll = DataSearchLogic::getDimensionList(1,$currency_type_id);
        $dimDataArrll = $dimDataArrll['dimArr'];

        $dataDimArr = [];
        foreach ($dimDataArrll as $dataDim){
            $dataDimArr[] = $dataDim['dim_table_id'];
        }

        $tableList = [];
        if(isset($params['is_export'])){
            $report_name = isset($params['report_name']) ? $params['report_name'] : "综合查询";
            $answer = DB::select($searchSql);
            $answer = Service::data($answer);
            $tableList = self::matchName($answer,$selectDimension,$currency_type_id) ? self::matchName($answer,$selectDimension,$currency_type_id) : $answer;
            $titleNameArr = [];
            foreach ($tableTitle as $tTitle){
                $titleNameArr[]= $tTitle['label'];
            }
            $title = iconv('utf-8','gb2312',implode(',', $titleNameArr));
            $values = is_array($tableList) ? $tableList : [];
            $string =Service::csv_output_str($title."\n", $values);
            $filename = iconv('utf-8','gb2312',$report_name).'-'.date('Ymd').'.csv'; //设置文件名
            Service::export_csv($filename,$string); //导出
            exit;

        }elseif(isset($params['is_mail'])){
            $search_name = isset($params['search_name']) ? $params['search_name'] : "用户ID{$userid}定制计划报表数据";
            $user_account = isset($params['user_account']) ? $params['user_account'] : "data_error@zplay.com";
            $answer = DB::select($searchSql);
            $answer = Service::data($answer);
            $tableList = self::matchName($answer,$selectDimension,$currency_type_id) ? self::matchName($answer,$selectDimension,$currency_type_id) : $answer;
            $titleNameArr = [];
            foreach ($tableTitle as $tTitle){
                $titleNameArr[]= $tTitle['label'];
            }
            $title = iconv('utf-8','gb2312',implode(',', $titleNameArr));
            $values = is_array($tableList) ? $tableList : [];
            $string =Service::csv_output_str($title."\n", $values);
            $filename = iconv('utf-8','gb2312',"综合查询-").date('Ymd').'.csv'; //设置文件名
            $dir = __DIR__.'/../../storage/mailExcel/'.date('Y-m-d');
            if(!is_dir($dir))
                mkdir($dir,0777);
            $filename =$dir. "/".$search_name."_".date('Y-m-d').".csv"; //设置文件名
            $real_filename = '/mailExcel/'.date('Y-m-d')."/".$search_name."_".date('Y-m-d').".csv";
            file_put_contents($filename,$string);

            $all_data['data']['table_title'] = $tableTitle;
            $all_data['data']['table_list'] = $answer;
            //var_dump($answer);
            $table = "<table  border='1' bordercolor='#ccc' cellspacing='0'>";
            $tr_title='<tr>';
            foreach ($tableTitle as $Ttitle){
                $tr_title.="<td>{$Ttitle['label']}</td>";
            }
            $tr_title.="</tr>";
            $tr_d='';
            foreach ($answer as $tr_data){
                $tr_d.= '<tr>';
                foreach ($tr_data as $t_data){
                    $tr_d.="<td>{$t_data}</td>";
                }
                $tr_d.="</tr>";
            }

            $table = $table.$tr_title.$tr_d."</table>";
            $mail_title = '数据平台报告_'.$search_name."_".date('Y-m-d');
            CommonFunction::sendCustomMail($table,$mail_title,$real_filename,$user_account);
            exit;

        }else{
            $answer = DB::select($searchSql_p);
            $answer = Service::data($answer);
            $tableList = self::matchName($answer,$selectDimension,$currency_type_id) ? self::matchName($answer,$selectDimension,$currency_type_id) : $answer;
        }
        $all_data['table_title'] = $tableTitle;
        $all_data['table_list'] = $tableList;

        $c_answer = DB::select($countSql);
        $c_answer = Service::data($c_answer);
        $count = $c_answer['0']['c'];

        $pageAll = ceil($count/$pageSize);
        $all_data['total'] = $count;
        $all_data['page_total'] = $pageAll;


        ApiResponseFactory::apiResponse($all_data,[]);

    }


    // 匹配相关名称
    private static function matchName($answer,$selectDimension,$currency_type_id){

        if ($answer){
            $basic_two_decimals = DB::table('s_cfg_select_dim')->select(['dim_id','dim_table_id','dim_decimals'])->where(['dim_type'=>1,'dim_decimals' =>2,'currency_type'=>$currency_type_id])->orderBy('dim_order')->get();
            $basic_two_decimals = Service::data($basic_two_decimals);
            $basic_two_decimals_new = [];
            if ($basic_two_decimals){
                foreach ($basic_two_decimals as $btdk => $btdv){
                    $basic_two_decimals_new[$btdv['dim_table_id']] = $btdv['dim_decimals'];
                }
            }

            $basic_four_decimals = DB::table('s_cfg_select_dim')->select(['dim_id','dim_table_id','dim_decimals'])->where(['dim_type'=>1,'dim_decimals' =>4,'currency_type'=>$currency_type_id])->orderBy('dim_order')->get();
            $basic_four_decimals = Service::data($basic_four_decimals);
            $basic_four_decimals_new = [];
            if ($basic_four_decimals){
                foreach ($basic_four_decimals as $bfdk => $bfdv){
                    $basic_four_decimals_new[$bfdv['dim_table_id']] = $bfdv['dim_decimals'];
                }
            }

            $all_basic_list = [];
            if ($selectDimension){
                $basic = DB::table('s_cfg_select_dim')->select(['dim_id','dim_name','dim_table','dim_value','dim_table_id','base_sel_key','base_sel_name'])->whereIn('dim_id',$selectDimension)->where(['currency_type'=>$currency_type_id])->orderBy('dim_order')->get();
                $basic = Service::data($basic);

                foreach ($basic as $key => $basic_info){
                    $pop_data_result = [];
                    $popData = [];
                    if($basic_info['dim_id']==4){
                        $popData = DB::table($basic_info['dim_table'])->select($basic_info['base_sel_name'],$basic_info['base_sel_key'])->where('type',2)->get();
                        $popData = Service::data($popData);


                    }elseif($basic_info['dim_id'] == 5){

                        $popData = DB::table($basic_info['dim_table'])->select($basic_info['base_sel_name'],$basic_info['base_sel_key'])->where('type',3)->get();
                        $popData = Service::data($popData);

                    }elseif($basic_info['dim_id'] == 1){

                        $popData = DB::table($basic_info['dim_table'])->select(['id','release_region_id','os_id','app_id','app_name'])->get();
                        $popData = Service::data($popData);

                    }elseif($basic_info['dim_id'] != 3 && $basic_info['dim_name'] != 62){
                        $popData = DB::table($basic_info['dim_table'])->select($basic_info['base_sel_name'],$basic_info['base_sel_key'])->get();
                        $popData = Service::data($popData);
                    }

                    if ($popData){
                        if($basic_info['dim_id'] == 1){
                            foreach ($popData as $p_k => $p_v){
                                // 发行区域ID(1,全球;2,国外;3,国内;)
                                $release_region_id = '';
                                if ($p_v['release_region_id'] == 1){
                                    $release_region_id = '全球-';
                                }elseif ($p_v['release_region_id'] == 2){
                                    $release_region_id = '国外-';
                                }elseif ($p_v['release_region_id'] == 3){
                                    $release_region_id = '国内-';
                                }else{
                                    $release_region_id = '未知区域-';
                                }

                                // 操作系统(1,ios;2,Android;3,h5;4,Amazon;)
                                $os_id = '';
                                if ($p_v['os_id'] == 1){
                                    $os_id = 'ios-';
                                }elseif ($p_v['os_id'] == 2){
                                    $os_id = 'Android-';
                                }elseif ($p_v['os_id'] == 3){
                                    $os_id = 'h5-';
                                }elseif ($p_v['os_id'] == 4){
                                    $os_id = 'Amazon-';
                                }else{
                                    $os_id = '未知系统-';
                                }

                                $pop_data_result[$p_k]['id'] = $p_v[$basic_info['base_sel_key']];
                                $pop_data_result[$p_k]['value'] = $release_region_id.$os_id.$p_v['app_name'].'-'.$p_v['app_id'];
                            }
                        }else{
                            foreach ($popData as $p_k => $p_v){
                                $pop_data_result[$p_k]['id'] = $p_v[$basic_info['base_sel_key']];
                                $pop_data_result[$p_k]['value'] = $p_v[$basic_info['base_sel_name']];
                            }
                        }
                    }
                    if ($pop_data_result) $all_basic_list[$basic_info['dim_value']] = $pop_data_result;

                }
            }

            foreach ($answer as $a_k => $answer_value){
                if ($all_basic_list){
                    foreach ($all_basic_list as $a_key => $all_basic_info){
                        if (isset($answer_value[$a_key]) && $all_basic_info){
                            foreach ($all_basic_info as $all_basic_value){
                                if ($answer_value[$a_key] == $all_basic_value['id']){
                                    $answer[$a_k][$a_key] = $all_basic_value['value'];
                                }
                            }
                        }
                    }
                }

            }

            if ($answer){
                $new_answer = [];
                foreach ($answer as $key => $value){
                    foreach ($value as $vk => $vv){
                        if (!$vv) $value[$vk] = 0;
                    }

                    if ($basic_four_decimals_new){
                        foreach ($basic_four_decimals_new as $bfdnk => $bfdnv){
                            if (isset($value[$bfdnk])){
                                $value[$bfdnk] = $value[$bfdnk] ? round($value[$bfdnk],$bfdnv) : round(0,$bfdnv);
                            }
                        }
                    }

                    if ($basic_two_decimals_new){

                        foreach ($basic_two_decimals_new as $btdnk => $btdnv){
                            if (isset($value[$btdnk])){
                                $value[$btdnk] = $value[$btdnk] ? round($value[$btdnk],$btdnv) : round(0,$btdnv);
                            }
                        }
                    }
                    $new_answer[$key] = array_values($value);
                }
                return $new_answer;
            }else{
                return false;
            }
        }else{
            return false;
        }

    }

    /**
     * @desc 邮件初始化 点击邮件发送按钮初始化
     * @route({"GET","/searchMail/*"})
     * @param({"guid","$.path[2]"})
     */
    public static function dataMailList($params){

        $userid = $_SESSION['erm_data']['guid'];
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;

        $MailInfo = DB::table('s_user_mail')
            ->select(['userid','mailInfo'])
            ->where(['userid' => $userid]);
        if($company == 9){
            $MailInfo->where('game_creator',$company);
        }elseif($company == 1 ){
            $MailInfo->whereNotIn('game_creator',[9]);
        }

        $MailInfo = $MailInfo->first();
        $MailInfo = Service::data($MailInfo);
        if($MailInfo['mailInfo']){
            $mailInfo = json_decode($MailInfo['mailInfo'],true);
        }else {
            $mailInfo = [];
        }

        ApiResponseFactory::apiResponse($mailInfo,[]);
    }

    /**
     * @desc 保存邮件
     * @route({"GET","/saveMail/*"})
     * @param({"guid","$.path[2]"})
     *
     */
    public static function dataMailSave($params){

        $userid = $_SESSION['erm_data']['guid'];
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        $email_info = isset($params['email']) ? $params['email'] : '';
        $game_creator = isset($params['user_company_id']) ? $params['user_company_id'] : 1;

        if($email_info){
            //如果邮件信息不是空的
            $update_time = date('Y-m-d H:i:s');
            $mailInfo = json_encode($email_info);

            $sql = "REPLACE INTO s_user_mail (`userid`,`mailInfo`,`update_time`,`game_creator`)VALUES($userid,'{$mailInfo}','{$update_time}','{$game_creator}')";
            $insertId = DB::insert($sql);
            if(!$insertId){
                ApiResponseFactory::apiResponse([],[],744);
            }
        }else{
            $re = DB::table('s_user_mail')->where(['userid' => $userid,'game_creator' => $game_creator])->delete();
            if(!$re){
                ApiResponseFactory::apiResponse([],[],745);
            }
        }

        $MailInfo = DB::table('s_user_mail')->select(['userid','mailInfo'])->where(['userid' => $userid,'game_creator' => $game_creator])->first();
        $MailInfo = Service::data($MailInfo);
        if($MailInfo['mailInfo']){
            $mailInfo = json_decode($MailInfo['mailInfo'],true);
        }else {
            $mailInfo = [];
        }
        ApiResponseFactory::apiResponse(['email' => $mailInfo],[]);
    }

    /**
     * @desc 查询定制条件
     * @route({"GET","custom"})
     *
     */
    public static function dataCustomQuery($params){

        $customId = isset($params['custom_id']) ? $params['custom_id'] : '';
        if(!$customId){
            ApiResponseFactory::apiResponse([],[],746);
        }

        $info = DB::table('s_search_custom')->select(['id','search_name','user_id','search_condition'])->where('id',$customId)->first();
        $info = Service::data($info);
        if(!$info){
            ApiResponseFactory::apiResponse([],[],747);
        }

        $search_condition = isset($info['search_condition']) ? $info['search_condition'] : '';
        $search_condition = json_decode($search_condition,true);

        $shortcut_id = isset($search_condition['shortcut_id']) ? $search_condition['shortcut_id'] : '';
        if($shortcut_id){ //当为true时为快捷选择
            if ($shortcut_id == 1){ // 今天
                $star_time =  date('Y-m-d');
                $end_time =  date('Y-m-d');
            }elseif($shortcut_id == 2){ // 昨天
                $star_time =  date('Y-m-d', strtotime('-1 day'));
                $end_time =  date('Y-m-d',strtotime('-1 day'));
            }elseif($shortcut_id == 3){ // 本月
                $star_time =  date('Y-m-01', strtotime(date("Y-m-d")));
                $end_time =  date('Y-m-d');
            }elseif($shortcut_id == 4){ // 上月
                $star_time =  date('Y-m-01', strtotime('-1 month'));
                $end_time =  date('Y-m-t', strtotime('-1 month'));
            }elseif($shortcut_id == 5){ // 最近7天
                $star_time =  date('Y-m-d', strtotime('-7 days'));
                $end_time =  date('Y-m-d');
            }elseif($shortcut_id == 6){ // 最近30天
                $star_time =  date('Y-m-d', strtotime('-30 days'));
                $end_time =  date('Y-m-d');
            }elseif($shortcut_id == 7){ // 最近90天
                $star_time =  date('Y-m-d', strtotime('-90 days'));
                $end_time =  date('Y-m-d');
            }
            $search_condition['end_time'] = $end_time;
            $search_condition['start_time'] = $star_time;
        }
        ApiResponseFactory::apiResponse($search_condition,[]);
    }

    /**
     * 定制保存
     * @route({"GET", "/save/*"})
     * @param({"guid","$.path[2]"})
     */
    public static function dataCustomSave($params){

        $userid = $_SESSION['erm_data']['guid'];;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }

        $searchCondition = $params ;
        $company = isset($searchCondition['user_company_id']) ? $searchCondition['user_company_id'] : 1;
        if(!isset($params['custom_id'])){
            $customName = $params['custom_name'];
            $searchData = DB::table('s_search_custom')
                ->select(['id','user_id','search_name'])
                ->where(['user_id'=>$userid, 'search_name' => $customName])
                ->first();
            $searchData = Service::data($searchData);
            if(!$searchData){
                $insertId = DB::table('s_search_custom')->insertGetId(['user_id'=>$userid,
                    'search_name' => $customName,
                    'create_time' => date('Y-m-d H:i:s'),
                    'search_condition' => json_encode($searchCondition),
                    'game_creator' => $company
                ]);
                $searchCondition = DataSearchLogic::searchCondition($userid,$company);
                $searchCondition = Service::data($searchCondition);
                $list = array();
                foreach ($searchCondition as $search){
                    $list[] = array('id'=>$search['id'],'value'=>$search['search_name']);

                }
                $data['custom_data']=array('select_id'=>$insertId,'data'=>$list);

                ApiResponseFactory::apiResponse($data,[]);
            }else{
                ApiResponseFactory::apiResponse([],[],749);
            }
        }elseif($params['custom_id']){
            $custom_id = $params['custom_id'];
            $type = isset($params['type']) ? $params['type'] : '';
            if (!$type){
                $params = ['search_condition'=>json_encode($searchCondition),'update_time'=>date('Y-m-d H:i:s'),'game_creator'=>$company];
                $re = DB::table('s_search_custom')->where(['id' => $custom_id, 'user_id' => $userid])->update($params);
                if(!$re){
                    ApiResponseFactory::apiResponse([],[],748);
                }

                $searchCondition = DataSearchLogic::searchCondition($userid,$company);
                $searchCondition = Service::data($searchCondition);
                $list = array();
                foreach ($searchCondition as $search){
                    $list[] = array('id'=>$search['id'],'value'=>$search['search_name']);
                }
                $data['custom_data']=array('select_id' => $custom_id,'data' => $list);

                ApiResponseFactory::apiResponse($data,[]);
            }else{
                $re = DB::table('s_search_custom')->where(['id' => $custom_id, 'user_id' => $userid])->delete();
                if(!$re){
                    ApiResponseFactory::apiResponse([],[],754);
                }

                $s_user_mail = DB::select("select * from s_user_mail where userid = {$userid}");
                $s_user_mail = Service::data($s_user_mail);
                //如果邮件信息不是空的
                if ($s_user_mail){
                    foreach ($s_user_mail as $kk => $vv){
                        $email_info = isset($vv['mailInfo']) ? $vv['mailInfo'] : '';
                        if ($email_info){
                            $email_info = json_decode($email_info,true);
                            foreach ($email_info as $key => $value){
                                if($value['custom_id'] == $custom_id){
                                    unset($email_info[$key]);
                                }
                            }
                        }

                        if ($email_info){
                            $update_time = date('Y-m-d H:i:s');

                            $mailInfo = json_encode(array_values($email_info));
                            $game_creator = $company;
                            $sql = "REPLACE INTO s_user_mail (`userid`,`mailInfo`,`update_time`,`game_creator`)VALUES($userid,'{$mailInfo}','{$update_time}','{$game_creator}')";
                            $insertId = DB::insert($sql);
                            if(!$insertId){
                                ApiResponseFactory::apiResponse([],[],755);
                            }
                        }else{
                            $re = DB::table('s_user_mail')->where(['userid' => $userid])->delete();
                            if(!$re){
                                ApiResponseFactory::apiResponse([],[],755);
                            }
                        }
                    }
                }

                // 返回列表
                $searchCondition = DataSearchLogic::searchCondition($userid,$company);
                $searchCondition = Service::data($searchCondition);
                $list = array();

                if ($searchCondition){
                    foreach ($searchCondition as $search){
                        $list[] = array('id'=>$search['id'],'value'=>$search['search_name']);
                    }
                    $data['custom_data']= ['select_id' => $custom_id,'data' => $list];
                }else{
                    $data['custom_data'] = [];
                }

                ApiResponseFactory::apiResponse($data,[]);
            }

        }else{
            ApiResponseFactory::apiResponse([],[],750);
        }
    }


    /**
     * 页面初始化
     */
    public static function dataIndexInit($params){

        $userid = $_SESSION['erm_data']['guid'];;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 60;
        //验证用户是否有权限登录
        $map1 = [];
        $map1['id'] = $userid;
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        //返回用户下用权限列表
        $app_permission = [];
        if($userInfo[0]['app_permission'] != -2){
            $app_permission = explode(',', $userInfo[0]['app_permission']);
        }

        $data = array();

        $gameData = array(array('id'=>'all','value'=>'全部应用'),array('id'=>'all_ios','value'=>'全部iOS应用'),array('id'=>'all_android','value'=>'全部Android应用'));
        //公司权限

        //当能查询到游戏权限时
        if($app_permission){
            $gameList = DB::table('c_app')
                ->select('id','app_name','app_id','release_region_id','os_id')
                ->where('status',1)
                ->whereIn("id",$app_permission);
            if($company == 9){
                $gameList->where('company_id',$company);
            }elseif($company == 1 ){
                $gameList->whereNotIn('company_id',[9]);
            }
            $gameList->orderBy('app_full_name');
            $gameList = $gameList->get();

        }else{
            $gameList = DB::table('c_app')
                ->select('id','app_name','app_id','release_region_id','os_id')
                ->where('status',1);
            if($company == 9){
                $gameList->where('company_id',$company);
            }elseif($company == 1 ){
                $gameList->whereNotIn('company_id',[9]);
            }
            $gameList->orderBy('app_full_name');
            $gameList = $gameList->get();
        }
        $gameList = Service::data($gameList);
        if ($gameList){
            foreach ($gameList as $game){

                // 发行区域ID(1,全球;2,国外;3,国内;)
                if ($game['release_region_id'] == 1){
                    $release_region_id = '全球-';
                }elseif ($game['release_region_id'] == 2){
                    $release_region_id = '国外-';
                }elseif ($game['release_region_id'] == 3){
                    $release_region_id = '国内-';
                }else{
                    $release_region_id = '未知区域-';
                }

                // 操作系统(1,ios;2,Android;3,h5;4,Amazon;)
                if ($game['os_id'] == 1){
                    $os_id = 'ios-';
                }elseif ($game['os_id'] == 2){
                    $os_id = 'Android-';
                }elseif ($game['os_id'] == 3){
                    $os_id = 'h5-';
                }elseif ($game['os_id'] == 4){
                    $os_id = 'Amazon-';
                }else{
                    $os_id = '未知系统-';
                }

                array_push($gameData,array('id'=>$game['id'],'value'=>$release_region_id.$os_id.$game['app_name'].'-'.$game['app_id']));
            }
        }

        $data['app']['select_id'] = 'all';
        $data['app']['app_dimension_id'] = 1;
        $data['app']['os_dimension_id'] = 6;
        $data['app']['country_dimension_id'] = 4;
        $data['app']['list'] = $gameData;
        $data['period']['select_id']='0';
        $data['period']['list'] = [
            ['id'=>'0','value'=>'今日'],
            ['id'=>'1','value'=>'昨日'],
            ['id'=>'2','value'=>'前天'],
            ['id'=>'7','value'=>'近7日'],
            ['id'=>'30','value'=>'本月'],
            ['id'=>'60','value'=>'上月']
            // ['id'=>'ALL','value'=>'累计']
        ];
        $showTimeInfo = DB::table('s_basic_data_homepage')->select("date_time")->distinct()->first();
        if ($currency_type_id == 60){
            $showTimeInfo = DB::table('s_basic_data_homepage_usd')->select("date_time")->distinct()->first();
        }
        $showTimeInfo = Service::data($showTimeInfo);
        $show_time = isset($showTimeInfo['date_time']) ? $showTimeInfo['date_time'] : date('Y-m-d',strtotime('-2 days'));
        $data['show_time'] = date('Y年m月d日',strtotime($show_time));

        $data['time'] =array('end_time'=>date('Y-m-d',strtotime('-2 day')),'start_time'=>date('Y-m-d',strtotime('-32 day')));
        //首页展示指标定制弹窗
        $targetSortList = array();
        $targetSortData = DB::table('s_cfg_select_dim')->select('dim_id','dim_name')->where(['homepage_on' =>1,'currency_type'=>$currency_type_id])->get();

        $targetSortData = Service::data($targetSortData);
        foreach ($targetSortData as $targetSortDataList){
            $targetSortList[] = array('id'=>$targetSortDataList['dim_id'],'value'=>$targetSortDataList['dim_name']);
        }

        // 特殊处理 补丁 91  开发者分成成本。    93 总成本    92 总利润
        $targetSortList[] = ['id'=>"93", 'value' => '总成本'];
        $targetSortList[] = ['id'=>"91", 'value' => '开发者分成成本'];
        $targetSortList[] = ['id'=>"92", 'value' => '总利润'];
        $data['target_list'] = $targetSortList;
        $custom_sort = DataSearchLogic::getTargetCustom($userid, 'custom_sort');
        sort($custom_sort);
        $data['custom_sort']['list'] = $custom_sort ? $custom_sort : [9,10,40,48,50,52];
        $data['custom_sort']['select_id'] = $custom_sort ? current($custom_sort) : current([9,10,40,48,50,52]);
        $custom_app = DataSearchLogic::getTargetCustom($userid, 'custom_app');
        sort($custom_app);
        $data['custom_app'] = $custom_app ? $custom_app : [9,10,40,48,50,92,52,54];
        $custom_chart = DataSearchLogic::getTargetCustom($userid, 'custom_chart');
        $data['custom_chart'] = $custom_chart ? $custom_chart : [9,10,40,48,50,92,52,54];

        ApiResponseFactory::apiResponse($data,[]);
    }

    /**
     * 应用列表
     */
    public static function dataAppList($params){

        $userid = $_SESSION['erm_data']['guid'];
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 60;

        $where = [];
        $custom_sort = $params['custom_sort'];//排序指标id
        $custom_sort_info = DataSearchLogic::getCustomSortInfo($custom_sort,$currency_type_id);
        $custom_sort_info = Service::data($custom_sort_info);
        if ($currency_type_id == 60){
            $order_by = 's_basic_data_homepage_usd.value'; //  desc
            $where['s_basic_data_homepage_usd.dim_id'] = $custom_sort ; // " and dim_id={$customSort}";
            if($company == 9){
                $where['s_basic_data_homepage_usd.game_creator'] = $company ;
            }elseif($company == 1 ){
                $where['not_in'] = ['s_basic_data_homepage_usd.game_creator',[9]] ;
            }
            $date_type = $params['date_type'];
            if($date_type === '0'){//今天
                $where['s_basic_data_homepage_usd.date_type'] = 0;
            }else{
                $where['s_basic_data_homepage_usd.date_type'] = $date_type;
            }

            //验证用户是否有权限登录
            $map1 = [];
            $map1['id'] = $userid;
            $user_info = UserLogic::Userlist($map1)->get();
            $user_info =Service::data($user_info);
            if(!$user_info) ApiResponseFactory::apiResponse([],[],741);
            //返回用户下用权限列表
            $app_permission = [];
            if($user_info[0]['app_permission'] != -2){
                $app_permission = explode(',', $user_info[0]['app_permission']);
            }

            $get_game = isset($params['app_select']) ? $params['app_select'] : 'all';
            if($get_game == 'all' || $get_game == ''){//全部应用
                if($app_permission)
                    $where['in'] = ['s_basic_data_homepage_usd.app_id',$app_permission];
            }elseif($get_game == 'all_ios'){
                $where['s_basic_data_homepage_usd.os_id'] = 1;
                if($app_permission)
                    $where['in'] = ['s_basic_data_homepage_usd.app_id',$app_permission];
            }elseif($get_game == 'all_android'){
                $where['s_basic_data_homepage_usd.os_id'] = 2;
                if($app_permission)
                    $where['in'] = ['s_basic_data_homepage_usd.app_id',$app_permission];
            }elseif($get_game == 'all_h5'){
                $where['s_basic_data_homepage_usd.os_id'] = 3;
                if($app_permission)
                    $where['in'] = ['s_basic_data_homepage_usd.app_id',$app_permission];
            }elseif($get_game == 'all_amazon'){
                $where['s_basic_data_homepage_usd.os_id'] = 4;
                if($app_permission)
                    $where['in'] = ['s_basic_data_homepage_usd.app_id',$app_permission];
            }else{
                $where['s_basic_data_homepage_usd.app_id'] = $get_game;
            }

            $request_num = isset($params['request']) ? $params['request'] : 9999;
            $group_by = "s_basic_data_homepage_usd.app_id";
            $fields = ['s_basic_data_homepage_usd.app_id','s_basic_data_homepage_usd.date_time','s_basic_data_homepage_usd.value','c_app.app_name','c_app.release_region_id','c_app.os_id','c_app.app_id as real_app_id'];
            $where['leftjoin'] = [
                ['c_app','c_app.id', 's_basic_data_homepage_usd.app_id']
            ];
        }else{
            $order_by = 's_basic_data_homepage.value'; //  desc
            $where['s_basic_data_homepage.dim_id'] = $custom_sort ; // " and dim_id={$customSort}";
            if($company == 9){
                $where['s_basic_data_homepage.game_creator'] = $company ;
            }elseif($company == 1 ){
                $where['not_in'] = ['s_basic_data_homepage.game_creator',[9]] ;
            }
            $date_type = $params['date_type'];
            if($date_type === '0'){//今天
                $where['s_basic_data_homepage.date_type'] = 0;
            }else{
                $where['s_basic_data_homepage.date_type'] = $date_type;
            }

            //验证用户是否有权限登录
            $map1 = [];
            $map1['id'] = $userid;
            $user_info = UserLogic::Userlist($map1)->get();
            $user_info =Service::data($user_info);
            if(!$user_info) ApiResponseFactory::apiResponse([],[],741);
            //返回用户下用权限列表
            $app_permission = [];
            if($user_info[0]['app_permission'] != -2){
                $app_permission = explode(',', $user_info[0]['app_permission']);
            }

            $get_game = isset($params['app_select']) ? $params['app_select'] : 'all';
            if($get_game == 'all' || $get_game == ''){//全部应用
                if($app_permission)
                    $where['in'] = ['s_basic_data_homepage.app_id',$app_permission];
            }elseif($get_game == 'all_ios'){
                $where['s_basic_data_homepage.os_id'] = 1;
                if($app_permission)
                    $where['in'] = ['s_basic_data_homepage.app_id',$app_permission];
            }elseif($get_game == 'all_android'){
                $where['s_basic_data_homepage.os_id'] = 2;
                if($app_permission)
                    $where['in'] = ['s_basic_data_homepage.app_id',$app_permission];
            }elseif($get_game == 'all_h5'){
                $where['s_basic_data_homepage.os_id'] = 3;
                if($app_permission)
                    $where['in'] = ['s_basic_data_homepage.app_id',$app_permission];
            }elseif($get_game == 'all_amazon'){
                $where['s_basic_data_homepage.os_id'] = 4;
                if($app_permission)
                    $where['in'] = ['s_basic_data_homepage.app_id',$app_permission];
            }else{
                $where['s_basic_data_homepage.app_id'] = $get_game;
            }

            $request_num = isset($params['request']) ? $params['request'] : 9999;
            $group_by = "s_basic_data_homepage.app_id";
            $fields = ['s_basic_data_homepage.app_id','s_basic_data_homepage.date_time','s_basic_data_homepage.value','c_app.app_name','c_app.release_region_id','c_app.os_id','c_app.app_id as real_app_id'];
            $where['leftjoin'] = [
                ['c_app','c_app.id', 's_basic_data_homepage.app_id']
            ];
        }

        $all_app_list = DataSearchLogic::getIndexAppList($currency_type_id,$where,$fields)->limit($request_num)->groupBy($group_by)->orderBy($order_by,'desc')->get();
        $all_app_list = Service::data($all_app_list);
        $app_list = [];
        $list = [];

        // 特殊处理 补丁 91  开发者分成成本。    93 总成本    92 总利润
        if ($custom_sort == 91 || $custom_sort == 92 || $custom_sort == 93){
            $custom_sort_info['dim_decimals'] = 2;
        }

        if ($all_app_list){
            foreach ($all_app_list as $key => $all_app_info){
                // 发行区域ID(1,全球;2,国外;3,国内;)
                if ($all_app_info['release_region_id'] == 1){
                    $release_region_id = '全球-';
                }elseif ($all_app_info['release_region_id'] == 2){
                    $release_region_id = '国外-';
                }elseif ($all_app_info['release_region_id'] == 3){
                    $release_region_id = '国内-';
                }else{
                    $release_region_id = '未知区域-';
                }

                // 操作系统(1,ios;2,Android;3,h5;4,Amazon;)
                if ($all_app_info['os_id'] == 1){
                    $os_id = 'ios-';
                }elseif ($all_app_info['os_id'] == 2){
                    $os_id = 'Android-';
                }elseif ($all_app_info['os_id'] == 3){
                    $os_id = 'h5-';
                }elseif ($all_app_info['os_id'] == 4){
                    $os_id = 'Amazon-';
                }else{
                    $os_id = '未知系统-';
                }

                // 因TD游戏月活用户数据没有，所以以下游戏中，本月、上月时间段，活跃用户指标设置为0
                if ($custom_sort == 10 && in_array($date_type,[30,60]) && in_array($all_app_info['real_app_id'],['ga007001','gi018010','ga019001','go015010','ga025004','gi007008','gi014004','ga014001','gi055002','ga028001','wo029004','gg007004','gi008022','gi016003','ga018002','go012003','ga035001','gg042002','gi007011','ga012001','gi021003','ga025001','go019010','gg008008','gi008021','gi015008','ga016001','go007012','wa032001','gg014002','gi007009','ga008002','gi019008','ga021001','go018012','ga042001','gi008020','gi015006','ga015001','gi033002','ga028002','ga135002','gg007005','gi008023'])){
                    $all_app_info['value'] = 0;
                }

                $list[] = [
                    'id' => $all_app_info['app_id'],
                    'value' => $release_region_id.$os_id.$all_app_info['app_name'].'-'.$all_app_info['real_app_id'],
                    'num' => sprintf("%.{$custom_sort_info['dim_decimals']}f",$all_app_info['value'])
                ];
            }

        }
        if($list){
            $list = Service::mySort($list,'num');
        }
        $app_list['list'] = $list;
        $app_list['total'] = $all_app_list ? count($all_app_list) : 0;

        ApiResponseFactory::apiResponse($app_list,[]);
    }


    /**
     * 应用概览数据
     */
    public static function dataGeneralList($params){

        $begintime_1 = time();

        $userid = $_SESSION['erm_data']['guid'];;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 60;
        //验证用户是否有权限登录
        $map1 = [];
        $map1['id'] = $userid;
        $user_info = UserLogic::Userlist($map1)->get();
        $user_info =Service::data($user_info);
        if(!$user_info) ApiResponseFactory::apiResponse([],[],741);
        //返回用户下用权限列表
        $app_permission = [];
        if($user_info[0]['app_permission'] != -2){
            $app_permission = explode(',', $user_info[0]['app_permission']);
        }

        $game_id_list = '';
        if($app_permission){
            $game_id_list = implode(',',$app_permission);
        }
        $where = " where 1=1 ";

        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        if($company == 9){
            $where .= " and b.game_creator = 9 ";
        }elseif($company == 1 ){
            $where .= " and b.game_creator != 9 ";
        }

        $get_game = isset($params['app_select']) ? $params['app_select'] : 'all';
        $group_by = 'group by b.dim_id,b.date_type';
        if($get_game=='all'||$get_game==''){//全部应用
            if($game_id_list)
                $where.=" and b.app_id in($game_id_list)";
        }elseif($get_game=='all_ios'){
            $where.=" and b.os_id = 1";
            if($game_id_list)
                $where.=" and b.app_id in($game_id_list)";
        }elseif($get_game=='all_android'){
            $where.=" and b.os_id = 2";
            if($game_id_list)
                $where.=" and b.app_id in($game_id_list)";
        }elseif($get_game=='all_h5'){
            $where.=" and b.os_id = 3";
            if($game_id_list)
                $where.=" and b.app_id in($game_id_list)";
        }elseif($get_game=='all_amazon'){
            $where.=" and b.os_id = 4";
            if($game_id_list)
                $where.=" and b.app_id in($game_id_list)";
        }else{
            $where.=" and b.app_id = {$get_game}";
            $group_by .= " ,b.app_id ";
        }

        $where_new = $where;
        $custom_app = explode(',',$params['custom_app']);
        $new_custom_app = [];
        $old_custom_app = [];
        foreach ($custom_app as $custom_id){
            if (in_array($custom_id,[91,92,93])){
                $new_custom_app[] = $custom_id;
            }else{
                $old_custom_app[] = $custom_id;
            }
        }

        if ($new_custom_app){
            $new_custom_app_str = implode(',',$new_custom_app);
            $where_new .=" and b.dim_id in ({$new_custom_app_str}) ";
        }
        if($old_custom_app){
            $where .= " and s.currency_type = {$currency_type_id} ";
            $old_custom_app_str = implode(',',$old_custom_app);
            $where .=" and b.dim_id in ({$old_custom_app_str}) ";
        }

        $basic_data_homepage = 's_basic_data_homepage';
        if($currency_type_id == 60){
            $basic_data_homepage = 's_basic_data_homepage_usd';
        }

        $searchSql = "select g.app_id,date_time,CASE s.dim_sumtype  WHEN 'sum' THEN 	sum(`value`) ELSE avg(`value`) END `value`,b.dim_id,s.dim_decimals,s.dim_name,date_type from {$basic_data_homepage} b left join c_app g on b.app_id = g.id left join s_cfg_select_dim s on b.dim_id = s.dim_id ".$where.$group_by." order by b.dim_id ";

//        echo $searchSql;die;
        $result_list = DB::select($searchSql);
        $result_list = Service::data($result_list);

        // 特殊处理 补丁 91  开发者分成成本。    93 总成本    92 总利润
//        select date_time,sum(`value`) as`value`,b.dim_id,4 as dim_decimals, case when b.dim_id = 91 then '开发者分成成本' when b.dim_id = 92 then '总利润' when b.dim_id = 93 then '总成本' end  as dim_name,date_type from s_basic_data_homepage b left join c_app g on b.app_id = g.id where 1=1 and b.game_creator != 9  and b.dim_id in (91,92,93) group by b.dim_id,b.date_type order by b.dim_id ;

        if ($new_custom_app) {
            $searchSql_new = "select g.app_id,date_time,sum(`value`) as `value`,b.dim_id,2 as `dim_decimals`,case when b.dim_id = 91 then '开发者分成成本' when b.dim_id = 92 then '总利润' when b.dim_id = 93 then '总成本' end  as dim_name,date_type from {$basic_data_homepage} b left join c_app g on b.app_id = g.id " . $where_new . $group_by . " order by b.dim_id ";

            $result_list_new = DB::select($searchSql_new);
            $result_list_new = Service::data($result_list_new);

            if ($result_list_new){
                $result_list = array_merge($result_list,$result_list_new);
            }
        }

//        var_dump(count($result_list));die;
        $new_data = array();
        if($result_list){
            foreach ($result_list as $sData){
                $sData['value'] = sprintf("%.{$sData['dim_decimals']}f",$sData['value']);
                $new_data[$sData['dim_id']][$sData['date_type']] = $sData;
            }
        }

        ksort($new_data);
        $tableList = array();
        $new_arr = array();


//        var_dump($new_data);die;

        foreach ($new_data as $key=>$customData){

            foreach ($customData as $cData){
                //var_dump($cData);
                $new_arr['id'] = $key;
                $new_arr['name'] = $cData['dim_name'];
//                if ($key != 10){
//                    $new_arr['today'] = $customData[0]['value'];
//                    $new_arr['todayRate'] = (isset($customData[1]['value']) && $customData[1]['value'] != '0.00' && $customData[1]['value']) ? round((($customData[0]['value']-$customData[1]['value'])/abs($customData[1]['value']))*100)."%" : "0%";
//                    if(abs(rtrim($new_arr['todayRate'],'%'))=='0')
//                        $new_arr['todayRate'] = '0%';
//
//                    $new_arr['yesterday'] = isset($customData[1]['value']) ? $customData[1]['value'] : 0;
//                    $new_arr['yestRate'] =(isset($customData[2]['value']) && $customData[2]['value']!='0.00'&&$customData[2]['value'])?round((($customData[1]['value']-$customData[2]['value'])/abs($customData[2]['value']))*100)."%":"0%";
//                    if(abs(rtrim($new_arr['yestRate'],'%'))=='0')
//                        $new_arr['yestRate'] = '0%';
//
//                    $new_arr['threeday'] = isset($customData[2]['value']) ? $customData[2]['value'] : 0;
//                    $new_arr['threeRate'] =(isset($customData[3]['value']) && $customData[3]['value']!='0.00'&&$customData[3]['value'])?round((($customData[2]['value']-$customData[3]['value'])/abs($customData[3]['value']))*100)."%":"0%";
//                    if(abs(rtrim($new_arr['threeRate'],'%'))=='0')
//                        $new_arr['threeRate'] = '0%';
//
//                    $new_arr['seven'] = isset($customData[7]['value']) ? $customData[7]['value'] : 0;;
//                    $new_arr['sevenRate'] =(isset($customData[14]['value']) && $customData[14]['value']!='0.00'&&$customData[14]['value'])? round((($customData[7]['value']-$customData[14]['value'])/abs($customData[14]['value']))*100)."%":"0%";
//                    if(abs(rtrim($new_arr['sevenRate'],'%'))=='0')
//                        $new_arr['sevenRate'] = '0%';
//
//                    $new_arr['thirty'] = isset($customData[30]['value']) ? $customData[30]['value'] : 0;
//                    $new_arr['thirtyRate'] = (isset($customData[60]['value']) && $customData[60]['value']!='0.00'&&$customData[60]['value'])?round((($customData[30]['value']-$customData[60]['value'])/abs($customData[60]['value']))*100)."%":"0%";
//                    if(abs(rtrim($new_arr['thirtyRate'],'%'))=='0')
//                        $new_arr['thirtyRate'] = '0%';
//
//                    $new_arr['total'] = isset($customData[-5]['value']) ? $customData[-5]['value'] : 0;
//
//                }else{
//                    $new_arr['today'] = $customData[0]['value'] / 1;
//                    $new_arr['todayRate'] = (isset($customData[1]['value']) && $customData[1]['value'] != '0.00' && $customData[1]['value']) ? round((($customData[0]['value']-$customData[1]['value'])/abs($customData[1]['value']))*100)."%" : "0%";
//                    if(abs(rtrim($new_arr['todayRate'],'%'))=='0')
//                        $new_arr['todayRate'] = '0%';
//
//                    $new_arr['yesterday'] = isset($customData[1]['value']) ? $customData[1]['value'] / 1 : 0;
//                    $new_arr['yestRate'] =(isset($customData[2]['value']) && $customData[2]['value']!='0.00'&&$customData[2]['value'])?round((($customData[1]['value']-$customData[2]['value'])/abs($customData[2]['value']))*100)."%":"0%";
//                    if(abs(rtrim($new_arr['yestRate'],'%'))=='0')
//                        $new_arr['yestRate'] = '0%';
//
//                    $new_arr['threeday'] = isset($customData[2]['value']) ? $customData[2]['value'] / 1 : 0;
//                    $new_arr['threeRate'] =(isset($customData[3]['value']) && $customData[3]['value']!='0.00'&&$customData[3]['value'])?round((($customData[2]['value']-$customData[3]['value'])/abs($customData[3]['value']))*100)."%":"0%";
//                    if(abs(rtrim($new_arr['threeRate'],'%'))=='0')
//                        $new_arr['threeRate'] = '0%';
//
//                    $new_arr['seven'] = isset($customData[7]['value']) ? round($customData[7]['value'] / 7) : 0;;
//                    $new_arr['sevenRate'] =(isset($customData[14]['value']) && $customData[14]['value']!='0.00'&&$customData[14]['value'])? round(((($customData[7]['value'] / 7)-($customData[14]['value'] / 7))/abs($customData[14]['value'] / 7))*100)."%":"0%";
//                    if(abs(rtrim($new_arr['sevenRate'],'%'))=='0')
//                        $new_arr['sevenRate'] = '0%';
//
//                    $new_arr['thirty'] = isset($customData[30]['value']) ? round($customData[30]['value'] / 30) : 0;
//                    $new_arr['thirtyRate'] = (isset($customData[60]['value']) && $customData[60]['value']!='0.00'&&$customData[60]['value'])?round(((($customData[30]['value'] / 30)-($customData[60]['value'] / 30))/abs($customData[60]['value'] / 30))*100)."%":"0%";
//                    if(abs(rtrim($new_arr['thirtyRate'],'%'))=='0')
//                        $new_arr['thirtyRate'] = '0%';
//                    $new_arr['total'] = '-';
//                }
                if ($key != 10){
                    if (isset($customData[0]['value'])) {
                        $new_arr['today'] = isset($customData[0]['value']) ? $customData[0]['value'] : 0;
                        $new_arr['todayRate'] = (isset($customData[1]['value']) && $customData[1]['value'] != '0.00' && isset($customData[1]['value']) && $customData[1]['value']) ? round((($customData[0]['value'] - $customData[1]['value']) / abs($customData[1]['value'])) * 100) . "%" : "0%";
                        if (abs(rtrim($new_arr['todayRate'], '%')) == '0') $new_arr['todayRate'] = '0%';
                    }else{
                        $new_arr['today'] = 0;
                        $new_arr['todayRate'] = "0%";
                    }

                    if (isset($customData[1]['value'])) {
                        $new_arr['yesterday'] = isset($customData[1]['value']) ? $customData[1]['value'] : 0;
                        $new_arr['yestRate'] = (isset($customData[2]['value']) && $customData[2]['value'] != '0.00' && $customData[2]['value']) ? round((($customData[1]['value'] - $customData[2]['value']) / abs($customData[2]['value'])) * 100) . "%" : "0%";
                        if (abs(rtrim($new_arr['yestRate'], '%')) == '0') $new_arr['yestRate'] = '0%';
                    }else{
                        $new_arr['yesterday'] = 0;
                        $new_arr['yestRate'] = "0%";
                    }

                    if (isset($customData[2]['value'])) {
                        $new_arr['threeday'] = isset($customData[2]['value']) ? $customData[2]['value'] : 0;
                        $new_arr['threeRate'] = (isset($customData[3]['value']) && $customData[3]['value'] != '0.00' && $customData[3]['value']) ? round((($customData[2]['value'] - $customData[3]['value']) / abs($customData[3]['value'])) * 100) . "%" : "0%";
                        if (abs(rtrim($new_arr['threeRate'], '%')) == '0') $new_arr['threeRate'] = '0%';
                    }else{
                        $new_arr['threeday'] = 0;
                        $new_arr['threeRate'] = "0%";
                    }

                    if (isset($customData[7]['value'])) {
                        $new_arr['seven'] = isset($customData[7]['value']) ? $customData[7]['value'] : 0;
                        $new_arr['sevenRate'] = (isset($customData[14]['value']) && $customData[14]['value'] != '0.00' && $customData[14]['value']) ? round((($customData[7]['value'] - $customData[14]['value']) / abs($customData[14]['value'])) * 100) . "%" : "0%";
                        if (abs(rtrim($new_arr['sevenRate'], '%')) == '0') $new_arr['sevenRate'] = '0%';
                    }else{
                        $new_arr['seven'] = 0;
                        $new_arr['sevenRate'] = "0%";
                    }

                    if (isset($customData[30]['value'])) {
                        $new_arr['thirty'] = isset($customData[30]['value']) ? $customData[30]['value'] : 0;
                        $new_arr['thirtyRate'] = (isset($customData[60]['value']) && $customData[60]['value'] != '0.00' && $customData[60]['value']) ? round((($customData[30]['value'] - $customData[60]['value']) / abs($customData[60]['value'])) * 100) . "%" : "0%";
//                        if (abs(rtrim($new_arr['thirtyRate'], '%')) == '0') $new_arr['thirtyRate'] = '0%';
                        $new_arr['thirtyRate'] = "";
                    }else{
                        $new_arr['thirty'] = 0;
                        $new_arr['thirtyRate'] = "";
                    }

                    if (isset($customData[60]['value'])) {
                        $new_arr['sixty'] = isset($customData[60]['value']) ? $customData[60]['value'] : 0;
                        $new_arr['sixtyRate'] = (isset($customData[90]['value']) && $customData[90]['value'] != '0.00' && $customData[90]['value']) ? round((($customData[60]['value'] - $customData[90]['value']) / abs($customData[90]['value'])) * 100) . "%" : "0%";
                        if (abs(rtrim($new_arr['sixtyRate'], '%')) == '0') $new_arr['sixtyRate'] = '0%';
                    }else{
                        $new_arr['sixty'] = 0;
                        $new_arr['sixtyRate'] = "0%";
                    }
                    //$new_arr['total'] = isset($customData[-5]['value']) ? $customData[-5]['value'] : 0;

                }else{
                    if (isset($customData[0]['value'])) {
                        $new_arr['today'] = $customData[0]['value'] / 1;
                        $new_arr['todayRate'] = (isset($customData[1]['value']) && $customData[1]['value'] != '0.00' && $customData[1]['value']) ? round((($customData[0]['value'] - $customData[1]['value']) / abs($customData[1]['value'])) * 100) . "%" : "0%";
                        if (abs(rtrim($new_arr['todayRate'], '%')) == '0') $new_arr['todayRate'] = '0%';
                    }else{
                        $new_arr['today'] = 0;
                        $new_arr['todayRate'] = "0%";
                    }

                    if (isset($customData[1]['value'])) {
                        $new_arr['yesterday'] = isset($customData[1]['value']) ? $customData[1]['value'] / 1 : 0;
                        $new_arr['yestRate'] = (isset($customData[2]['value']) && $customData[2]['value'] != '0.00' && $customData[2]['value']) ? round((($customData[1]['value'] - $customData[2]['value']) / abs($customData[2]['value'])) * 100) . "%" : "0%";
                        if (abs(rtrim($new_arr['yestRate'], '%')) == '0') $new_arr['yestRate'] = '0%';
                    }else{
                        $new_arr['yesterday'] = 0;
                        $new_arr['yestRate'] = "0%";
                    }

                    if (isset($customData[2]['value'])) {
                        $new_arr['threeday'] = isset($customData[2]['value']) ? $customData[2]['value'] / 1 : 0;
                        $new_arr['threeRate'] = (isset($customData[3]['value']) && $customData[3]['value'] != '0.00' && $customData[3]['value']) ? round((($customData[2]['value'] - $customData[3]['value']) / abs($customData[3]['value'])) * 100) . "%" : "0%";
                        if (abs(rtrim($new_arr['threeRate'], '%')) == '0') $new_arr['threeRate'] = '0%';
                    }else{
                        $new_arr['threeday'] = 0;
                        $new_arr['threeRate'] = "0%";
                    }

                    if (isset($customData[7]['value'])) {
                        $new_arr['seven'] = isset($customData[7]['value']) ? round($customData[7]['value'] / 7) : 0;;
                        $new_arr['sevenRate'] = (isset($customData[14]['value']) && $customData[14]['value'] != '0.00' && $customData[14]['value']) ? round(((($customData[7]['value'] / 7) - ($customData[14]['value'] / 7)) / abs($customData[14]['value'] / 7)) * 100) . "%" : "0%";
                        if (abs(rtrim($new_arr['sevenRate'], '%')) == '0') $new_arr['sevenRate'] = '0%';
                    }else{
                        $new_arr['seven'] = 0;
                        $new_arr['sevenRate'] = "0%";
                    }

                    // 因TD游戏月活用户数据没有，所以以下游戏中，本月、上月时间段，活跃用户指标设置为0

                    if (isset($customData[30]['value'])) {
                        if (in_array($cData['app_id'],['ga007001','gi018010','ga019001','go015010','ga025004','gi007008','gi014004','ga014001','gi055002','ga028001','wo029004','gg007004','gi008022','gi016003','ga018002','go012003','ga035001','gg042002','gi007011','ga012001','gi021003','ga025001','go019010','gg008008','gi008021','gi015008','ga016001','go007012','wa032001','gg014002','gi007009','ga008002','gi019008','ga021001','go018012','ga042001','gi008020','gi015006','ga015001','gi033002','ga028002','ga135002','gg007005','gi008023'])){
                            $new_arr['thirty'] = 0;
                            $new_arr['thirtyRate'] = "";
                        }else{
                            $new_arr['thirty'] = isset($customData[30]['value']) ? round($customData[30]['value']) : 0;
                            $new_arr['thirtyRate'] = (isset($customData[60]['value']) && $customData[60]['value'] != '0.00' && $customData[60]['value']) ? round(((($customData[30]['value']) - ($customData[60]['value'])) / abs($customData[60]['value'])) * 100) . "%" : "0%";
//                        if (abs(rtrim($new_arr['thirtyRate'], '%')) == '0') $new_arr['thirtyRate'] = '0%';
                            $new_arr['thirtyRate'] = "";
                        }

                    }else{
                        $new_arr['thirty'] = 0;
                        $new_arr['thirtyRate'] = "";
                    }

                    if (isset($customData[60]['value'])) {
                        if (in_array($cData['app_id'],['ga007001','gi018010','ga019001','go015010','ga025004','gi007008','gi014004','ga014001','gi055002','ga028001','wo029004','gg007004','gi008022','gi016003','ga018002','go012003','ga035001','gg042002','gi007011','ga012001','gi021003','ga025001','go019010','gg008008','gi008021','gi015008','ga016001','go007012','wa032001','gg014002','gi007009','ga008002','gi019008','ga021001','go018012','ga042001','gi008020','gi015006','ga015001','gi033002','ga028002','ga135002','gg007005','gi008023'])){
                            $new_arr['sixty'] = 0;
                            $new_arr['sixtyRate'] = "0%";
                        }else{
                            $new_arr['sixty'] = isset($customData[60]['value']) ? round($customData[60]['value']) : 0;
                            $new_arr['sixtyRate'] = (isset($customData[90]['value']) && $customData[90]['value'] != '0.00' && $customData[90]['value']) ? round(((($customData[60]['value']) - ($customData[90]['value'])) / abs($customData[90]['value'])) * 100) . "%" : "0%";
                            if (abs(rtrim($new_arr['sixtyRate'], '%')) == '0') $new_arr['sixtyRate'] = '0%';
                        }

                    }else{
                        $new_arr['sixty'] = 0;
                        $new_arr['sixtyRate'] = "0%";
                    }

                    // $new_arr['total'] = '-';
                }

            }
            $tableList[] = $new_arr;
        }

//        $endtime_1 = time();
//
//        $message = '用户ID：'.$userid.'，数据概览开始时间：'.$begintime_1.',结束时间：'.$endtime_1.',时间差'.($endtime_1-$begintime_1);
//
//        $dir = './storage/country';
//        if (!is_dir($dir)) {
//            mkdir($dir,0777,true);
//        }
//        $logFilename = $dir.'/'.'country.log';
//        //生成日志
//        file_put_contents( $logFilename,$message . "\n\n",FILE_APPEND);

        ApiResponseFactory::apiResponse(['table_list' => $tableList],[]);
    }


    /**
     * 趋势分析
     */
    public static function dataAppLine($params){
        //游戏权限
        $userid = $_SESSION['erm_data']['guid'];;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 60;
        $get_game = isset($params['app_select']) ? $params['app_select'] : 'all';
        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        $customChart = $params['custom_chart'];

        // 特殊处理 补丁 91  开发者分成成本。    93 总成本    92 总利润
        if (in_array($customChart,[91,92,93])){

            if ($currency_type_id == 60){
                $develop_table_name = 'zplay_divide_develop';
            }else{
                $develop_table_name = 'zplay_divide_develop_cny';
            }


            //验证用户是否有权限登录
            $map1 = [];
            $map1['id'] = $userid;
            $user_info = UserLogic::Userlist($map1)->get();
            $user_info =Service::data($user_info);
            if(!$user_info) ApiResponseFactory::apiResponse([],[],741);
            //返回用户下用权限列表
            $app_permission = [];
            if($user_info[0]['app_permission'] != -2){
                $app_permission = $user_info[0]['app_permission'];
            }
            $game_id_list = $app_permission;
//            if($app_permission){
//                $apps = DB::select("select id,app_id from c_app where id in ($app_permission)");
//                $apps = Service::data($apps);
//                if ($apps){
//                    foreach ($apps as $app_id_info){
//                        $power[] = $app_id_info['app_id'];
//                    }
//
//                    $game_id_list = implode("','",$power);
//                }
//            }

            $g_by = ' group by date';
            $where = " where 1=1 ";

            if($company == 9){
                $where .= " and game_creator = 9 ";
            }elseif($company == 1 ){
                $where .= " and game_creator != 9 ";
            }
            $startTime = $params['start_time'];
            $endTime = $params['end_time'];
            $where .= " and date between '{$startTime}' and '{$endTime}'";

            if($get_game=='all'||$get_game==''){//全部应用
                if($game_id_list)
                    $where.=" and app_id in($game_id_list)";
            }elseif($get_game=='all_ios'){
                $where.=" and os_id = 1";
                if($game_id_list)
                    $where.=" and app_id in($game_id_list)";
            }elseif($get_game=='all_android'){
                $where.=" and os_id = 2";
                if($game_id_list)
                    $where.=" and app_id in($game_id_list)";
            }elseif($get_game=='all_h5'){
                $where.=" and os_id = 3";
                if($game_id_list)
                    $where.=" and app_id in($game_id_list)";
            }elseif($get_game=='all_amazon'){
                $where.=" and os_id = 4";
                if($game_id_list)
                    $where.=" and app_id in($game_id_list)";
            }else{
                $where.=" and app_id = {$get_game}";
            }

            // 特殊处理 补丁 91  开发者分成成本   92 总利润   93 总成本
            $select_str = '';
            $dim_name = '';
            if ($customChart == 91){
                $dim_name = '开发者分成成本';
                $select_str = " sum(develop_cost) as money ";
            }elseif ($customChart == 92){
                $dim_name = '总利润';
                $select_str = " (sum(ff_income)+sum(ad_income))-(sum(develop_cost)+sum(tg_cost)) as money ";
            }elseif ($customChart == 93){
                $dim_name = '总成本';
                $select_str = " sum(develop_cost)+sum(tg_cost) as money ";
            }


            $new_sql = "select date, {$select_str}  from {$develop_table_name} {$where} group by date order by date";

            $chart_answer = Db::select($new_sql);
            $chart_answer = Service::data($chart_answer);


            $time_period = Service::timePeriod($startTime,$endTime);
            $date_target_arr = []; // 日期时间段
            // 时间段区分
            $date_time_column = '';
            for ($d = 0; $d <= $time_period; $d++){
                $date_target_arr[date('Y-m-d',strtotime("{$startTime} +$d days"))] = [];
            }

            $chartx = [];
            $chartList = [];
            $chartList_old = [];
            // chartx
            foreach ($date_target_arr as $dtaak => $dtaav){
                $chartx[] = $dtaak;
            }

            foreach ($date_target_arr as $dtak => $dtav){
                foreach($chart_answer as $chart_data){
                    if ($dtak == $chart_data['date']) {
                        $chartList_old[$dtak] = round($chart_data['money'], 2);
                        break;
                    }
                }
            }


            foreach ($date_target_arr as $ddtak => $ddtav){
                if (key_exists($ddtak,$chartList_old)){
                    $chartList[] = $chartList_old[$ddtak];
                }else{
                    $chartList[] = 0;
                }
            }


            ApiResponseFactory::apiResponse(array('id'=>$customChart,'name'=>$dim_name,'chartList'=>$chartList,'chartx'=>$chartx),[]);

        }else{
            $searchWhereInfo  = self::getSearchWhere($userid, $get_game, $params['start_time'],  $params['end_time'],$company);
            $where = $searchWhereInfo['searchWhere'];
            $g_by = $searchWhereInfo['g_by'];
            $custom_select_arr = self::getAnalyStr($customChart,$currency_type_id);

            $select_dim_str = $custom_select_arr['select_dim_str'];
            $column = $custom_select_arr['data_dim_column'];
            $order_by = " order by date_time";
            $group_by = " group by date_time";

            // 分区查询
            $partition = '';
            $all_month_arr = Service::dateMonthsSections($params['start_time'],$params['end_time']);
            $all_month = [];
            if ($all_month_arr){
                foreach ($all_month_arr as $month_srt){
                    $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
                }
                if ($all_month){
                    $partition = " partition (".implode(',',$all_month).")";
                }
            }
            $where .=" and flow_type = 1 and statistics = 0 and b.platform_id != 'ptg31' ";

            $sql = "select date_time, ".$select_dim_str." from zplay_basic_report_daily {$partition} b".$where.$group_by.$g_by.$order_by;

            $chart_answer = Db::select($sql);
            $chart_answer = Service::data($chart_answer);

            $basic_two_decimals = DB::table('s_cfg_select_dim')->select(['dim_id','dim_table_id','dim_decimals'])->where(['dim_type'=>1,'dim_decimals' =>2,'currency_type'=>$currency_type_id])->orderBy('dim_order')->get();
            $basic_two_decimals = Service::data($basic_two_decimals);
            $basic_two_decimals_new = [];
            if ($basic_two_decimals){
                foreach ($basic_two_decimals as $btdk => $btdv){
                    $basic_two_decimals_new[$btdv['dim_table_id']] = $btdv['dim_decimals'];
                }
            }

            $basic_four_decimals = DB::table('s_cfg_select_dim')->select(['dim_id','dim_table_id','dim_decimals'])->where(['dim_type'=>1,'dim_decimals' =>4,'currency_type'=>$currency_type_id])->orderBy('dim_order')->get();
            $basic_four_decimals = Service::data($basic_four_decimals);
            $basic_four_decimals_new = [];
            if ($basic_four_decimals){
                foreach ($basic_four_decimals as $bfdk => $bfdv){
                    $basic_four_decimals_new[$bfdv['dim_table_id']] = $bfdv['dim_decimals'];
                }
            }

            $chartx = [];
            $chartList = [];
            foreach($chart_answer as $chart_data){
                $chartx[] = $chart_data['date_time'];

                if ($basic_four_decimals_new){
                    foreach ($basic_four_decimals_new as $bfdnk => $bfdnv){
                        if (isset($chart_data[$bfdnk])){
                            $chart_data[$bfdnk] = $chart_data[$bfdnk] ? round($chart_data[$bfdnk],$bfdnv) : round(0,$bfdnv);
                        }
                    }
                }

                if ($basic_two_decimals_new){

                    foreach ($basic_two_decimals_new as $btdnk => $btdnv){
                        if (isset($chart_data[$btdnk])){
                            $chart_data[$btdnk] = $chart_data[$btdnk] ? round($chart_data[$btdnk],$btdnv) : round(0,$btdnv);
                        }
                    }
                }

                $chartList[] = $chart_data[$column];

            }

            ApiResponseFactory::apiResponse(array('id'=>$customChart,'name'=>$custom_select_arr['dim_name'],'chartList'=>$chartList,'chartx'=>$chartx),[]);
        }


        

    }

    /**
     * 地域分析
     */
    public static function dataAppCountry($params){
        $begintime_1 = time();

        $userid = $_SESSION['erm_data']['guid'];
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 60;
        $get_game = isset($params['app_select']) ? $params['app_select'] : 'all';
        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        $searchWhereInfo  = self::getSearchWhere($userid,$get_game,$params['start_time'],$params['end_time'],$company);
        $where = $searchWhereInfo['searchWhere'];
        $g_by = $searchWhereInfo['g_by'];

        $customChart = $params['custom_chart'];

        // 特殊处理 补丁 91  开发者分成成本。    93 总成本    92 总利润
        if (in_array($customChart,[91,92,93])){
            ApiResponseFactory::apiResponse([],[]);
        }

        $custom_select_arr = self::getAnalyStr($customChart,$currency_type_id);
        $select_dim_str = $custom_select_arr['select_dim_str'];

        // 分区查询
        $partition = '';
        $all_month_arr = Service::dateMonthsSections($params['start_time'],$params['end_time']);
        $all_month = [];
        if ($all_month_arr){
            foreach ($all_month_arr as $month_srt){
                $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
            }
            if ($all_month){
                $partition = " partition (".implode(',',$all_month).")";
            }
        }

        $where .=" and flow_type = 1 and statistics = 0 and b.platform_id != 'ptg31' ";

        $regionSql = "select  ".$select_dim_str.",country_id from zplay_basic_report_daily {$partition} b {$where} group by country_id ".$g_by;

        $region_answer = DB::select($regionSql);
        $region_answer = Service::data($region_answer);

        $basic_two_decimals = DB::table('s_cfg_select_dim')->select(['dim_id','dim_table_id','dim_decimals'])->where(['dim_type'=>1,'dim_decimals' =>2,'currency_type'=>$currency_type_id])->orderBy('dim_order')->get();
        $basic_two_decimals = Service::data($basic_two_decimals);
        $basic_two_decimals_new = [];
        if ($basic_two_decimals){
            foreach ($basic_two_decimals as $btdk => $btdv){
                $basic_two_decimals_new[$btdv['dim_table_id']] = $btdv['dim_decimals'];
            }
        }

        $basic_four_decimals = DB::table('s_cfg_select_dim')->select(['dim_id','dim_table_id','dim_decimals'])->where(['dim_type'=>1,'dim_decimals' =>4,'currency_type'=>$currency_type_id])->orderBy('dim_order')->get();
        $basic_four_decimals = Service::data($basic_four_decimals);
        $basic_four_decimals_new = [];
        if ($basic_four_decimals){
            foreach ($basic_four_decimals as $bfdk => $bfdv){
                $basic_four_decimals_new[$bfdv['dim_table_id']] = $bfdv['dim_decimals'];
            }
        }

        $country_list = DB::table('c_country_standard')->select(['full_name as country_name_e','id'])->where('type',2)->get();
        $country_list = Service::data($country_list);
        $country_ids = [];
        foreach ($country_list as $ck => $cv){
            $country_ids[] = $cv['id'];
        }
        $province_list = DB::table('c_country_standard_province')->select(['province_name as country_name_e','province_id as id'])->where('type',3)->get();
        $province_list = Service::data($province_list);
        $province_ids = [];
        foreach ($province_list as $pk => $pv){
            $province_ids[] = $pv['id'];
        }
        $regionData = [];
        if($region_answer){
            foreach ($region_answer as $region_data){

                if ($basic_four_decimals_new){
                    foreach ($basic_four_decimals_new as $bfdnk => $bfdnv){
                        if (array_key_exists($bfdnk, $region_data)){
                            $region_data[$bfdnk] = $region_data[$bfdnk] ? round($region_data[$bfdnk],$bfdnv) : round(0,$bfdnv);
                        }
                    }
                }

                if ($basic_two_decimals_new){
                    foreach ($basic_two_decimals_new as $btdnk => $btdnv){
                        if (array_key_exists($btdnk, $region_data)){
                            $region_data[$btdnk] = $region_data[$btdnk] ? round($region_data[$btdnk],$btdnv) : round(0,$btdnv);
                        }
                    }
                }

                if ($country_list && $country_ids){
                    if (in_array($region_data['country_id'],$country_ids)){
                        foreach ($country_list as $country){
                            if ($region_data['country_id'] == $country['id']){
                                $regionData['data'][] = array('value'=>$region_data[$custom_select_arr['data_dim_column']],'name'=>$country['country_name_e']);
                            }
                        }
                    }elseif(in_array($region_data['country_id'],$province_ids)){
                        foreach ($province_list as $province){
                            if ($region_data['country_id'] == $province['id']){
                                $regionData['data'][] = array('value'=>$region_data[$custom_select_arr['data_dim_column']],'name'=>$province['country_name_e']);
                            }
                        }
                    }
                }
            }
        }else{
            $regionData['data']= array();
        }

//        $endtime_1 = time();
//
//        $message = '用户ID：'.$userid.'，地域开始时间：'.$begintime_1.',结束时间：'.$endtime_1.',时间差'.($endtime_1-$begintime_1);
//
//        $dir = './storage/country';
//        if (!is_dir($dir)) {
//            mkdir($dir,0777,true);
//        }
//        $logFilename = $dir.'/'.'country.log';
//        //生成日志
//        file_put_contents( $logFilename,$message . "\n\n",FILE_APPEND);

        ApiResponseFactory::apiResponse($regionData,[]);

    }

    /**
     * 构成分析
     */
    public static function dataAppForm($params){

        $begintime_1 = time();
        //游戏权限
        $userid = $_SESSION['erm_data']['guid'];;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 60;
        $get_game = isset($params['app_select']) ? $params['app_select'] : 'all';
        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        $searchWhereInfo  = self::getSearchWhere($userid,$get_game, $params['start_time'],  $params['end_time'],$company);

        $where = $searchWhereInfo['searchWhere'];
        $g_by = $searchWhereInfo['g_by'];

        $customChart = $params['custom_chart'];

        // 特殊处理 补丁 91  开发者分成成本。    93 总成本    92 总利润
        if (in_array($customChart,[91,92,93])){

            $form_data = [
                'inData' => [
                    "id" => "$customChart",
                    "data" => []
                ],
                'outData' => [
                    "id" => "",
                    "data" => []
                ]
            ];

//            $endtime_1 = time();
//
//            $message = '用户ID：'.$userid.'，构成分析开始时间：'.$begintime_1.',结束时间：'.$endtime_1.',时间差'.($endtime_1-$begintime_1);
//
//            $dir = './storage/country';
//            if (!is_dir($dir)) {
//                mkdir($dir,0777,true);
//            }
//            $logFilename = $dir.'/'.'country.log';
//
//            //生成日志
//            file_put_contents( $logFilename,$message . "\n\n",FILE_APPEND);

            ApiResponseFactory::apiResponse($form_data,[]);
        }

        $custom_select_arr = self::getAnalyStr($customChart,$currency_type_id);

        // 分区查询
        $partition = '';
        $all_month_arr = Service::dateMonthsSections($params['start_time'],$params['end_time']);
        $all_month = [];
        if ($all_month_arr){
            foreach ($all_month_arr as $month_srt){
                $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
            }
            if ($all_month){
                $partition = " partition (".implode(',',$all_month).")";
            }
        }

        if($custom_select_arr['homepage_pietype']){
            if ($custom_select_arr['join_search_str']){
                $searchSql = "select b.*, ";
                $searchSql .= $custom_select_arr['join_search_str'];
            }else{
                $searchSql = "select b.* ";
            }

            $searchSql .= " from (select  ".$custom_select_arr['pie_select_dim'];
            if ($custom_select_arr['search_str']){
                $searchSql .= ",".$custom_select_arr['search_str'];
            }else{
                $g_by = ltrim($g_by,' ,');
            }
            $searchSql .= " from zplay_basic_report_daily {$partition} b ";

            $where .=" and flow_type = 1 and statistics = 0 and b.platform_id != 'ptg31' ";

            $searchSql .= $where." group by ".$custom_select_arr['search_str'].$g_by ." order by {$custom_select_arr['data_dim_column']} desc limit 10 )b";

            if ($custom_select_arr['join_str']){
                $searchSql .= $custom_select_arr['join_str'];
            }

            $searchSql .= " where {$custom_select_arr['data_dim_column']} > 0 ";
            
            $formData = DB::select($searchSql);
            $formData = Service::data($formData);

            $basic_two_decimals = DB::table('s_cfg_select_dim')->select(['dim_id','dim_table_id','dim_decimals'])->where(['dim_type'=>1,'dim_decimals' =>2,'currency_type'=>$currency_type_id])->orderBy('dim_order')->get();
            $basic_two_decimals = Service::data($basic_two_decimals);
            $basic_two_decimals_new = [];
            if ($basic_two_decimals){
                foreach ($basic_two_decimals as $btdk => $btdv){
                    $basic_two_decimals_new[$btdv['dim_table_id']] = $btdv['dim_decimals'];
                }
            }

            $basic_four_decimals = DB::table('s_cfg_select_dim')->select(['dim_id','dim_table_id','dim_decimals'])->where(['dim_type'=>1,'dim_decimals' =>4,'currency_type'=>$currency_type_id])->orderBy('dim_order')->get();
            $basic_four_decimals = Service::data($basic_four_decimals);
            $basic_four_decimals_new = [];
            if ($basic_four_decimals){
                foreach ($basic_four_decimals as $bfdk => $bfdv){
                    $basic_four_decimals_new[$bfdv['dim_table_id']] = $bfdv['dim_decimals'];
                }
            }

            $outData = [];
            $inData = [];

            if(!empty($formData)){
                $new_formData = [];
                if ($customChart == 66 || $customChart == 48){
                    $country_list = DB::table('c_country_standard')->select(['full_name as country_name_e','id'])->where('type',2)->get();
                    $country_list = Service::data($country_list);
                    $country_ids = [];
                    foreach ($country_list as $ck => $cv){
                        $country_ids[] = $cv['id'];
                    }
                    $province_list = DB::table('c_country_standard_province')->select(['province_name as country_name_e','province_id as id'])->where('type',3)->get();
                    $province_list = Service::data($province_list);
                    $province_ids = [];
                    foreach ($province_list as $pk => $pv){
                        $province_ids[] = $pv['id'];
                    }

                    foreach ($formData as $fk => $region_data){
                        if ($country_list && $country_ids){
                            if (in_array($region_data['country_id'],$country_ids)){
                                foreach ($country_list as $country){
                                    if ($region_data['country_id'] == $country['id']){
                                        $formData[$fk]['country_id'] = $country['country_name_e'];
                                    }
                                }
                            }elseif(in_array($region_data['country_id'],$province_ids)){
                                foreach ($province_list as $province){
                                    if ($region_data['country_id'] == $province['id']){
                                        $formData[$fk]['country_id'] = $province['country_name_e'];
                                    }
                                }
                            }
                        }
                    }
                }

                if ($formData){

                    foreach ($formData as $data){

                        if ($basic_four_decimals_new){
                            foreach ($basic_four_decimals_new as $bfdnk => $bfdnv){
                                if (array_key_exists($bfdnk, $data)){
                                    $data[$bfdnk] = $data[$bfdnk] ? round($data[$bfdnk],$bfdnv) : round(0,$bfdnv);
                                }
                            }
                        }

                        if ($basic_two_decimals_new){
                            foreach ($basic_two_decimals_new as $btdnk => $btdnv){
                                if (array_key_exists($btdnk, $data)){
                                    $data[$btdnk] = $data[$btdnk] ? round($data[$btdnk],$btdnv) : round(0,$btdnv);
                                }
                            }
                        }

                        if(isset($custom_select_arr['name_column'][1])) {
                            $inData[$data[$custom_select_arr['name_column'][0]]][] = $data;
                            $outData[$data[$custom_select_arr['name_column'][1]]][] = $data;
                        }else{
                            $inData[$data[$custom_select_arr['name_column'][0]]][] = $data;
                        }

                    }

                    // 外圈数据
                    $new_out_data = [];
                    if ($outData){
                        foreach ($outData as $ook => $out_data){
                            $new_out_data[$ook] = [];
                            $new_out_data_ok['value'] = 0;
                            $new_out_data_ok['name'] = '';
                            foreach ($out_data as $o_k => $o_data){
                                if ($ook == $o_data[$custom_select_arr['name_column'][1]]){
                                    $new_out_data_ok['value'] += $o_data[$custom_select_arr['data_dim_column']];
                                    $new_out_data_ok['name'] = $ook ? $ook : '未知平台';
                                    $new_out_data[$ook] = $new_out_data_ok;
                                }
                            }
                        }
                    }


                    // 内圈数据
                    $new_in_data = [];
                    if ($inData){
                        foreach ($inData as $iok => $in_data){
                            $new_in_data[$iok] = [];
                            $new_in_data_ik['value'] = 0;
                            $new_in_data_ik['name'] = '';
                            foreach ($in_data as $o_k => $i_data){
                                if ($iok == $i_data[$custom_select_arr['name_column'][0]]){
                                    $new_in_data_ik['value'] += $i_data[$custom_select_arr['data_dim_column']];
                                    $new_in_data_ik['name'] = $iok ? $iok : '未知平台';
                                    $new_in_data[$iok] = $new_in_data_ik;
                                }
                            }
                        }
                    }
                }
            }

            $data = array();
            $data['inData']['id'] = $custom_select_arr['pie_name_dim_id'][0];
            if(!empty($new_in_data)){
                foreach ($new_in_data as $inD){
                    $data['inData']['data'][] = $inD;
                }
            }else{
                $data['inData']['data'] = array();
            }

            if(!empty($new_out_data)){
                $data['outData']['id'] = $custom_select_arr['pie_name_dim_id'][1];
                foreach ($new_out_data as $outD){
                    $data['outData']['data'][] = $outD;
                }

            }else{
                $data['outData']['id'] = '';
                $data['outData']['data'] = array();
            }

//            $endtime_1 = time();
//
//            $message = '用户ID：'.$userid.'，构成分析开始时间：'.$begintime_1.',结束时间：'.$endtime_1.',时间差'.($endtime_1-$begintime_1);
//
//            $dir = './storage/country';
//            if (!is_dir($dir)) {
//                mkdir($dir,0777,true);
//            }
//            $logFilename = $dir.'/'.'country.log';
//
//            //生成日志
//            file_put_contents( $logFilename,$message . "\n\n",FILE_APPEND);

            ApiResponseFactory::apiResponse($data,[]);
        }else{

//            $endtime_1 = time();
//
//            $message = '用户ID：'.$userid.'，构成分析开始时间：'.$begintime_1.',结束时间：'.$endtime_1.',时间差'.($endtime_1-$begintime_1);
//
//            $dir = './storage/country';
//            if (!is_dir($dir)) {
//                mkdir($dir,0777,true);
//            }
//            $logFilename = $dir.'/'.'country.log';
//            //生成日志
//            file_put_contents( $logFilename,$message . "\n\n",FILE_APPEND);

            ApiResponseFactory::apiResponse(['outData'=>array('data'=>array(),'id'=>''),'inData'=>array('data'=>array(),'id'=>'')],[]);
        }

    }

    // 拼接搜索条件
    private static function getSearchWhere($guid,$get_game,$startTime,$endTime,$company=''){
        //验证用户是否有权限登录
        $map1 = [];
        $map1['id'] = $guid;
        $user_info = UserLogic::Userlist($map1)->get();
        $user_info =Service::data($user_info);
        if(!$user_info) ApiResponseFactory::apiResponse([],[],741);
        //返回用户下用权限列表
        $app_permission = [];
        if($user_info[0]['app_permission'] != -2){
            $app_permission = explode(',', $user_info[0]['app_permission']);
        }
        $game_id_list = '';
        if($app_permission){
            $game_id_list = implode(',',$app_permission);
        }

        $g_by = '';
        $where = " where 1=1 ";

        if($company == 9){
            $where .= " and b.game_creator = 9 ";
        }elseif($company == 1 ){
            $where .= " and b.game_creator != 9 ";
        }

        $where .= " and date_time between '{$startTime}' and '{$endTime}'";

        if($get_game=='all'||$get_game==''){//全部应用
            if($game_id_list)
                $where.=" and b.app_id in($game_id_list)";
        }elseif($get_game=='all_ios'){
            $where.=" and b.os_id = 1";
            if($game_id_list)
                $where.=" and b.app_id in($game_id_list)";
        }elseif($get_game=='all_android'){
            $where.=" and b.os_id = 2";
            if($game_id_list)
                $where.=" and b.app_id in($game_id_list)";
        }elseif($get_game=='all_h5'){
            $where.=" and b.os_id = 3";
            if($game_id_list)
                $where.=" and b.app_id in($game_id_list)";
        }elseif($get_game=='all_amazon'){
            $where.=" and b.os_id = 4";
            if($game_id_list)
                $where.=" and b.app_id in($game_id_list)";
        }else{
            $where.=" and b.app_id = {$get_game}";
            $g_by .= " ,b.app_id ";
        }

        return ['searchWhere' => $where,'g_by' => $g_by];
    }

    //根据指标id获取信息
    private static function getAnalyStr($IndexId,$currency_type_id){
        $dimInfo = DB::table('s_cfg_select_dim')->select(['dim_value','dim_name','homepage_pietype','dim_id','dim_table_id'])->where(["dim_id"=>$IndexId,"currency_type"=>$currency_type_id])->first();
        $dimInfo = Service::data($dimInfo);

        //拼接查询数字段
        $select_dim= $dimInfo['dim_value']." as ".$dimInfo['dim_table_id'];
        $homepage_pietype_arr = $dimInfo['homepage_pietype'] ? explode(';',$dimInfo['homepage_pietype']) : [];
        $pie_select_dim = $dimInfo['dim_value']." as ".$dimInfo['dim_table_id'];
        $pie_data_dim = $dimInfo['dim_table_id'];
        $pie_search_str = '';
        $join_search_str = '';
        $table_str = '';
        $name_column = array();
        $dim_name_id_arr = [];
        foreach ($homepage_pietype_arr as $pie_arr){
            $pie_str = explode(',', $pie_arr);

            if ($IndexId == 9 || $IndexId == 11 || $IndexId == 12 || $IndexId == 10){
                $pie_search_str.= "b.".$pie_str[3].",";
                $join_search_str.= "$pie_str[1].".$pie_str[2].",";
                $table_str.= " left join ".$pie_str[1]." $pie_str[1] on $pie_str[1].{$pie_str[0]}=b.{$pie_str[3]}";
                $name_column []=  $pie_str[2];

                $dim_name_id_info = DB::table('s_cfg_select_dim')->select(['dim_id','dim_value'])->where(["dim_value"=>$pie_str[3],"currency_type"=>$currency_type_id])->first();
                $dim_name_id_info = Service::data($dim_name_id_info);
                $dim_name_id_arr[] = $dim_name_id_info['dim_id'];

            }elseif($IndexId != 66 && $IndexId != 48){
                $pie_search_str.= "b.".$pie_str[0].",";
                $join_search_str.= "$pie_str[1].".$pie_str[2].",";

                if ($pie_str[1] == 'c_platform'){
                    $table_str.= " left join ( select distinct platform_id,platform_name from c_platform ) "." $pie_str[1] on $pie_str[1].{$pie_str[0]}=b.{$pie_str[0]}";
                }else{
                    $table_str.= " left join ".$pie_str[1]." $pie_str[1] on $pie_str[1].{$pie_str[0]}=b.{$pie_str[0]}";
                }
                $name_column []=  $pie_str[2];

                $dim_name_id_info = DB::table('s_cfg_select_dim')->select(['dim_id','dim_value'])->where(["base_sel_name"=>$pie_str[2],"currency_type"=>$currency_type_id])->first();
                $dim_name_id_info = Service::data($dim_name_id_info);
                $dim_name_id_arr[] = $dim_name_id_info['dim_id'];

            }else{
                $pie_search_str .= 'b.country_id';
                $name_column []=  $pie_str[0];
                $dim_name_id_info = DB::table('s_cfg_select_dim')->select(['dim_id','dim_value'])->where(["dim_value"=>$pie_str[0],"currency_type"=>$currency_type_id])->first();
                $dim_name_id_info = Service::data($dim_name_id_info);
                $dim_name_id_arr[] = $dim_name_id_info['dim_id'];
            }

        }
        //是否有构成
        if(!$dimInfo['homepage_pietype']){
            $homepage_pietype = 0;
        }else{
            $homepage_pietype = 1;
        }
        $pie_search_arr = [
            'select_dim_str'=>$select_dim,
            'data_dim_column'=>$pie_data_dim,
            'dim_name'=>$dimInfo['dim_name'],
            'search_str'=>rtrim($pie_search_str,','),
            'join_search_str'=>rtrim($join_search_str,','),
            'join_str'=>$table_str,
            'name_column'=>$name_column,
            'pie_select_dim'=>$pie_select_dim,
            'pie_name_dim_id'=>$dim_name_id_arr,
            'homepage_pietype'=>$homepage_pietype
        ];

        return $pie_search_arr;
    }

    /**
     * 指标排序保存
     */
    public static function dataCustomTarget($params){

        //验证用户是否有权限登录
        $userid = $_SESSION['erm_data']['guid'];;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }

        $custom_sort = isset($params['custom_sort']) ? $params['custom_sort'] : '';
        $custom_app = isset($params['custom_app']) ? $params['custom_app'] : '';
        $custom_chart = isset($params['custom_chart']) ? $params['custom_chart'] : '';

        if(!$custom_sort && !$custom_app && !$custom_chart){
            ApiResponseFactory::apiResponse([],[],752);
        }


        if($custom_sort){
            $sql = "insert into s_homepage_custom (user_id,update_time,custom_sort) values($userid,now(),'{$custom_sort}') on DUPLICATE  key update custom_sort='{$custom_sort}',update_time=now()";


        }
        if($custom_app){
            $sql = "insert into s_homepage_custom (user_id,update_time,custom_app) values($userid,now(),'{$custom_app}') on DUPLICATE  key update custom_app='{$custom_app}',update_time=now()";

        }

        if($custom_chart){
            $sql = "insert into s_homepage_custom (user_id,update_time,custom_chart) values($userid,now(),'{$custom_chart}') on DUPLICATE  key update custom_chart='{$custom_chart}',update_time=now()";
        }

        if(!DB::insert($sql))
            ApiResponseFactory::apiResponse([],[],753);

        ApiResponseFactory::apiResponse([],[]);
    }

    // 推广数据初始化
    public static function dataPromotionInit($params){
        //验证用户是否有权限登录
        $userid = $_SESSION['erm_data']['guid'];;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        //验证用户是否有权限登录
        $map1 = [];
        $map1['id'] = $userid;
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        //返回用户下用权限列表
        $app_permission = [];
        if($userInfo[0]['app_permission'] != -2){
            $app_permission = explode(',', $userInfo[0]['app_permission']);
        }



        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 60;
        $basic_tg_table = 'zplay_basic_tg_report_total';
        if ($currency_type_id == 60){
            $basic_tg_table = 'zplay_basic_tg_report_total_usd';
        }
        $data = [];
        // 查询条件
        $get_game = isset($params['app_select']) ? $params['app_select'] : '';
        $os_id = isset($params['os_id']) ? $params['os_id'] : '';

        $where = " where 1=1 ";
        if($get_game){
            $where .= " and b.app_id  = {$get_game}";
        }elseif($app_permission){
            $game_id_list = implode(',',$app_permission);
            $where .= " and b.app_id in($game_id_list)";
        }

        $country_id = isset($params['country_id']) ? $params['country_id'] : '';
        if ($country_id){
            $where .= " and b.country_id  = {$country_id}";
        }

        if ($os_id){
            $where .= " and b.os_id  = {$os_id}";
        }

        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        if($company == 9){
            $where .= " and b.company_id = 9 ";
        }elseif($company == 1 ){
            $where .= " and b.company_id != 9 ";
        }

        // 分组
        $group_by = '';
        // 当前时间段 查询
        $start_time = isset($params['start_time']) ? $params['start_time'] : date('Y-m-d',strtotime("-7 days"));
        $end_time = isset($params['end_time']) ? $params['end_time'] : date('Y-m-d');

        $target_time_arr = self::getTargetTimeWhere($start_time,$end_time);

        $data_where = $target_time_arr['where'];
        $partition = $target_time_arr['partition'];

        $sql = "select sum(new) as add_user_total,sum(new_nonature) as add_user_unnatural ,sum(new_nature) as add_user_natural , sum(cost) as cost_total, sum(earning_all) as income_total from {$basic_tg_table} {$partition} b".$where.$data_where.$group_by;

        $new_total_data = Db::select($sql);
        $new_total_data = Service::data($new_total_data);

        // 上一个环比时间段 查询
        $time_period = Service::timePeriod($start_time,$end_time);
        if ($start_time == $end_time){
            $time_period = $time_period + 1;
        }
        $start_time = date('Y-m-d',strtotime("{$start_time} -{$time_period} days"));
        $end_time = date('Y-m-d',strtotime("{$end_time} -{$time_period} days"));

        $target_time_arr = self::getTargetTimeWhere($start_time,$end_time);

        $data_where = $target_time_arr['where'];
        $partition = $target_time_arr['partition'];

        $sql = "select sum(new) as add_user_total,sum(new_nonature) as add_user_unnatural ,sum(new_nature) as add_user_natural , sum(cost) as cost_total, sum(earning_all) as income_total from {$basic_tg_table} {$partition} b".$where.$data_where.$group_by;

        $old_total_data = Db::select($sql);
        $old_total_data = Service::data($old_total_data);

        $return_total_data = [];
        $return_total_data['add_user_total']['data_count'] =  $new_total_data[0]['add_user_total'] ? $new_total_data[0]['add_user_total'] : 0;
        $return_total_data['add_user_total']['ratio'] = intval($old_total_data[0]['add_user_total']) ? round(((($new_total_data[0]['add_user_total'] - $old_total_data[0]['add_user_total']) / $old_total_data[0]['add_user_total'])*100),2)."%" : "0.00%";
        $return_total_data['add_user_unnatural']['data_count'] =  $new_total_data[0]['add_user_unnatural'] ? $new_total_data[0]['add_user_unnatural'] : 0;
        $return_total_data['add_user_unnatural']['ratio'] = intval($old_total_data[0]['add_user_unnatural']) ? round(((($new_total_data[0]['add_user_unnatural'] - $old_total_data[0]['add_user_unnatural']) / $old_total_data[0]['add_user_unnatural'])*100),2)."%" : "0.00%";
        $return_total_data['cost_total']['data_count'] =  round($new_total_data[0]['cost_total'],0);
        $return_total_data['cost_total']['ratio'] =  intval($old_total_data[0]['cost_total']) ? round(((($new_total_data[0]['cost_total'] - $old_total_data[0]['cost_total']) / $old_total_data[0]['cost_total'])*100),2)."%" : "0.00%";
        $return_total_data['income_total']['data_count'] =  round($new_total_data[0]['income_total'],0);
        $return_total_data['income_total']['ratio'] = intval($old_total_data[0]['income_total']) ? round(((($new_total_data[0]['income_total'] - $old_total_data[0]['income_total']) / $old_total_data[0]['income_total'])*100),2)."%" : "0.00%";
        $new_user_natural =  $new_total_data[0]['add_user_total']- $new_total_data[0]['add_user_unnatural'];
        $old_user_natural =  $old_total_data[0]['add_user_total']- $old_total_data[0]['add_user_unnatural'];
        $return_total_data['add_user_natural']['data_count'] =$new_user_natural;

        $return_total_data['add_user_natural']['ratio'] =  $old_user_natural ? round(((($new_user_natural - $old_user_natural) / $old_user_natural)*100),2)."%" : "0.00%";

        $data['total_list'] = $return_total_data;

        ApiResponseFactory::apiResponse($return_total_data,[]);
    }

    // 根据时间段匹配where
    private static function getTargetTimeWhere($start_time,$end_time){
        $target_time_arr = [];


        // 分区查询
        $partition = '';
        $all_month_arr = Service::dateMonthsSections($start_time,$end_time);
        $all_month = [];
        if ($all_month_arr){
            foreach ($all_month_arr as $month_srt){
                $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
            }
            if ($all_month){
                $partition = " partition (".implode(',',$all_month).")";
            }
        }

        $where = " and date between '{$start_time}' and '{$end_time}'";

        $target_time_arr['partition'] = $partition;
        $target_time_arr['where'] = $where;

        return $target_time_arr;
    }

    // 总体情况增长量图
    public static function dataPromotionGrowth($params){
        //验证用户是否有权限登录
        $userid = $_SESSION['erm_data']['guid'];;
//        $userid = 2;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        //验证用户是否有权限登录
        $map1 = [];
        $map1['id'] = $userid;
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        //返回用户下用权限列表
        $app_permission = [];
        if($userInfo[0]['app_permission'] != -2){
            $app_permission = explode(',', $userInfo[0]['app_permission']);
        }

        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 60;

        // 查询条件
        $get_game = isset($params['app_select']) ? $params['app_select'] : '';
        $os_id = isset($params['os_id']) ? $params['os_id'] : '';
        $type = isset($params['type']) ? $params['type'] : '';
        // 分组
        $group_by = '';
        $where = " where 1=1 ";
        if($get_game){
            $where .= " and b.app_id  = {$get_game}";
        }elseif($app_permission){
            $game_id_list = implode(',',$app_permission);
            $where .= " and b.app_id in($game_id_list)";
        }

        if ($os_id){
            $where .= " and b.os_id  = {$os_id}";
        }

        $country_id = isset($params['country_id']) ? $params['country_id'] : '';
        if ($country_id){
            $where .= " and b.country_id  = {$country_id}";
        }

        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        if($company == 9){
            $where .= " and b.company_id = 9 ";
        }elseif($company == 1 ){
            $where .= " and b.company_id != 9 ";
        }
        // 筛选数据维度
        $matching_name = [];
        $search_field = '';
        $key_field = '';
        $tg_platform_table = '';
        if ($type == 4){
            $group_by .= " group by country_id";
            $search_field = ' ,country_id';
            $key_field = 'country_id';
            $country_arr = DB::table('c_country_standard')->select(['full_name as country_name_e','id'])->where('type',2)->get();
            $country_arr = Service::data($country_arr);

            foreach ($country_arr as $ck => $cv){
                $matching_name[$cv['id']] = $cv['country_name_e'];
            }

        }elseif($type == 58){
            $group_by .= " group by platform_id";
            $search_field = ' ,platform_id';
            $key_field = 'platform_id';
            $country_arr = DB::table('c_platform')->select(['platform_name','platform_id'])->where('platform_type_id',4)->get();
            $country_arr = Service::data($country_arr);

            foreach ($country_arr as $ck => $cv){
                $matching_name[$cv['platform_id']] = $cv['platform_name'];
            }

            $tg_platform_table = 'zplay_basic_tg_plat_report_total';
            if ($currency_type_id == 60){
                $tg_platform_table = 'zplay_basic_tg_plat_report_total_usd';
            }
        }

        $total_data = [];
        // 当前时间段 查询
        $start_time = isset($params['start_time']) ? $params['start_time'] : date('Y-m-d',strtotime("-7 days"));
        $end_time = isset($params['end_time']) ? $params['end_time'] : date('Y-m-d');

        // 分区查询
        $partition = '';
        $all_month_arr = Service::dateMonthsSections($start_time,$end_time);
        $all_month = [];


        // 日期相差时间段
        $time_period = Service::timePeriod($start_time,$end_time);
        $date_target_arr = []; // 日期时间段

//        var_dump($group_by);die;
        if ($time_period <= 31){
            // 一个月之内
            if ($all_month_arr){
                foreach ($all_month_arr as $month_srt){
                    $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
                }
                if ($all_month){
                    $partition = " partition (".implode(',',$all_month).")";
                }
            }
            if ($group_by)
                $group_by .= ' ,date';
            else $group_by = ' group by date';
            $group_all_by = ' group by date';
            $date_time_column = "  ,date as date_time";

            for ($d = 0; $d <= $time_period; $d++){
                $date_target_arr[date('Y-m-d',strtotime("{$start_time} +$d days"))] = [];
            }

        }elseif (count($all_month_arr) <= 12){
            // 6个月之内 按 15天分区
            if ($all_month_arr){
                foreach ($all_month_arr as $month_srt){
                    $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
                }
                if ($all_month){
                    $partition = " partition (".implode(',',$all_month).")";
                }
            }
            if ($group_by)
                $group_by .= ' ,SUBSTR(date, 1,7)';
            else $group_by = ' group by SUBSTR(date, 1,7)';
            $group_all_by = ' group by SUBSTR(date, 1,7)';
            $date_time_column = "  ,SUBSTR(date, 1,7 ) as date_time";

            for ($m = 0;$m < count($all_month_arr); $m++){
                $date_target_arr[date('Y-m',strtotime("{$start_time} +$m month"))] = [];
            }


        }else{
            // 超过1年
            if ($group_by)
                $group_by .= ' ,SUBSTR(date, 1,4)';
            else $group_by = ' group by SUBSTR(date, 1,4)';

            $group_all_by = ' group by SUBSTR(date, 1,4)';

            $date_time_column = "  ,SUBSTR(date, 1,4 ) as date_time";

            $start_year = date('Y',strtotime($start_time));
            $end_year = date('Y',strtotime($end_time));
            $year_period = $end_year - $start_year;
            for ($y = 0;$y < $year_period; $y++){
                $date_target_arr[date('Y',strtotime("{$start_time} +$y year"))] = [];
            }

        }

        $where .= " and date between '{$start_time}' and '{$end_time}'";

        $chartx = [];
        $chartList = [];
        $return_data = [];

        if ($type == 4 || !$type){

            $tg_platform_table_new = 'zplay_basic_tg_report_total';
            if ($currency_type_id == 60){
                $tg_platform_table_new = 'zplay_basic_tg_report_total_usd';
            }

            $sql = "select sum(new) as add_user_total,sum(active) as add_active_total,sum(new_nonature) as add_user_unnatural,sum(new_nature) as add_user_natural {$date_time_column} {$search_field} from {$tg_platform_table_new} {$partition} b".$where.$group_by;

            $total_data = Db::select($sql);
            $total_data = Service::data($total_data);

            $new_return_data = [];

            if ($total_data){
                if (!$type){
                    foreach ($total_data as $key => $data){
                        $total_data[$key]['add_user_total'] = intval($data['add_user_total']);
                        $total_data[$key]['add_active_total'] = intval($data['add_active_total']);
                        $total_data[$key]['add_user_unnatural'] = intval($data['add_user_unnatural']);
                        $total_data[$key]['add_user_natural'] = intval($data['add_user_total'])-intval($data['add_user_natural']);
                    }

                    foreach($total_data as $chart_data) {
                        if (isset($date_target_arr[$chart_data['date_time']])) {
                            $chartx[] = $chart_data['date_time'];
                            $chartList['add_natural'][] =$chart_data['add_user_total']- $chart_data['add_user_unnatural'];
                            $chartList['add_unnatural'][] = $chart_data['add_user_unnatural'];
                            $chartList['add_active'][] = $chart_data['add_active_total'];
                            $chartList['add_total'][] = $chart_data['add_user_total'];
                        } else {
                            $chartx[] = $chart_data['date_time'];
                            $chartList['add_natural'][] = 0;
                            $chartList['add_unnatural'][] = 0;
                            $chartList['add_active'][] = 0;
                            $chartList['add_total'][] = 0;
                        }
                    }

                }else{
                    foreach ($total_data as $key => $data){
                        $data['add_user_total'] = intval($data['add_user_total']);
                        $data['add_active_total'] = intval($data['add_active_total']);
                        $data['add_user_unnatural'] = intval($data['add_user_unnatural']);
                        $data['add_user_natural'] =intval($data['add_user_total'])- intval($data['add_user_unnatural']);

                        if ($matching_name && $key_field){
                            foreach ($matching_name as $m_key => $m_value){
                                if ($data[$key_field] == $m_key){
                                    $data[$key_field] = $m_value;
                                }
                            }
                        }
                        $total_data[$key] = $data;
                    }

                    foreach ($date_target_arr as $dtaak => $dtaav){
                        $chartx[] = $dtaak;
                    }

                    foreach ($total_data as $tk => $tv){
                        foreach ($date_target_arr as $dtak => $dtav){
                            $new_return_data[$tv[$key_field]][$dtak]['add_user_total'] = 0;
                            $new_return_data[$tv[$key_field]][$dtak]['add_user_unnatural'] = 0;
                            $new_return_data[$tv[$key_field]][$dtak]['add_user_natural'] = 0;
                            $new_return_data[$tv[$key_field]][$dtak]['add_active_total'] = 0;
                        }
                    }

                    foreach ($total_data as $tk => $tv){
                        foreach ($new_return_data as $nrdk => $nrdv){
                            if ($nrdk == $tv[$key_field]) {
                                foreach ($nrdv as $nrdvk => $nrdvv){
                                    if ($tv['date_time'] == $nrdvk){
                                        $new_return_data[$nrdk][$nrdvk]['add_user_total'] = $tv['add_user_total'];
                                        $new_return_data[$nrdk][$nrdvk]['add_active_total'] = $tv['add_active_total'];
                                        $new_return_data[$nrdk][$nrdvk]['add_user_unnatural'] = $tv['add_user_unnatural'];
                                        $new_return_data[$nrdk][$nrdvk]['add_user_natural'] = $tv['add_user_total']-$tv['add_user_unnatural'];
                                    }
                                }
                            }
                        }
                    }


                    if ($type == 58){
                        foreach($new_return_data as $nrdk => $chart_data) {
                            foreach ($date_target_arr as $dtak => $dtav){
                                $chartList['plat_account'][$nrdk][] = $chart_data[$dtak]['add_user_unnatural'];
                            }
                        }
                    }elseif($type == 4){
                        foreach($new_return_data as $nrdk => $chart_data) {
                            foreach ($date_target_arr as $dtak => $dtav){
                                $chartList['country_account'][$nrdk][] = $chart_data[$dtak]['add_user_unnatural'];
                            }
                        }
                    }


                    if (isset($chartList['country_account'])){
                        foreach ($chartList['country_account'] as $k_k => $k_v){
                            if (array_sum($k_v) == 0){
                                unset($chartList['country_account'][$k_k]);
                            }
                        }
                    }

                    foreach ($date_target_arr as $dtak => $dtav){
                        $add_total = 0;
                        $add_natural = 0;
                        foreach($new_return_data as $nrdk => $chart_data) {
                            foreach ($chart_data as $cdk => $cdv){
                                if ($cdk == $dtak){
                                    $add_total += $cdv['add_user_total'];
                                    $add_natural += $cdv['add_user_natural'];
                                }
                            }
                        }
                        $chartList['add_natural'][] = $add_natural;
                        $chartList['add_total'][] = $add_total;
                    }
                }
            }
        }elseif($type == 58){
            //  获取平台总非自然增长量
            $sql = "select sum(new) as add_user_unnatural {$date_time_column} {$search_field} from {$tg_platform_table} {$partition} b".$where.$group_by;
            
            $platform_new_total = Db::select($sql);
            $platform_new_total = Service::data($platform_new_total);

            //  获取平台总自然增长量 总增长量
            $tg_platform_table_new = 'zplay_basic_tg_report_total';
            if ($currency_type_id == 60){
                $tg_platform_table_new = 'zplay_basic_tg_report_total_usd';
            }
            $sql = "select sum(new) as add_user_total,sum(new_nature) as add_user_natural {$date_time_column} from {$tg_platform_table_new} {$partition} b".$where.$group_all_by;

            $total_data = Db::select($sql);
            $total_data = Service::data($total_data);

            $new_return_data = [];

            if ($total_data){

                // chartx
                foreach ($date_target_arr as $dtaak => $dtaav){
                    $chartx[] = $dtaak;
                }
                // chartlist total natural
                foreach ($total_data as $key => $data){
                    $total_data[$key]['add_user_total'] = intval($data['add_user_total']);
                    $total_data[$key]['add_user_natural'] = intval($data['add_user_natural']);
                }

                foreach($total_data as $chart_data) {
                    foreach ($date_target_arr as $dtak => $dtav){
                        if ($dtak == $chart_data['date_time']) {
                            $chartList['add_natural'][] = $chart_data['add_user_natural'];
                            $chartList['add_total'][] = $chart_data['add_user_total'];
                        } else {
                            $chartList['add_natural'][] = 0;
                            $chartList['add_total'][] = 0;
                        }
                    }

                }

                // chartlist platform unnatural

                foreach ($platform_new_total as $key => $data){
                    $data['add_user_unnatural'] = intval($data['add_user_unnatural']);

                    if ($matching_name && $key_field){
                        foreach ($matching_name as $m_key => $m_value){
                            if ($data[$key_field] == $m_key){
                                $data[$key_field] = $m_value;
                            }
                        }
                    }
                    $platform_new_total[$key] = $data;
                }

                foreach ($platform_new_total as $tk => $tv){
                    foreach ($date_target_arr as $dtak => $dtav){
                        $new_return_data[$tv[$key_field]][$dtak]['add_user_unnatural'] = 0;
                    }
                }

                foreach ($platform_new_total as $tk => $tv){
                    foreach ($new_return_data as $nrdk => $nrdv){
                        if ($nrdk == $tv[$key_field]) {
                            foreach ($nrdv as $nrdvk => $nrdvv){
                                if ($tv['date_time'] == $nrdvk){
                                    $new_return_data[$nrdk][$nrdvk]['add_user_unnatural'] = $tv['add_user_unnatural'];
                                }
                            }
                        }
                    }
                }


                foreach($new_return_data as $nrdk => $chart_data) {
                    foreach ($date_target_arr as $dtak => $dtav){
                        $chartList['plat_account'][$nrdk][] = $chart_data[$dtak]['add_user_unnatural'];
                    }
                }

            }
        }


        $return_data['chartx'] = $chartx;
        $return_data['chart_list'] = $chartList;

        ApiResponseFactory::apiResponse($return_data,[]);

    }

    // 总体情况成本收入图
    public static function dataPromotionIncome($params){
        //验证用户是否有权限登录
        $userid = $_SESSION['erm_data']['guid'];;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        //验证用户是否有权限登录
        $map1 = [];
        $map1['id'] = $userid;
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        //返回用户下用权限列表
        $app_permission = [];
        if($userInfo[0]['app_permission'] != -2){
            $app_permission = explode(',', $userInfo[0]['app_permission']);
        }

        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 60;

        // 查询条件
        $get_game = isset($params['app_select']) ? $params['app_select'] : '';
        $os_id = isset($params['os_id']) ? $params['os_id'] : '';
        $type = isset($params['type']) ? $params['type'] : '';
        // 分组
        $group_by = '';
        $where = " where 1=1 ";
        if($get_game){
            $where .= " and b.app_id  = {$get_game}";
        }elseif($app_permission){
            $game_id_list = implode(',',$app_permission);
            $where .= " and b.app_id in($game_id_list)";
        }

        $country_id = isset($params['country_id']) ? $params['country_id'] : '';
        if ($country_id){
            $where .= " and b.country_id  = {$country_id}";
        }

        if ($os_id){
            $where .= " and b.os_id  = {$os_id}";
        }

        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        if($company == 9){
            $where .= " and b.company_id = 9 ";
        }elseif($company == 1 ){
            $where .= " and b.company_id != 9 ";
        }
        // 筛选数据维度
        $matching_name = [];
        $search_field = '';
        $key_field = '';
        $tg_platform_table = '';
        if ($type == 4){
            $group_by .= " group by country_id";
            $search_field = ' ,country_id';
            $key_field = 'country_id';
            $country_arr = DB::table('c_country_standard')->select(['full_name as country_name_e','id'])->where('type',2)->get();
            $country_arr = Service::data($country_arr);

            foreach ($country_arr as $ck => $cv){
                $matching_name[$cv['id']] = $cv['country_name_e'];
            }


        }elseif($type == 58){
            $group_by .= " group by platform_id";
            $search_field = ' ,platform_id';
            $key_field = 'platform_id';
            $country_arr = DB::table('c_platform')->select(['platform_name','platform_id'])->where('platform_type_id',4)->get();
            $country_arr = Service::data($country_arr);

            foreach ($country_arr as $ck => $cv){
                $matching_name[$cv['platform_id']] = $cv['platform_name'];
            }

            $tg_platform_table = 'zplay_basic_tg_plat_report_total';
            if ($currency_type_id == 60){
                $tg_platform_table = 'zplay_basic_tg_plat_report_total_usd';
            }
        }

        $total_data = [];
        // 当前时间段 查询
        $start_time = isset($params['start_time']) ? $params['start_time'] : date('Y-m-d',strtotime("-7 days"));
        $end_time = isset($params['end_time']) ? $params['end_time'] : date('Y-m-d');

        // 分区查询
        $partition = '';
        $all_month_arr = Service::dateMonthsSections($start_time,$end_time);
        $all_month = [];

        // 日期相差时间段
        $time_period = Service::timePeriod($start_time,$end_time);
        $date_target_arr = []; // 日期时间段
        if ($time_period <= 31){
            // 一个月之内
            if ($all_month_arr){
                foreach ($all_month_arr as $month_srt){
                    $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
                }
                if ($all_month){
                    $partition = " partition (".implode(',',$all_month).")";
                }
            }
            if ($group_by)
                $group_by .= ' ,date';
            else $group_by = ' group by date';
            $group_all_by = ' group by date';
            $date_time_column = "  ,date as date_time";

            for ($d = 0; $d <= $time_period; $d++){
                $date_target_arr[date('Y-m-d',strtotime("{$start_time} +$d days"))] = [];
            }

        }elseif (count($all_month_arr) <= 12){
            // 6个月之内 按 15天分区
            if ($all_month_arr){
                foreach ($all_month_arr as $month_srt){
                    $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
                }
                if ($all_month){
                    $partition = " partition (".implode(',',$all_month).")";
                }
            }
            if ($group_by)
                $group_by .= ' ,SUBSTR(date, 1,7)';
            else $group_by = ' group by SUBSTR(date, 1,7)';
            $group_all_by = ' group by SUBSTR(date, 1,7)';
            $date_time_column = "  ,SUBSTR(date, 1,7 ) as date_time";

            for ($m = 0;$m < count($all_month_arr); $m++){
                $date_target_arr[date('Y-m',strtotime("{$start_time} +$m month"))] = [];
            }


        }else{
            // 超过1年
            if ($group_by)
                $group_by .= ' ,SUBSTR(date, 1,4)';
            else $group_by = ' group by SUBSTR(date, 1,4)';
            $group_all_by = ' group by SUBSTR(date, 1,4)';
            $date_time_column = "  ,SUBSTR(date, 1,4 ) as date_time";

            $start_year = date('Y',strtotime($start_time));
            $end_year = date('Y',strtotime($end_time));
            $year_period = $end_year - $start_year;
            for ($y = 0;$y < $year_period; $y++){
                $date_target_arr[date('Y',strtotime("{$start_time} +$y year"))] = [];
            }

        }

        $where .= " and date between '{$start_time}' and '{$end_time}'";

        $chartx = [];
        $chartList = [];

        if ($type == 4 || !$type){

            $tg_platform_table_new = 'zplay_basic_tg_report_total';
            if ($currency_type_id == 60){
                $tg_platform_table_new = 'zplay_basic_tg_report_total_usd';
            }

            $sql = "select sum(cost) as cost,sum(earning_all) as income {$date_time_column} {$search_field} from {$tg_platform_table_new} {$partition} b".$where.$group_by;

            $total_data = Db::select($sql);
            $total_data = Service::data($total_data);


            $return_data = [];
            $new_return_data = [];

            if ($total_data){
                if (!$type){
                    foreach ($total_data as $key => $data){
                        $total_data[$key]['cost'] = intval($data['cost']);
                        $total_data[$key]['income'] = intval($data['income']);
                    }

                    foreach($total_data as $chart_data) {
                        if (isset($date_target_arr[$chart_data['date_time']])) {
                            $chartx[] = $chart_data['date_time'];
                            $chartList['cost'][] = $chart_data['cost'];
                            $chartList['income'][] = $chart_data['income'];
                        } else {
                            $chartx[] = $chart_data['date_time'];
                            $chartList['cost'][] = 0;
                            $chartList['income'][] = 0;
                        }
                    }

                }else{
                    foreach ($total_data as $key => $data){
                        $data['cost'] = intval($data['cost']);
                        $data['income'] = intval($data['income']);

                        if ($matching_name && $key_field){
                            foreach ($matching_name as $m_key => $m_value){
                                if ($data[$key_field] == $m_key){
                                    $data[$key_field] = $m_value;
                                }
                            }
                        }
                        $total_data[$key] = $data;
                    }

                    foreach ($date_target_arr as $dtaak => $dtaav){
                        $chartx[] = $dtaak;
                    }

                    foreach ($total_data as $tk => $tv){
                        foreach ($date_target_arr as $dtak => $dtav){
                            $new_return_data[$tv[$key_field]][$dtak]['cost'] = 0;
                            $new_return_data[$tv[$key_field]][$dtak]['income'] = 0;
                        }
                    }

                    foreach ($total_data as $tk => $tv){
                        foreach ($new_return_data as $nrdk => $nrdv){
                            if ($nrdk == $tv[$key_field]) {
                                foreach ($nrdv as $nrdvk => $nrdvv){
                                    if ($tv['date_time'] == $nrdvk){
                                        $new_return_data[$nrdk][$nrdvk]['cost'] = $tv['cost'];
                                        $new_return_data[$nrdk][$nrdvk]['income'] = $tv['income'];
                                    }
                                }
                            }
                        }
                    }


                    if ($type == 58){
                        foreach($new_return_data as $nrdk => $chart_data) {
                            foreach ($date_target_arr as $dtak => $dtav){
                                $chartList['plat_cost'][$nrdk][] = $chart_data[$dtak]['cost'];
                            }
                        }
                    }elseif($type == 4){
                        foreach($new_return_data as $nrdk => $chart_data) {
                            foreach ($date_target_arr as $dtak => $dtav){
                                $chartList['country_cost'][$nrdk][] = $chart_data[$dtak]['cost'];
                            }
                        }
                    }

                    if (isset($chartList['country_cost'])){
                        foreach ($chartList['country_cost'] as $k_k => $k_v){
                            if (array_sum($k_v) == 0){
                                unset($chartList['country_cost'][$k_k]);
                            }
                        }
                    }

                    foreach ($date_target_arr as $dtak => $dtav){
                        $add_total = 0;
                        foreach($new_return_data as $nrdk => $chart_data) {
                            foreach ($chart_data as $cdk => $cdv){
                                if ($cdk == $dtak){
                                    $add_total += $cdv['income'];
                                }
                            }
                        }
                        $chartList['income'][] = $add_total;
                    }
                }
            }
        }elseif($type == 58){
            //  获取平台总非自然增长量
            $sql = "select sum(cost) as cost {$date_time_column} {$search_field} from {$tg_platform_table} {$partition} b".$where.$group_by;

            $platform_new_total = Db::select($sql);
            $platform_new_total = Service::data($platform_new_total);

            $tg_platform_table_new = 'zplay_basic_tg_report_total';
            if ($currency_type_id == 60){
                $tg_platform_table_new = 'zplay_basic_tg_report_total_usd';
            }
            //  获取平台总自然增长量 总增长量
            $sql = "select sum(earning_all) as income {$date_time_column} from {$tg_platform_table_new} {$partition} b".$where.$group_all_by;

            $total_data = Db::select($sql);
            $total_data = Service::data($total_data);

            $new_return_data = [];

            if ($total_data){

                // chartx
                foreach ($date_target_arr as $dtaak => $dtaav){
                    $chartx[] = $dtaak;
                }
                // chartlist total natural
                foreach ($total_data as $key => $data){
                    $total_data[$key]['income'] = intval($data['income']);
                }

                foreach($total_data as $chart_data) {
                    foreach ($date_target_arr as $dtak => $dtav){
                        if ($dtak == $chart_data['date_time']) {
                            $chartList['income'][] = $chart_data['income'];
                        } else {
                            $chartList['income'][] = 0;
                        }
                    }

                }

                // chartlist platform unnatural

                foreach ($platform_new_total as $key => $data){
                    $data['cost'] = intval($data['cost']);

                    if ($matching_name && $key_field){
                        foreach ($matching_name as $m_key => $m_value){
                            if ($data[$key_field] == $m_key){
                                $data[$key_field] = $m_value;
                            }
                        }
                    }
                    $platform_new_total[$key] = $data;
                }

                foreach ($platform_new_total as $tk => $tv){
                    foreach ($date_target_arr as $dtak => $dtav){
                        $new_return_data[$tv[$key_field]][$dtak]['cost'] = 0;
                    }
                }

                foreach ($platform_new_total as $tk => $tv){
                    foreach ($new_return_data as $nrdk => $nrdv){
                        if ($nrdk == $tv[$key_field]) {
                            foreach ($nrdv as $nrdvk => $nrdvv){
                                if ($tv['date_time'] == $nrdvk){
                                    $new_return_data[$nrdk][$nrdvk]['cost'] = $tv['cost'];
                                }
                            }
                        }
                    }
                }


                foreach($new_return_data as $nrdk => $chart_data) {
                    foreach ($date_target_arr as $dtak => $dtav){
                        $chartList['plat_cost'][$nrdk][] = $chart_data[$dtak]['cost'];
                    }
                }

            }
        }

        $return_data['chartx'] = $chartx;
        $return_data['chart_list'] = $chartList;

        ApiResponseFactory::apiResponse($return_data,[]);

    }

    // 总体情况CPI
    public static function dataPromotionCpi($params){
        //验证用户是否有权限登录
        $userid = $_SESSION['erm_data']['guid'];;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        //验证用户是否有权限登录
        $map1 = [];
        $map1['id'] = $userid;
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        //返回用户下用权限列表
        $app_permission = [];
        if($userInfo[0]['app_permission'] != -2){
            $app_permission = explode(',', $userInfo[0]['app_permission']);
        }

        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 60;

        // 查询条件
        $get_game = isset($params['app_select']) ? $params['app_select'] : '';
        $os_id = isset($params['os_id']) ? $params['os_id'] : '';
        $type = isset($params['type']) ? $params['type'] : '';
        // 分组
        $group_by = '';
        $where = " where 1=1 ";
        if($get_game){
            $where .= " and b.app_id  = {$get_game}";
        }elseif($app_permission){
            $game_id_list = implode(',',$app_permission);
            $where .= " and b.app_id in($game_id_list)";
        }

        if ($os_id){
            $where .= " and b.os_id  = {$os_id}";
        }

        $country_id = isset($params['country_id']) ? $params['country_id'] : '';
        if ($country_id){
            $where .= " and b.country_id  = {$country_id}";
        }

        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        if($company == 9){
            $where .= " and b.company_id = 9 ";
        }elseif($company == 1 ){
            $where .= " and b.company_id != 9 ";
        }
        // 筛选数据维度
        $matching_name = [];
        $search_field = '';
        $key_field = '';
        $tg_platform_table = '';
        $tg_platform_table = 'zplay_basic_tg_plat_report_total';
        if ($currency_type_id == 60){
            $tg_platform_table = 'zplay_basic_tg_plat_report_total_usd';
        }
        if ($type == 4){
            $group_by .= " group by country_id";
            $search_field = ' ,country_id';
            $key_field = 'country_id';
            $country_arr = DB::table('c_country_standard')->select(['full_name as country_name_e','id'])->where('type',2)->get();
            $country_arr = Service::data($country_arr);

            foreach ($country_arr as $ck => $cv){
                $matching_name[$cv['id']] = $cv['country_name_e'];
            }

        }elseif($type == 58){
            $group_by .= " group by platform_id";
            $search_field = ' ,platform_id';
            $key_field = 'platform_id';
            $country_arr = DB::table('c_platform')->select(['platform_name','platform_id'])->where('platform_type_id',4)->get();
            $country_arr = Service::data($country_arr);

            foreach ($country_arr as $ck => $cv){
                $matching_name[$cv['platform_id']] = $cv['platform_name'];
            }

        }

        $total_data = [];
        // 当前时间段 查询
        $start_time = isset($params['start_time']) ? $params['start_time'] : date('Y-m-d',strtotime("-7 days"));
        $end_time = isset($params['end_time']) ? $params['end_time'] : date('Y-m-d');

        $partition = '';
        $all_month_arr = Service::getAllMonthNum($start_time,$end_time);
        // 日期相差时间段
        $time_period = Service::timePeriod($start_time,$end_time);
        $all_month = [];
        $date_target_arr = []; // 日期时间段

//        var_dump($group_by);die;
        if ($time_period <= 31){
            // 一个月之内
            for ($m = 0;$m <= count($all_month_arr); $m++){
                $all_month[$m] = 'basicmonth'.date('Ym',strtotime("{$start_time} +$m month"));
            }
            $partition = " partition (".implode(',',$all_month).")";
            if ($group_by)
                $group_by .= ' ,date';
            else $group_by = ' group by date';
            $group_all_by = ' group by date';
            $date_time_column = "  ,date as date_time";

            for ($d = 0; $d <= $time_period; $d++){
                $date_target_arr[date('Y-m-d',strtotime("{$start_time} +$d days"))] = [];
            }

        }elseif (count($all_month_arr) <= 12){
            // 6个月之内 按 15天分区
            for ($m = 0;$m <= count($all_month_arr); $m++){
                $all_month[$m] = 'basicmonth'.date('Ym',strtotime("{$start_time} +$m month"));
            }
            $partition = " partition (".implode(',',$all_month).")";
            if ($group_by)
                $group_by .= ' ,SUBSTR(date, 1,7)';
            else $group_by = ' group by SUBSTR(date, 1,7)';
            $group_all_by = ' group by SUBSTR(date, 1,7)';
            $date_time_column = "  ,SUBSTR(date, 1,7 ) as date_time";

            for ($m = 0;$m < count($all_month_arr); $m++){
                $date_target_arr[date('Y-m',strtotime("{$start_time} +$m month"))] = [];
            }


        }else{
            // 超过1年
            if ($group_by)
                $group_by .= ' ,SUBSTR(date, 1,4)';
            else $group_by = ' group by SUBSTR(date, 1,4)';

            $group_all_by = ' group by SUBSTR(date, 1,4)';

            $date_time_column = "  ,SUBSTR(date, 1,4 ) as date_time";

            $start_year = date('Y',strtotime($start_time));
            $end_year = date('Y',strtotime($end_time));
            $year_period = $end_year - $start_year;
            for ($y = 0;$y < $year_period; $y++){
                $date_target_arr[date('Y',strtotime("{$start_time} +$y year"))] = [];
            }

        }

        $where .= " and date between '{$start_time}' and '{$end_time}'";

        $chartx = [];
        $chartList = [];
        $return_data = [];

        if ($type == 4 || !$type){

            //  获取平台总非自然增长量
            $sql = "select sum(new) as new,sum(cost) as cost {$date_time_column} {$search_field} from {$tg_platform_table} {$partition} b".$where.$group_by;

            $total_data = Db::select($sql);
            $total_data = Service::data($total_data);

            $new_return_data = [];

            if ($total_data){
                if (!$type){
                    foreach ($total_data as $key => $data){
                        $total_data[$key]['cpi'] = $data['new'] ? round(($data['cost']/$data['new']),4) : 0.0000;//cpi 单价
                    }
                    foreach($total_data as $chart_data) {
                        if (isset($date_target_arr[$chart_data['date_time']])) {
                            $chartx[] = $chart_data['date_time'];
                            $chartList['cpi'][] = $chart_data['cpi'];
                        } else {
                            $chartx[] = $chart_data['date_time'];
                            $chartList['cpi'][] = 0.0000;
                        }
                    }

                }else{
                    foreach ($total_data as $key => $data){
                        $data['cost'] = intval($data['cost']);
                        $data['new'] = intval($data['new']);
                        $data['cpi'] = $data['new'] ? round(($data['cost']/$data['new']),4) : 0.0000;//cpi 单价

                        if ($matching_name && $key_field){
                            foreach ($matching_name as $m_key => $m_value){
                                if ($data[$key_field] == $m_key){
                                    $data[$key_field] = $m_value;
                                }
                            }
                        }
                        $total_data[$key] = $data;
                    }

                    foreach ($date_target_arr as $dtaak => $dtaav){
                        $chartx[] = $dtaak;
                    }

                    foreach ($total_data as $tk => $tv){
                        foreach ($date_target_arr as $dtak => $dtav){
                            $new_return_data[$tv[$key_field]][$dtak]['cpi'] = 0.0000;
                        }
                    }

                    foreach ($total_data as $tk => $tv){
                        foreach ($new_return_data as $nrdk => $nrdv){
                            if ($nrdk == $tv[$key_field]) {
                                foreach ($nrdv as $nrdvk => $nrdvv){
                                    if ($tv['date_time'] == $nrdvk){
                                        $new_return_data[$nrdk][$nrdvk]['cpi'] = $tv['cpi'];
                                    }
                                }
                            }
                        }
                    }


                    if ($type == 58){
                        foreach($new_return_data as $nrdk => $chart_data) {
                            foreach ($date_target_arr as $dtak => $dtav){
                                $chartList['plat_cpi'][$nrdk][] = $chart_data[$dtak]['cpi'];
                            }
                        }
                    }elseif($type == 4){
                        foreach($new_return_data as $nrdk => $chart_data) {
                            foreach ($date_target_arr as $dtak => $dtav){
                                $chartList['country_cpi'][$nrdk][] = $chart_data[$dtak]['cpi'];
                            }
                        }
                    }


                    if (isset($chartList['country_cpi'])){
                        foreach ($chartList['country_cpi'] as $k_k => $k_v){
                            if (array_sum($k_v) == 0){
                                unset($chartList['country_cpi'][$k_k]);
                            }
                        }
                    }
                }
            }
        }elseif($type == 58){
            //  获取平台总非自然增长量
            $sql = "select sum(new) as new,sum(cost) as cost {$date_time_column} {$search_field} from {$tg_platform_table} {$partition} b".$where.$group_by;

            $total_data = Db::select($sql);
            $total_data = Service::data($total_data);

            $new_return_data = [];

            if ($total_data){

                foreach ($total_data as $key => $data){
                    $data['cpi'] = $data['new'] ? round(($data['cost']/$data['new']),4) : 0.0000;//cpi 单价

                    if ($matching_name && $key_field){
                        foreach ($matching_name as $m_key => $m_value){
                            if ($data[$key_field] == $m_key){
                                $data[$key_field] = $m_value;
                            }
                        }
                    }
                    $total_data[$key] = $data;
                }

                foreach ($date_target_arr as $dtaak => $dtaav){
                    $chartx[] = $dtaak;
                }

                foreach ($total_data as $tk => $tv){
                    foreach ($date_target_arr as $dtak => $dtav){
                        $new_return_data[$tv[$key_field]][$dtak]['cpi'] = 0.0000;
                    }
                }

                foreach ($total_data as $tk => $tv){
                    foreach ($new_return_data as $nrdk => $nrdv){
                        if ($nrdk == $tv[$key_field]) {
                            foreach ($nrdv as $nrdvk => $nrdvv){
                                if ($tv['date_time'] == $nrdvk){
                                    $new_return_data[$nrdk][$nrdvk]['cpi'] = $tv['cpi'];
                                }
                            }
                        }
                    }
                }

                foreach($new_return_data as $nrdk => $chart_data) {
                    foreach ($date_target_arr as $dtak => $dtav){
                        $chartList['plat_cpi'][$nrdk][] = $chart_data[$dtak]['cpi'];
                    }
                }

            }
        }


        $return_data['chartx'] = $chartx;
        $return_data['chart_list'] = $chartList;

        ApiResponseFactory::apiResponse($return_data,[]);

    }

    // 总体情况AROU
    public static function dataPromotionArpu($params){
        //验证用户是否有权限登录
        $userid = $_SESSION['erm_data']['guid'];;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        //验证用户是否有权限登录
        $map1 = [];
        $map1['id'] = $userid;
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        //返回用户下用权限列表
        $app_permission = [];
        if($userInfo[0]['app_permission'] != -2){
            $app_permission = explode(',', $userInfo[0]['app_permission']);
        }

        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 60;

        // 查询条件
        $get_game = isset($params['app_select']) ? $params['app_select'] : '';
        $os_id = isset($params['os_id']) ? $params['os_id'] : '';
        $type = isset($params['type']) ? $params['type'] : '';
        // 分组
        $group_by = '';
        $where = " where 1=1 ";
        if($get_game){
            $where .= " and b.app_id  = {$get_game}";
        }elseif($app_permission){
            $game_id_list = implode(',',$app_permission);
            $where .= " and b.app_id in($game_id_list)";
        }

        $country_id = isset($params['country_id']) ? $params['country_id'] : '';
        if ($country_id){
            $where .= " and b.country_id  = {$country_id}";
        }

        if ($os_id){
            $where .= " and b.os_id  = {$os_id}";
        }

        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        if($company == 9){
            $where .= " and b.company_id = 9 ";
        }elseif($company == 1 ){
            $where .= " and b.company_id != 9 ";
        }
        // 筛选数据维度
        $matching_name = [];
        $search_field = '';
        $key_field = '';
        $tg_platform_table = '';
        if ($type == 4){
            $group_by .= " group by country_id";
            $search_field = ' ,country_id';
            $key_field = 'country_id';
            $country_arr = DB::table('c_country_standard')->select(['full_name as country_name_e','id'])->where('type',2)->get();
            $country_arr = Service::data($country_arr);

            foreach ($country_arr as $ck => $cv){
                $matching_name[$cv['id']] = $cv['country_name_e'];
            }


        }elseif($type == 58){
            $group_by .= " group by platform_id";
            $search_field = ' ,platform_id';
            $key_field = 'platform_id';
            $country_arr = DB::table('c_platform')->select(['platform_name','platform_id'])->where('platform_type_id',4)->get();
            $country_arr = Service::data($country_arr);

            foreach ($country_arr as $ck => $cv){
                $matching_name[$cv['platform_id']] = $cv['platform_name'];
            }

            $tg_platform_table = 'zplay_basic_tg_plat_report_total';
            if ($currency_type_id == 60){
                $tg_platform_table = 'zplay_basic_tg_plat_report_total_usd';
            }
        }

        $total_data = [];
        // 当前时间段 查询
        $start_time = isset($params['start_time']) ? $params['start_time'] : date('Y-m-d',strtotime("-7 days"));
        $end_time = isset($params['end_time']) ? $params['end_time'] : date('Y-m-d');

        $partition = '';
        $all_month_arr = Service::getAllMonthNum($start_time,$end_time);
        // 日期相差时间段
        $time_period = Service::timePeriod($start_time,$end_time);
        $all_month = [];
        $date_target_arr = []; // 日期时间段
        if ($time_period <= 31){
            // 一个月之内
            for ($m = 0;$m <= count($all_month_arr); $m++){
                $all_month[$m] = 'basicmonth'.date('Ym',strtotime("{$start_time} +$m month"));
            }
            $partition = " partition (".implode(',',$all_month).")";
            if ($group_by)
                $group_by .= ' ,date';
            else $group_by = ' group by date';
            $group_all_by = ' group by date';
            $date_time_column = "  ,date as date_time";

            for ($d = 0; $d <= $time_period; $d++){
                $date_target_arr[date('Y-m-d',strtotime("{$start_time} +$d days"))] = [];
            }

        }elseif (count($all_month_arr) <= 12){
            // 6个月之内 按 15天分区
            for ($m = 0;$m <= count($all_month_arr); $m++){
                $all_month[$m] = 'basicmonth'.date('Ym',strtotime("{$start_time} +$m month"));
            }
            $partition = " partition (".implode(',',$all_month).")";
            if ($group_by)
                $group_by .= ' ,SUBSTR(date, 1,7)';
            else $group_by = ' group by SUBSTR(date, 1,7)';
            $group_all_by = ' group by SUBSTR(date, 1,7)';
            $date_time_column = "  ,SUBSTR(date, 1,7 ) as date_time";

            for ($m = 0;$m < count($all_month_arr); $m++){
                $date_target_arr[date('Y-m',strtotime("{$start_time} +$m month"))] = [];
            }


        }else{
            // 超过1年
            if ($group_by)
                $group_by .= ' ,SUBSTR(date, 1,4)';
            else $group_by = ' group by SUBSTR(date, 1,4)';
            $group_all_by = ' group by SUBSTR(date, 1,4)';
            $date_time_column = "  ,SUBSTR(date, 1,4 ) as date_time";

            $start_year = date('Y',strtotime($start_time));
            $end_year = date('Y',strtotime($end_time));
            $year_period = $end_year - $start_year;
            for ($y = 0;$y < $year_period; $y++){
                $date_target_arr[date('Y',strtotime("{$start_time} +$y year"))] = [];
            }

        }

        $where .= " and date between '{$start_time}' and '{$end_time}'";

        $chartx = [];
        $chartList = [];

        if ($type == 4 || !$type){

            $tg_platform_table_new = 'zplay_basic_tg_report_total';
            if ($currency_type_id == 60){
                $tg_platform_table_new = 'zplay_basic_tg_report_total_usd';
            }

            $sql = "select sum(earning_all) as income,sum(active) as active {$date_time_column} {$search_field} from {$tg_platform_table_new} {$partition} b".$where.$group_by;

            $total_data = Db::select($sql);
            $total_data = Service::data($total_data);

            $return_data = [];
            $new_return_data = [];

            if ($total_data){
                if (!$type){
                    foreach ($total_data as $key => $data){
                        $total_data[$key]['arpu'] = $data['active'] ? round($data['income'] / $data['active'],4) : 0.0000;//arpu
                    }

                    foreach($total_data as $chart_data) {
                        if (isset($date_target_arr[$chart_data['date_time']])) {
                            $chartx[] = $chart_data['date_time'];
                            $chartList['arpu'][] = $chart_data['arpu'];
                        } else {
                            $chartx[] = $chart_data['date_time'];
                            $chartList['arpu'][] = 0;
                        }
                    }

                }else{
                    foreach ($total_data as $key => $data){
                        $data['active'] = intval($data['active']);
                        $data['income'] = intval($data['income']);
                        $data['arpu'] = $data['active'] ? round($data['income'] / $data['active'],4) : 0.0000;//arpu

                        if ($matching_name && $key_field){
                            foreach ($matching_name as $m_key => $m_value){
                                if ($data[$key_field] == $m_key){
                                    $data[$key_field] = $m_value;
                                }
                            }
                        }
                        $total_data[$key] = $data;
                    }

                    foreach ($date_target_arr as $dtaak => $dtaav){
                        $chartx[] = $dtaak;
                    }

                    foreach ($total_data as $tk => $tv){
                        foreach ($date_target_arr as $dtak => $dtav){
                            $new_return_data[$tv[$key_field]][$dtak]['arpu'] = 0.0000;
                        }
                    }

                    foreach ($total_data as $tk => $tv){
                        foreach ($new_return_data as $nrdk => $nrdv){
                            if ($nrdk == $tv[$key_field]) {
                                foreach ($nrdv as $nrdvk => $nrdvv){
                                    if ($tv['date_time'] == $nrdvk){
                                        $new_return_data[$nrdk][$nrdvk]['arpu'] = $tv['arpu'];
                                    }
                                }
                            }
                        }
                    }


                    if ($type == 58){
                        foreach($new_return_data as $nrdk => $chart_data) {
                            foreach ($date_target_arr as $dtak => $dtav){
                                $chartList['plat_arpu'][$nrdk][] = $chart_data[$dtak]['arpu'];
                            }
                        }
                    }elseif($type == 4){
                        foreach($new_return_data as $nrdk => $chart_data) {
                            foreach ($date_target_arr as $dtak => $dtav){
                                $chartList['country_arpu'][$nrdk][] = $chart_data[$dtak]['arpu'];
                            }
                        }
                    }

                    if (isset($chartList['country_arpu'])){
                        foreach ($chartList['country_arpu'] as $k_k => $k_v){
                            if (array_sum($k_v) == 0){
                                unset($chartList['country_arpu'][$k_k]);
                            }
                        }
                    }
                }
            }
        }elseif($type == 58){
            //  获取平台总非自然增长量
            $sql = "select sum(cost) as cost {$date_time_column} {$search_field} from {$tg_platform_table} {$partition} b".$where.$group_by;

            $platform_new_total = Db::select($sql);
            $platform_new_total = Service::data($platform_new_total);

            $tg_platform_table_new = 'zplay_basic_tg_report_total';
            if ($currency_type_id == 60){
                $tg_platform_table_new = 'zplay_basic_tg_report_total_usd';
            }
            //  获取平台总自然增长量 总增长量
            $sql = "select sum(earning_all) as income {$date_time_column} from {$tg_platform_table_new} {$partition} b".$where.$group_all_by;

            $total_data = Db::select($sql);
            $total_data = Service::data($total_data);

            $new_return_data = [];

            if ($total_data){

                // chartx
                foreach ($date_target_arr as $dtaak => $dtaav){
                    $chartx[] = $dtaak;
                }
                // chartlist total natural
                foreach ($total_data as $key => $data){
                    $total_data[$key]['income'] = intval($data['income']);
                }

                foreach($total_data as $chart_data) {
                    foreach ($date_target_arr as $dtak => $dtav){
                        if ($dtak == $chart_data['date_time']) {
                            $chartList['income'][] = $chart_data['income'];
                        } else {
                            $chartList['income'][] = 0;
                        }
                    }

                }

                // chartlist platform unnatural

                foreach ($platform_new_total as $key => $data){
                    $data['cost'] = intval($data['cost']);

                    if ($matching_name && $key_field){
                        foreach ($matching_name as $m_key => $m_value){
                            if ($data[$key_field] == $m_key){
                                $data[$key_field] = $m_value;
                            }
                        }
                    }
                    $platform_new_total[$key] = $data;
                }

                foreach ($platform_new_total as $tk => $tv){
                    foreach ($date_target_arr as $dtak => $dtav){
                        $new_return_data[$tv[$key_field]][$dtak]['cost'] = 0;
                    }
                }

                foreach ($platform_new_total as $tk => $tv){
                    foreach ($new_return_data as $nrdk => $nrdv){
                        if ($nrdk == $tv[$key_field]) {
                            foreach ($nrdv as $nrdvk => $nrdvv){
                                if ($tv['date_time'] == $nrdvk){
                                    $new_return_data[$nrdk][$nrdvk]['cost'] = $tv['cost'];
                                }
                            }
                        }
                    }
                }


                foreach($new_return_data as $nrdk => $chart_data) {
                    foreach ($date_target_arr as $dtak => $dtav){
                        $chartList['plat_cost'][$nrdk][] = $chart_data[$dtak]['cost'];
                    }
                }

            }
        }

        $return_data['chartx'] = $chartx;
        $return_data['chart_list'] = $chartList;

        ApiResponseFactory::apiResponse($return_data,[]);

    }

    // 总体情况 新增活跃 柱状图
    public static function dataPromotionUser($params){
        //验证用户是否有权限登录
        $userid = $_SESSION['erm_data']['guid'];;
//        $userid = 2;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        //验证用户是否有权限登录
        $map1 = [];
        $map1['id'] = $userid;
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        //返回用户下用权限列表
        $app_permission = [];
        if($userInfo[0]['app_permission'] != -2){
            $app_permission = explode(',', $userInfo[0]['app_permission']);
        }

        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 60;

        // 查询条件
        $get_game = isset($params['app_select']) ? $params['app_select'] : '';
        $os_id = isset($params['os_id']) ? $params['os_id'] : '';
        $type = isset($params['type']) ? $params['type'] : '';
        // 分组
        $group_by = '';
        $where = " where 1=1 ";
        if($get_game){
            $where .= " and b.app_id  = {$get_game}";
        }elseif($app_permission){
            $game_id_list = implode(',',$app_permission);
            $where .= " and b.app_id in($game_id_list)";
        }

        if ($os_id){
            $where .= " and b.os_id  = {$os_id}";
        }

        $country_id = isset($params['country_id']) ? $params['country_id'] : '';
        if ($country_id){
            $where .= " and b.country_id  = {$country_id}";
        }

        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        if($company == 9){
            $where .= " and b.company_id = 9 ";
        }elseif($company == 1 ){
            $where .= " and b.company_id != 9 ";
        }

        // 筛选数据维度
        $matching_name = [];
        $search_field = '';
        $key_field = '';
        $tg_platform_table = '';
        if (!$country_id){
            $group_by .= " group by country_id";
            $search_field = ' ,country_id';
            $key_field = 'country_id';
            $country_arr = DB::table('c_country_standard')->select(['china_name as country_name_e','id'])->where('type',2)->get();
            $country_arr = Service::data($country_arr);

            foreach ($country_arr as $ck => $cv){
                $matching_name[$cv['id']] = $cv['country_name_e'];
            }

            $tg_platform_table = 'zplay_basic_tg_country_new';

        }else{
            $group_by .= " group by platform_id";
            $search_field = ' ,platform_id';
            $key_field = 'platform_id';
            $country_arr = DB::table('c_platform')->select(['platform_name','platform_id'])->where('platform_type_id',4)->get();
            $country_arr = Service::data($country_arr);

            foreach ($country_arr as $ck => $cv){
                $matching_name[$cv['platform_id']] = $cv['platform_name'];
            }

            $tg_platform_table = 'zplay_basic_tg_platform_new';
        }

        $total_data = [];
        // 当前时间段 查询
        $start_time = isset($params['start_time']) ? $params['start_time'] : date('Y-m-d',strtotime("-7 days"));
        $end_time = isset($params['end_time']) ? $params['end_time'] : date('Y-m-d');

        // 分区查询
        $partition = '';
        $all_month_arr = Service::dateMonthsSections($start_time,$end_time);
        $all_month = [];


        // 日期相差时间段
        $time_period = Service::timePeriod($start_time,$end_time);
        $date_target_arr = []; // 日期时间段

//        var_dump($group_by);die;
        if ($time_period <= 31){
            // 一个月之内
            if ($all_month_arr){
                foreach ($all_month_arr as $month_srt){
                    $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
                }
                if ($all_month){
                    $partition = " partition (".implode(',',$all_month).")";
                }
            }
//            if ($group_by)
//                $group_by .= ' ,date';
//            else $group_by = ' group by date';
            $group_all_by = ' group by date';
            $date_time_column = "  ,date as date_time";

            for ($d = 0; $d <= $time_period; $d++){
                $date_target_arr[date('Y-m-d',strtotime("{$start_time} +$d days"))] = [];
            }

        }elseif (count($all_month_arr) <= 12){
            // 6个月之内 按 15天分区
            if ($all_month_arr){
                foreach ($all_month_arr as $month_srt){
                    $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
                }
                if ($all_month){
                    $partition = " partition (".implode(',',$all_month).")";
                }
            }
//            if ($group_by)
//                $group_by .= ' ,SUBSTR(date, 1,7)';
//            else $group_by = ' group by SUBSTR(date, 1,7)';
            $group_all_by = ' group by SUBSTR(date, 1,7)';
            $date_time_column = "  ,SUBSTR(date, 1,7 ) as date_time";

            for ($m = 0;$m < count($all_month_arr); $m++){
                $date_target_arr[date('Y-m',strtotime("{$start_time} +$m month"))] = [];
            }


        }else{
            // 超过1年
//            if ($group_by)
//                $group_by .= ' ,SUBSTR(date, 1,4)';
//            else $group_by = ' group by SUBSTR(date, 1,4)';

            $group_all_by = ' group by SUBSTR(date, 1,4)';

            $date_time_column = "  ,SUBSTR(date, 1,4 ) as date_time";

            $start_year = date('Y',strtotime($start_time));
            $end_year = date('Y',strtotime($end_time));
            $year_period = $end_year - $start_year;
            for ($y = 0;$y < $year_period; $y++){
                $date_target_arr[date('Y',strtotime("{$start_time} +$y year"))] = [];
            }

        }

        $where .= " and date between '{$start_time}' and '{$end_time}'";

        $chartx = [];
        $chartList = [];
        $return_data = [];

        if ($country_id){

            $sql = "select sum(new_nonature) as add_user_unnatural {$search_field} from {$tg_platform_table} {$partition} b".$where.$group_by." order by sum(new_nonature) desc";

            $total_data = Db::select($sql);
            $total_data = Service::data($total_data);

            if ($total_data){

                foreach ($total_data as $key => $data){
                    $data['add_user_unnatural'] = intval($data['add_user_unnatural']);
                    if ($matching_name && $key_field){
                        foreach ($matching_name as $m_key => $m_value){
                            if ($data[$key_field] == $m_key){
                                $data[$key_field] = $m_value;
                            }
                        }
                    }
                    $total_data[$key] = $data;
                }


                foreach ($total_data as $key => $data){
                    $chartx[] = $data['platform_id'];
                    $chartList['add_unnatural'][] = $data['add_user_unnatural'];
                }


            }



        }else{
            $sql = "select sum(new_total) as new_total,sum(new_nonature) as add_user_unnatural,sum(new_nature) as add_user_natural {$search_field} from {$tg_platform_table} {$partition} b".$where.$group_by." order by sum(new_nonature) desc  limit 30";

            $total_data = Db::select($sql);
            $total_data = Service::data($total_data);

            if ($total_data){
                foreach ($total_data as $key => $data){
                    $data['add_user_unnatural'] = intval($data['add_user_unnatural']);
                    $data['add_user_natural'] = intval($data['add_user_natural']);
                    $data['add_user_total'] = intval($data['new_total']);

                    if ($matching_name && $key_field){
                        foreach ($matching_name as $m_key => $m_value){
                            if ($data[$key_field] == $m_key){
                                $data[$key_field] = $m_value;
                            }
                        }
                    }
                    $total_data[$key] = $data;
                }

                foreach ($total_data as $key => $data){
                    $chartx[] = $data['country_id'];
                    $chartList['add_unnatural'][] = $data['add_user_unnatural'];
                    $chartList['add_natural'][] = $data['add_user_natural'];
                    $chartList['add_total'][] = $data['add_user_total'];
                }
            }

        }

        $return_data['chartx'] = $chartx;
        $return_data['chart_list'] = $chartList;

        ApiResponseFactory::apiResponse($return_data,[]);

    }

    /**
     * [dataPromotionPopulation 推广数据总体数据]
     * @param  [type] $params [description]
     * @return [type]         [description]
     */
    public static function dataPromotionPopulation($params){
         //验证用户是否有权限登录
        $userid = $_SESSION['erm_data']['guid'];;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        //验证用户是否有权限登录
        $map1 = [];
        $map1['id'] = $userid;
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        // //返回用户下用权限列表
         $app_permission = [];
        if($userInfo[0]['app_permission'] != -2){
            $app_permission = explode(',', $userInfo[0]['app_permission']);
        }

        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 60;
        $basic_tg_table = 'zplay_basic_tg_report_total';
        if ($currency_type_id == 60){
            $basic_tg_table = 'zplay_basic_tg_report_total_usd';
        }

        $data = [];

        // 查询条件
        $get_game = isset($params['app_select']) ? $params['app_select'] : '';
        $os_id = isset($params['os_id']) ? $params['os_id'] : '';

        $where = " where 1=1 ";
        $game_id_list = '';
        if($get_game){
            $where .= " and b.app_id  = {$get_game}";
        }elseif($app_permission){
            $game_id_list = implode(',',$app_permission);
            $where .= " and b.app_id in($game_id_list)";
        }

        if ($os_id){
            $where .= " and b.os_id  = {$os_id}";
        }

        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        if($company == 9){
            $where .= " and b.company_id = 9 ";
        }elseif($company == 1 ){
            $where .= " and b.company_id != 9 ";
        }

        // 分区查询
        $start_time = isset($params['start_time']) ? $params['start_time'] : date('Y-m-d',strtotime("-7 days"));
        $end_time = isset($params['end_time']) ? $params['end_time'] : date('Y-m-d');
        $where .= " and date between '{$start_time}' and '{$end_time}'";

        // 分区查询
        $partition = '';
        $all_month_arr = Service::dateMonthsSections($start_time,$end_time);
        $all_month = [];
        if ($all_month_arr){
            foreach ($all_month_arr as $month_srt){
                $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
            }
            if ($all_month){
                $partition = " partition (".implode(',',$all_month).")";
            }
        }

        $where .= ' and (new > 0 or active > 0 or new_nature > 0 or new_nonature > 0 or cost > 0 or earning_all > 0)';
        // 分组
        $group_by = ' group by b.date,b.app_id,b.os_id,b.country_id';
        $sql = "select 
        date,
        app.app_name,
        c.china_name,
        b.os_id,
        sum(new) as new,
        sum(active) as active,
        sum(new_nature) as new_nature,
        sum(new_nonature) as new_nonature,
        sum(cost) as cost,
        sum(earning_all) as earning_all
        from {$basic_tg_table} {$partition} b
        left join c_app app on b.app_id = app.id
        left join c_country_standard c on b.country_id = c.id
        ".$where.$group_by;

        $total_data = Db::select($sql);
        $total_data = Service::data($total_data);
        $array =[];

        foreach ($total_data as $key => $value) {

            $array[$key]['date'] =$value['date'];
            $array[$key]['app_name'] =$value['app_name'] ? $value['app_name'] : '未知应用';

            // 系统
            if ($value['os_id'] == 1){
                $os_id = 'iOS';
            }elseif ($value['os_id'] == 2){
                $os_id = 'Android';
            }else{
                $os_id = '未知';
            }
            $array[$key]['os'] = $os_id;

            $array[$key]['country'] =$value['china_name'] ? $value['china_name'] : '未知国家';
            $array[$key]['add_total'] =$value['new'] ? $value['new'] : 0;//总新增
            $array[$key]['active_total'] =$value['active'] ? $value['active'] : 0;//总活跃
            $array[$key]['add_natural'] =$value['new_nature'] ? $value['new_nature'] : 0;//自然新增 (总新增-非自然新增)
            $array[$key]['add_unnatural'] =$value['new_nonature'] ? $value['new_nonature'] : 0;//非自然新增
            $array[$key]['cpi_total'] = $value['new_nonature'] ? round(($value['cost']/$value['new_nonature']),2) : '0.00';//总CPI
            $array[$key]['cpi_add'] = $value['new'] ? round($value['cost']/$value['new'],2) : '0.00';//新增CPI
//            $array[$key]['cpi_active'] = $value['active'] ? round($value['cost']/$value['active']*100,2) : '0.00%';//活跃CPI
            $array[$key]['cost'] =$value['cost'] ? $value['cost'] : 0;//成本
            $array[$key]['arpu'] =$value['active'] ? round($value['earning_all'] / $value['active'],2) : 0.00;//arpu
            $array[$key]['income'] =$value['earning_all'] ? $value['earning_all'] : 0;//收入
            $gross_profit = $value['earning_all'] - $value['cost'];//毛利润
            $array[$key]['gross_profit'] = $gross_profit ? $gross_profit : 0;//毛利润
        }

        $data['table_list'] = $array;

        ApiResponseFactory::apiResponse($data,[]);
    }

    /**
     * [dataPromotionPopulation 推广数据总体数据]
     * @param  [type] $params [description]
     * @return [type]         [description]
     */
    public static function dataPromotionPopulationPageSize($params){
        //验证用户是否有权限登录
        $userid = $_SESSION['erm_data']['guid'];;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        //验证用户是否有权限登录
        $map1 = [];
        $map1['id'] = $userid;
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        // //返回用户下用权限列表
        $app_permission = [];
        if($userInfo[0]['app_permission'] != -2){
            $app_permission = explode(',', $userInfo[0]['app_permission']);
        }

        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 60;
        $basic_tg_table = 'zplay_basic_tg_report_total';
        if ($currency_type_id == 60){
            $basic_tg_table = 'zplay_basic_tg_report_total_usd';
        }

        $data = [];

        // 查询条件
        $get_game = isset($params['app_select']) ? $params['app_select'] : '';
        $os_id = isset($params['os_id']) ? $params['os_id'] : '';
        $country_id = isset($params['country_id']) ? $params['country_id'] : '';

        $where = " where 1=1 ";
        $game_id_list = '';
        if($get_game){
            $where .= " and b.app_id  = {$get_game}";
        }elseif($app_permission){
            $game_id_list = implode(',',$app_permission);
            $where .= " and b.app_id in($game_id_list)";
        }

        if ($os_id){
            $where .= " and b.os_id  = {$os_id}";
        }

        if ($country_id){
            $where .= " and b.country_id  = {$country_id}";
        }

        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        if($company == 9){
            $where .= " and b.company_id = 9 ";
        }elseif($company == 1 ){
            $where .= " and b.company_id != 9 ";
        }

        // 分区查询
        $start_time = isset($params['start_time']) ? $params['start_time'] : date('Y-m-d',strtotime("-7 days"));
        $end_time = isset($params['end_time']) ? $params['end_time'] : date('Y-m-d');
        $where .= " and date between '{$start_time}' and '{$end_time}'";

        // 分区查询
        $partition = '';
        $all_month_arr = Service::dateMonthsSections($start_time,$end_time);
        $all_month = [];
        if ($all_month_arr){
            foreach ($all_month_arr as $month_srt){
                $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
            }
            if ($all_month){
                $partition = " partition (".implode(',',$all_month).")";
            }
        }

        $where .= ' and (new > 0 or active > 0 or new_nature > 0 or new_nonature > 0 or cost > 0 or earning_all > 0)';
        // 分组
        $group_by = ' group by b.date,b.app_id,b.os_id,b.country_id';
        $order_by = ' order by b.date desc,sum(cost) desc';
        $sql = "select 
        date,
        app.app_name,
        app.release_region_id,
        c.china_name,
        app.id as appid,
        app.app_id,
        b.country_id,
        b.os_id,
        sum(new) as new,
        sum(active) as active,
        sum(new_nature) as new_nature,
        sum(new_nonature) as new_nonature,
        sum(cost) as cost,
        sum(earning_all) as earning_all
        from {$basic_tg_table} {$partition} b
        left join c_app app on b.app_id = app.id
        left join c_country_standard c on b.country_id = c.id
        ".$where.$group_by.$order_by;


        $pageSize = isset($params['size']) ? $params['size'] : 99999;
        $p = isset($params['page']) ? $params['page'] : 1;

        $searchSql ="select SQL_CALC_FOUND_ROWS a.* from ($sql)a  ";
        $countSql = "select count(*) c from ($sql)a";

        $start = ($p-1) * $pageSize;
        $searchSql_p = $searchSql." limit {$start},{$pageSize}";

        $total_data = Db::select($searchSql_p);
        $total_data = Service::data($total_data);
        $array =[];

        foreach ($total_data as $key => $value) {

            $array[$key]['date'] =$value['date'];

            // 发行区域ID(1,全球;2,国外;3,国内;)
            if ($value['release_region_id'] == 1){
                $release_region_id = '全球-';
            }elseif ($value['release_region_id'] == 2){
                $release_region_id = '国外-';
            }elseif ($value['release_region_id'] == 3){
                $release_region_id = '国内-';
            }else{
                $release_region_id = '未知区域-';
            }

            // 操作系统(1,ios;2,Android;3,h5;4,Amazon;)
            if ($value['os_id'] == 1){
                $os_id = 'ios-';
            }elseif ($value['os_id'] == 2){
                $os_id = 'Android-';
            }elseif ($value['os_id'] == 3){
                $os_id = 'h5-';
            }elseif ($value['os_id'] == 4){
                $os_id = 'Amazon-';
            }else{
                $os_id = '未知系统-';
            }

            $array[$key]['app_id'] = $value['appid'];
            $array[$key]['app_name'] = $release_region_id.$os_id.$value['app_name'].'-'.$value['app_id'];
            $array[$key]['country_id'] = $value['country_id'];

            // 系统
            if ($value['os_id'] == 1){
                $os_id = 'iOS';
            }elseif ($value['os_id'] == 2){
                $os_id = 'Android';
            }else{
                $os_id = '未知';
            }
            $array[$key]['os'] = $os_id;

            $array[$key]['country'] =$value['china_name'] ? $value['china_name'] : '未知国家';
            $array[$key]['add_total'] =$value['new'] ? $value['new'] : 0;//总新增
            $array[$key]['active_total'] =$value['active'] ? $value['active'] : 0;//总活跃
//            $array[$key]['add_natural'] =$value['new_nature'] ? $value['new_nature'] : 0;//自然新增 (总新增-非自然新增)
            $array[$key]['add_unnatural'] =$value['new_nonature'] ? $value['new_nonature'] : 0;//非自然新增
            $array[$key]['add_natural'] =$value['new'] - $value['new_nonature'];//自然新增 (总新增-非自然新增)
            $array[$key]['cpi_total'] = $value['new_nonature'] ? round(($value['cost']/$value['new_nonature']),4) : 0.0000;//总CPI
            $array[$key]['cpi_add'] = $value['new'] ? round($value['cost']/$value['new'],2) : '0.00';//新增CPI
//            $array[$key]['cpi_active'] = $value['active'] ? round($value['cost']/$value['active']*100,2) : '0.00%';//活跃CPI
            $array[$key]['cost'] =$value['cost'] ? round($value['cost'],2) : 0;//成本
            $array[$key]['arpu'] =$value['active'] ? round($value['earning_all'] / $value['active'],4) : 0.0000;//arpu
            $array[$key]['income'] =$value['earning_all'] ? round($value['earning_all'],2) : 0;//收入
            $gross_profit = $value['earning_all'] - $value['cost'] ;//毛利润
            $array[$key]['gross_profit'] = $gross_profit ? round($gross_profit, 2) : 0;//毛利润
        }

        $c_answer = DB::select($countSql);
        $c_answer = Service::data($c_answer);
        $count = $c_answer['0']['c'];

        $pageAll = ceil($count/$pageSize);
        $data['total'] = $count;
        $data['page_total'] = $pageAll;

        $data['table_list'] = $array;

        ApiResponseFactory::apiResponse($data,[]);
    }

    /**
     * [dataPromotionPopulation 推广数据设备]
     * @param  [type] $params [description]
     * @return [type]         [description]
     */
    public static function dataPromotionDevice($params){
         //验证用户是否有权限登录
        $userid = $_SESSION['erm_data']['guid'];;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        //验证用户是否有权限登录
        $map1 = [];
        $map1['id'] = $userid;
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        // // //返回用户下用权限列表
        $app_permission = [];
        if($userInfo[0]['app_permission'] != -2){
            $app_permission = explode(',', $userInfo[0]['app_permission']);
        }

        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 60;
        $basic_tg_table = 'zplay_basic_tg_report_total';
        if ($currency_type_id == 60){
            $basic_tg_table = 'zplay_basic_tg_report_total_usd';
        }

        $data = [];

        // 查询条件
        $get_game = isset($params['app_select']) ? $params['app_select'] : '';
        $os_id = isset($params['os_id']) ? $params['os_id'] : '';

        $where = " where 1=1 ";
        $game_id_list = '';
        if($get_game){
            $where .= " and b.app_id  = {$get_game}";
        }elseif($app_permission){
            $game_id_list = implode(',',$app_permission);
            $where .= " and b.app_id in($game_id_list)";
        }

        if ($os_id){
            $where .= " and b.os_id  = {$os_id}";
        }

        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        if($company == 9){
            $where .= " and b.company_id = 9 ";
        }elseif($company == 1 ){
            $where .= " and b.company_id != 9 ";
        }

        // 分区查询
        $start_time = isset($params['start_time']) ? $params['start_time'] : date('Y-m-d',strtotime("-7 days"));
        $end_time = isset($params['end_time']) ? $params['end_time'] : date('Y-m-d');
        $where .= " and date between '{$start_time}' and '{$end_time}'";

        // 分区查询
        $partition = '';
        $all_month_arr = Service::dateMonthsSections($start_time,$end_time);
        $all_month = [];
        if ($all_month_arr){
            foreach ($all_month_arr as $month_srt){
                $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
            }
            if ($all_month){
                $partition = " partition (".implode(',',$all_month).")";
            }
        }


        // 分组
        $group_by = '';
        $sql = "select 1 as device,b.os_id,b.date,sum(b.cost) as cost,sum(new_nature_phone) as new_nature,sum(new_nonature) as new_nonature,sum(new_nonature_phone) as new_nonature,sum(new_appsflyer_phone) as  total_new,app.app_name from {$basic_tg_table} {$partition} b
        left join c_app app on b.app_id = app.id
        ".$where.' and country_id = 247 group by b.app_id,b.os_id';
        $sql .= ' union all ';
        $sql .= "select 2 as device,b.os_id,b.date,sum(b.cost) as cost,sum(new_nature_pad) as new_nature,sum(new_nonature) as new_nonature,sum(new_nonature_pad) as new_nonature,sum(new_appsflyer_pad) as  total_new,app.app_name from {$basic_tg_table} {$partition} b
        left join c_app app on b.app_id = app.id
        ".$where.' and country_id = 247 group by b.app_id,b.os_id';

        //日期，应用，系统，设备，总新增，自然新增，非自然新增，总CPI，新增CPI，成本。

        $total_data = Db::select($sql);
        $total_data = Service::data($total_data);

        $array =[];
        foreach ($total_data as $key => $value) {
            $array[$key]['date'] =$value['date'];
            $array[$key]['app_name'] =$value['app_name'] ? $value['app_name'] : '未知应用';

            // 系统
            if ($value['os_id'] == 1){
                $os_id = 'iOS';
            }elseif ($value['os_id'] == 2){
                $os_id = 'Android';
            }else{
                $os_id = '未知';
            }
            $array[$key]['os'] = $os_id;

            // 设备类型 1,phone 2,pad 3,未知
            if ($value['device'] == 1){
                $device = 'phone';
            }elseif ($value['device'] == 2){
                $device = 'pad';
            }else{
                $device = '未知';
            }
            $array[$key]['device'] = $device;

            $array[$key]['add_total'] = $value['total_new'] ? $value['total_new'] : 0;//总新增
            $array[$key]['add_natural'] = $value['new_nature'] ? $value['new_nature'] : 0; //自然新增
            $array[$key]['add_unnatural'] =$value['new_nonature'] ? $value['new_nonature'] : 0; //非自然新增
            $array[$key]['cpi_total'] = $value['new_nonature'] ? round(($value['cost']/$value['new_nonature']),2) : '0.00';//总CPI
            $array[$key]['cpi_add'] = $value['total_new'] ? round($value['cost']/$value['total_new'],2) : '0.00';//新增CPI
            $array[$key]['cost'] = $value['cost'] ? round($value['cost'],2) : 0;//成本

        }

        $data['table_list'] = $array;

        ApiResponseFactory::apiResponse($data,[]);
    }


        /**
     * [dataPromotionPopulation 推广数据平台]
     * @param  [type] $params [description]
     * @return [type]         [description]
     */
    public static function dataPromotionPlatform($params){
         //验证用户是否有权限登录
        $userid = $_SESSION['erm_data']['guid'];;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        //验证用户是否有权限登录
        $map1 = [];
        $map1['id'] = $userid;
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        // // //返回用户下用权限列表
         $app_permission = [];
        if($userInfo[0]['app_permission'] != -2){
            $app_permission = explode(',', $userInfo[0]['app_permission']);
        }

        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 60;
        $basic_tg_table = 'zplay_basic_tg_plat_report_total';
        if ($currency_type_id == 60){
            $basic_tg_table = 'zplay_basic_tg_plat_report_total_usd';
        }

        $data = [];

        // 查询条件
        $get_game = isset($params['app_select']) ? $params['app_select'] : '';
        $os_id = isset($params['os_id']) ? $params['os_id'] : '';

        $where = " where 1=1 ";
        $game_id_list = '';
        if($get_game){
            $where .= " and b.app_id  = {$get_game}";
        }elseif($app_permission){
            $game_id_list = implode(',',$app_permission);
            $where .= " and b.app_id in($game_id_list)";
        }

        $country_id = isset($params['country_id']) ? $params['country_id'] : '';
        if ($country_id){
            $where .= " and b.country_id  = {$country_id}";
        }

        if ($os_id){
            $where .= " and b.os_id  = {$os_id}";
        }

        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        if($company == 9){
            $where .= " and b.company_id = 9 ";
        }elseif($company == 1 ){
            $where .= " and b.company_id != 9 ";
        }

        // 分区查询
        $start_time = isset($params['start_time']) ? $params['start_time'] : date('Y-m-d',strtotime("-7 days"));
        $end_time = isset($params['end_time']) ? $params['end_time'] : date('Y-m-d');
        $where .= " and date between '{$start_time}' and '{$end_time}'";

        // 分区查询
        $partition = '';
        $all_month_arr = Service::dateMonthsSections($start_time,$end_time);
        $all_month = [];
        if ($all_month_arr){
            foreach ($all_month_arr as $month_srt){
                $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
            }
            if ($all_month){
                $partition = " partition (".implode(',',$all_month).")";
            }
        }

        // 分组
        $group_by = '';
        $sql = "select b.tg_type,c.china_name,b.device,b.date,sum(b.click) as click,sum(b.impression) as imp,sum(b.new) as new,sum(b.cost) as cost,app.app_name,app.release_region_id,app.os_id,app.app_id,c.china_name,p.platform_name from {$basic_tg_table} {$partition} b
        left join c_app app on b.app_id = app.id
        left join (select distinct platform_id,platform_name from c_platform) p on b.platform_id = p.platform_id
        left join c_country_standard c on b.country_id = c.id
        ".$where.' and ( b.new > 0 or b.cost > 0) group by b.date,b.app_id,b.platform_id,b.country_id order by cost desc';
//        ".$where.' group by b.app_id,b.device,b.platform_id,b.os_id';
        // 日期，应用，系统，推广类型，推广平台，设备，激活，展示，点击，点击率，点击-激活率，展示-激活率，CPI单价（原币）/CPI（本币）
        $total_data = Db::select($sql);
        $total_data = Service::data($total_data);
        $array =[];
        foreach ($total_data as $key => $value) {
            $array[$key]['date'] =$value['date'];

            // 发行区域ID(1,全球;2,国外;3,国内;)
            if ($value['release_region_id'] == 1){
                $release_region_id = '全球-';
            }elseif ($value['release_region_id'] == 2){
                $release_region_id = '国外-';
            }elseif ($value['release_region_id'] == 3){
                $release_region_id = '国内-';
            }else{
                $release_region_id = '未知区域-';
            }

            // 操作系统(1,ios;2,Android;3,h5;4,Amazon;)
            if ($value['os_id'] == 1){
                $os_id = 'ios-';
            }elseif ($value['os_id'] == 2){
                $os_id = 'Android-';
            }elseif ($value['os_id'] == 3){
                $os_id = 'h5-';
            }elseif ($value['os_id'] == 4){
                $os_id = 'Amazon-';
            }else{
                $os_id = '未知系统-';
            }

            $array[$key]['app_name'] = $release_region_id.$os_id.$value['app_name'].'-'.$value['app_id'];
            $array[$key]['country'] =$value['china_name'] ? $value['china_name'] : '未知国家';

//            // 系统
//            if ($value['os_id'] == 1){
//                $os_id = 'iOS';
//            }elseif ($value['os_id'] == 2){
//                $os_id = 'Android';
//            }else{
//                $os_id = '未知';
//            }
//            $array[$key]['os'] = $os_id;

            // 推广类型 1,买量 2,广告推广
//            if($value['tg_type'] == 1){
//                $type ='买量推广';
//            }else{
//                $type ='广告推广';
//            }
//            $array[$key]['promotion_type'] = $type;
            $array[$key]['plat'] = $value['platform_name'] ? $value['platform_name'] : '未知平台';

            // 设备类型 1,phone 2,pad 3,未知
//            if ($value['device'] == 1){
//                $device = 'phone';
//            }elseif ($value['device'] == 2){
//                $device = 'pad';
//            }else{
//                $device = '未知';
//            }
//            $array[$key]['device'] = $device;

            $array[$key]['activate'] = $value['new'] ? $value['new'] : 0;
            $array[$key]['cpi'] = $value['new'] ? round(($value['cost']/$value['new']),4) : '0.00';//cpi 单价
            $array[$key]['cost'] = $value['cost'] ? round($value['cost'] , 4): 0 ;//成本
//            $array[$key]['exhibition'] = $value['imp'] ? $value['imp'] : 0;
//            $array[$key]['click'] = $value['click'] ? $value['click'] : 0;
//
//            $array[$key]['click_rate'] = $value['imp'] ? round($value['click']/$value['imp']*100,2).'%' : '0.00%';
//            $array[$key]['click_active_rate'] = $value['click'] ? round($value['new']/$value['click']*100,2).'%' : '0.00%';//点击-激活率
//            $array[$key]['exhibition_active_rate'] = $value['imp'] ? round($value['new']/$value['imp']*100,2).'%' : '0.00%';//展示-激活率


        }

        $data['table_list'] = $array;
        // todo 返回汇率待确认
        if ($all_month_arr){
            $all_month_str = implode(',',$all_month_arr);
        }else{
            $all_month_str = date('Ym');
        }

        $sql = "select ex.currency_id,c.currency_en,c.currency_name,ex.currency_ex,ex.effective_time from c_currency_ex ex left join c_currency_type c on c.id = ex.currency_id where ex.effective_time in ({$all_month_str}) and ex.currency_id = 60 group by ex.currency_id,ex.effective_time order by ex.effective_time ";
        $currency_list = DB::select($sql);
        $currency_list = Service::data($currency_list);
        $exchange_rate = [];
        if ($currency_list){
            foreach ($currency_list as $clk => $clv){
                $year = substr($clv['effective_time'],0,4);//当前年月
                $month = substr($clv['effective_time'],4,6);//当前年月
                $exchange_rate[] = $year .'-'.$month. '-01'.':' . $clv['currency_ex'];
            }
        }
        $data['exchange_rate'] = $exchange_rate;

        ApiResponseFactory::apiResponse($data,[]);
    }


    /**
     * [dataPromotionPopulation 推广数据平台]
     * @param  [type] $params [description]
     * @return [type]         [description]
     */
    public static function dataPromotionPopulationDetails($params){
        //验证用户是否有权限登录
        $userid = $_SESSION['erm_data']['guid'];;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        //验证用户是否有权限登录
        $map1 = [];
        $map1['id'] = $userid;
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        // // //返回用户下用权限列表
        $app_permission = [];
        if($userInfo[0]['app_permission'] != -2){
            $app_permission = explode(',', $userInfo[0]['app_permission']);
        }

        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 60;
        $basic_tg_table = 'zplay_basic_tg_plat_report_total';
        if ($currency_type_id == 60){
            $basic_tg_table = 'zplay_basic_tg_plat_report_total_usd';
        }

        $data = [];

        // 查询条件
        $get_game = isset($params['app_select']) ? $params['app_select'] : '';
        $os_id = isset($params['os_id']) ? $params['os_id'] : '';

        $where = " where 1=1 ";
        $game_id_list = '';
        if($get_game){
            $where .= " and b.app_id  = {$get_game}";
        }elseif($app_permission){
            $game_id_list = implode(',',$app_permission);
            $where .= " and b.app_id in($game_id_list)";
        }

        $country_id = isset($params['country_id']) ? $params['country_id'] : '';
        if ($country_id){
            $where .= " and b.country_id  = {$country_id}";
        }

        if ($os_id){
            $where .= " and b.os_id  = {$os_id}";
        }

        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        if($company == 9){
            $where .= " and b.company_id = 9 ";
        }elseif($company == 1 ){
            $where .= " and b.company_id != 9 ";
        }

        // 分区查询
        $start_time = isset($params['date_time']) ? $params['date_time'] : date('Y-m-d',strtotime("-7 days"));
        $end_time = isset($params['date_time']) ? $params['date_time'] : date('Y-m-d');
        $where .= " and date between '{$start_time}' and '{$end_time}'";

        // 分区查询
        $partition = '';
        $all_month_arr = Service::dateMonthsSections($start_time,$end_time);
        $all_month = [];
        if ($all_month_arr){
            foreach ($all_month_arr as $month_srt){
                $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
            }
            if ($all_month){
                $partition = " partition (".implode(',',$all_month).")";
            }
        }

        // 分组
        $group_by = '';
        $sql = "select b.tg_type,c.china_name,b.device,b.date,sum(b.click) as click,sum(b.impression) as imp,sum(b.new) as new,sum(b.cost) as cost,app.app_name,app.release_region_id,app.os_id,app.app_id,c.china_name,p.platform_name from {$basic_tg_table} {$partition} b
        left join c_app app on b.app_id = app.id
        left join (select distinct platform_id,platform_name from c_platform) p on b.platform_id = p.platform_id
        left join c_country_standard c on b.country_id = c.id
        ".$where.' and ( b.new > 0 or b.cost > 0) group by b.date,b.app_id,b.platform_id,b.country_id order by cost desc';
//        ".$where.' group by b.app_id,b.device,b.platform_id,b.os_id';

//        echo $sql;die;
        // 日期，应用，系统，推广类型，推广平台，设备，激活，展示，点击，点击率，点击-激活率，展示-激活率，CPI单价（原币）/CPI（本币）
        $total_data = Db::select($sql);
        $total_data = Service::data($total_data);
        $array =[];
        foreach ($total_data as $key => $value) {
            $array[$key]['date'] =$value['date'];

            // 发行区域ID(1,全球;2,国外;3,国内;)
            if ($value['release_region_id'] == 1){
                $release_region_id = '全球-';
            }elseif ($value['release_region_id'] == 2){
                $release_region_id = '国外-';
            }elseif ($value['release_region_id'] == 3){
                $release_region_id = '国内-';
            }else{
                $release_region_id = '未知区域-';
            }

            // 操作系统(1,ios;2,Android;3,h5;4,Amazon;)
            if ($value['os_id'] == 1){
                $os_id = 'ios-';
            }elseif ($value['os_id'] == 2){
                $os_id = 'Android-';
            }elseif ($value['os_id'] == 3){
                $os_id = 'h5-';
            }elseif ($value['os_id'] == 4){
                $os_id = 'Amazon-';
            }else{
                $os_id = '未知系统-';
            }

            $array[$key]['app_name'] = $release_region_id.$os_id.$value['app_name'].'-'.$value['app_id'];
            $array[$key]['country'] =$value['china_name'] ? $value['china_name'] : '未知国家';

//            // 系统
//            if ($value['os_id'] == 1){
//                $os_id = 'iOS';
//            }elseif ($value['os_id'] == 2){
//                $os_id = 'Android';
//            }else{
//                $os_id = '未知';
//            }
//            $array[$key]['os'] = $os_id;

            // 推广类型 1,买量 2,广告推广
//            if($value['tg_type'] == 1){
//                $type ='买量推广';
//            }else{
//                $type ='广告推广';
//            }
//            $array[$key]['promotion_type'] = $type;
            $array[$key]['plat'] = $value['platform_name'] ? $value['platform_name'] : '未知平台';

            // 设备类型 1,phone 2,pad 3,未知
//            if ($value['device'] == 1){
//                $device = 'phone';
//            }elseif ($value['device'] == 2){
//                $device = 'pad';
//            }else{
//                $device = '未知';
//            }
//            $array[$key]['device'] = $device;

            $array[$key]['activate'] = $value['new'] ? $value['new'] : 0;
            $array[$key]['cpi'] = $value['new'] ? round(($value['cost']/$value['new']),4) : '0.00';//cpi 单价
            $array[$key]['cost'] = $value['cost'] ? round($value['cost'] , 4): 0 ;//成本
//            $array[$key]['exhibition'] = $value['imp'] ? $value['imp'] : 0;
//            $array[$key]['click'] = $value['click'] ? $value['click'] : 0;
//
//            $array[$key]['click_rate'] = $value['imp'] ? round($value['click']/$value['imp']*100,2).'%' : '0.00%';
//            $array[$key]['click_active_rate'] = $value['click'] ? round($value['new']/$value['click']*100,2).'%' : '0.00%';//点击-激活率
//            $array[$key]['exhibition_active_rate'] = $value['imp'] ? round($value['new']/$value['imp']*100,2).'%' : '0.00%';//展示-激活率


        }

        $data['table_list'] = $array;
        // todo 返回汇率待确认
        if ($all_month_arr){
            $all_month_str = implode(',',$all_month_arr);
        }else{
            $all_month_str = date('Ym');
        }

        $sql = "select ex.currency_id,c.currency_en,c.currency_name,ex.currency_ex,ex.effective_time from c_currency_ex ex left join c_currency_type c on c.id = ex.currency_id where ex.effective_time in ({$all_month_str}) and ex.currency_id = 60 group by ex.currency_id,ex.effective_time order by ex.effective_time ";
        $currency_list = DB::select($sql);
        $currency_list = Service::data($currency_list);
        $exchange_rate = [];
        if ($currency_list){
            foreach ($currency_list as $clk => $clv){
                $year = substr($clv['effective_time'],0,4);//当前年月
                $month = substr($clv['effective_time'],4,6);//当前年月
                $exchange_rate[] = $year .'-'.$month. '-01'.':' . $clv['currency_ex'];
            }
        }
        $data['exchange_rate'] = $exchange_rate;

        ApiResponseFactory::apiResponse($data,[]);
    }


    /**
     * [dataPromotionPopulation 推广数据平台] ARPU 分平台
     * @param  [type] $params [description]
     * @return [type]         [description]
     */
    public static function dataPromotionArpuDetails($params){
        //验证用户是否有权限登录
        $userid = $_SESSION['erm_data']['guid'];
//        $userid = 2;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        //验证用户是否有权限登录
        $map1 = [];
        $map1['id'] = $userid;
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        // // //返回用户下用权限列表
        $app_permission = [];
        if($userInfo[0]['app_permission'] != -2){
            $app_permission = explode(',', $userInfo[0]['app_permission']);
        }

        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 60;
        $basic_plat_income_table = 'zplay_basic_tg_plat_country_income';
        $basic_country_income_table = 'zplay_basic_tg_report_total';
        if ($currency_type_id == 60){
            $basic_plat_income_table = 'zplay_basic_tg_plat_country_income_usd';
            $basic_country_income_table = 'zplay_basic_tg_report_total_usd';
        }

        $data = [];

        // 查询条件
        $get_game = isset($params['app_select']) ? $params['app_select'] : '';
        $os_id = isset($params['os_id']) ? $params['os_id'] : '';

        $where = " where 1=1 ";
        $game_id_list = '';
        if($get_game){
            $where .= " and b.app_id  = {$get_game}";
        }elseif($app_permission){
            $game_id_list = implode(',',$app_permission);
            $where .= " and b.app_id in($game_id_list)";
        }

        $country_id = isset($params['country_id']) ? $params['country_id'] : '';
        if ($country_id){
            $where .= " and b.country_id  = {$country_id}";
        }

        if ($os_id){
            $where .= " and b.os_id  = {$os_id}";
        }

        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        if($company == 9){
            $where .= " and b.company_id = 9 ";
        }elseif($company == 1 ){
            $where .= " and b.company_id != 9 ";
        }

        // 分区查询
        $start_time = isset($params['date_time']) ? $params['date_time'] : date('Y-m-d',strtotime("-7 days"));
        $end_time = isset($params['date_time']) ? $params['date_time'] : date('Y-m-d');
        $where .= " and date between '{$start_time}' and '{$end_time}'";

        // 分区查询
        $partition = '';
        $all_month_arr = Service::dateMonthsSections($start_time,$end_time);
        $all_month = [];
        if ($all_month_arr){
            foreach ($all_month_arr as $month_srt){
                $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
            }
            if ($all_month){
                $partition = " partition (".implode(',',$all_month).")";
            }
        }

        // 分组
        $group_by = '';
        $plat_income_sql = "select b.country_id,c.china_name,b.date,sum(b.income) as income,
        app.app_name,app.release_region_id,app.os_id,app.app_id,p.platform_name from {$basic_plat_income_table} {$partition} b
        left join c_app app on b.app_id = app.id
        left join (select distinct platform_id,platform_name from c_platform) p on b.platform_id = p.platform_id
        left join c_country_standard c on b.country_id = c.id
        ".$where.' and b.income > 0 group by b.date,b.app_id,b.platform_id,b.country_id order by sum(b.income) desc';

//        echo $plat_income_sql;

        $total_plat_data = Db::select($plat_income_sql);
        $total_plat_data = Service::data($total_plat_data);

        $country_income_sql = "select b.country_id,c.china_name,b.date,sum(b.earning_all) as total_income,sum(b.active) as active,
        app.app_name,app.release_region_id,app.os_id,app.app_id from {$basic_country_income_table} {$partition} b
        left join c_app app on b.app_id = app.id
        left join c_country_standard c on b.country_id = c.id
        ".$where.' and ( b.active > 0 or b.earning_all > 0) group by b.date,b.app_id,b.country_id order by sum(b.active) desc';

//        echo $country_income_sql;
//        die;
        // 日期，应用，系统，推广类型，推广平台，设备，激活，展示，点击，点击率，点击-激活率，展示-激活率，CPI单价（原币）/CPI（本币）
        $total_data = Db::select($country_income_sql);
        $total_data = Service::data($total_data);

        $return_array =[];
        if ($total_plat_data && $total_data){
            foreach ($total_plat_data as $total_plat_key => $total_plat_value){
                foreach ($total_data as $total_key => $total_value){
                    if (($total_plat_value['date'] == $total_value['date']) && ($total_plat_value['app_id'] == $total_value['app_id']) && ($total_plat_value['country_id'] == $total_value['country_id']) ){

                        // 发行区域ID(1,全球;2,国外;3,国内;)
                        if ($total_plat_value['release_region_id'] == 1){
                            $release_region_id = '全球-';
                        }elseif ($total_plat_value['release_region_id'] == 2){
                            $release_region_id = '国外-';
                        }elseif ($total_plat_value['release_region_id'] == 3){
                            $release_region_id = '国内-';
                        }else{
                            $release_region_id = '未知区域-';
                        }

                        // 操作系统(1,ios;2,Android;3,h5;4,Amazon;)
                        if ($total_plat_value['os_id'] == 1){
                            $os_id = 'ios-';
                        }elseif ($total_plat_value['os_id'] == 2){
                            $os_id = 'Android-';
                        }elseif ($total_plat_value['os_id'] == 3){
                            $os_id = 'h5-';
                        }elseif ($total_plat_value['os_id'] == 4){
                            $os_id = 'Amazon-';
                        }else{
                            $os_id = '未知系统-';
                        }

                        $return_array[$total_plat_key]['date'] = $total_plat_value['date'];
                        $return_array[$total_plat_key]['app_name']  = $release_region_id.$os_id.$total_plat_value['app_name'].'-'.$total_plat_value['app_id'];
                        $return_array[$total_plat_key]['plat'] = $total_plat_value['platform_name'] ? $total_plat_value['platform_name'] : '未知平台';
                        $return_array[$total_plat_key]['arpu'] = floatval($total_value['active']) ? round(floatval($total_plat_value['income']) / floatval($total_value['active']),4): 0.0000;
                        $return_array[$total_plat_key]['ratio'] = floatval($total_value['total_income']) ? round((floatval($total_plat_value['income']) / floatval($total_value['total_income']))*100,4)."%" : 0.00;
                        $return_array[$total_plat_key]['income'] = floatval($total_plat_value['income']) ? round(floatval($total_plat_value['income']),2) : 0.00;
                    }
                }
            }
        }
        ksort($return_array);
        $data['table_list'] = $return_array;
        ApiResponseFactory::apiResponse($data,[]);
    }


    /**
     * [dataPromotionPopulation 推广数据国家]
     * @param  [type] $params [description]
     * @return [type]         [description]
     */
    public static function dataPromotionCountry($params){
         //验证用户是否有权限登录
        $userid = $_SESSION['erm_data']['guid'];;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        //验证用户是否有权限登录
        $map1 = [];
        $map1['id'] = $userid;
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        // // //返回用户下用权限列表
         $app_permission = [];
        if($userInfo[0]['app_permission'] != -2){
            $app_permission = explode(',', $userInfo[0]['app_permission']);
        }

        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 60;
        $basic_tg_table = 'zplay_basic_tg_plat_report_total';
        if ($currency_type_id == 60){
            $basic_tg_table = 'zplay_basic_tg_plat_report_total_usd';
        }

        $data = [];
        // 查询条件
        $get_game = isset($params['app_select']) ? $params['app_select'] : '';
        $os_id = isset($params['os_id']) ? $params['os_id'] : '';

        $where = " where 1=1 ";
        $game_id_list = '';
        if($get_game){
            $where .= " and b.app_id  = {$get_game}";
        }elseif($app_permission){
            $game_id_list = implode(',',$app_permission);
            $where .= " and b.app_id in($game_id_list)";
        }

        if ($os_id){
            $where .= " and b.os_id  = {$os_id}";
        }

        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        if($company == 9){
            $where .= " and b.company_id = 9 ";
        }elseif($company == 1 ){
            $where .= " and b.company_id != 9 ";
        }

        // 分区查询
        $start_time = isset($params['start_time']) ? $params['start_time'] : date('Y-m-d',strtotime("-7 days"));
        $end_time = isset($params['end_time']) ? $params['end_time'] : date('Y-m-d');
        $where .= " and date between '{$start_time}' and '{$end_time}'";

        // 分区查询
        $partition = '';
        $all_month_arr = Service::dateMonthsSections($start_time,$end_time);
        $all_month = [];
        if ($all_month_arr){
            foreach ($all_month_arr as $month_srt){
                $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
            }
            if ($all_month){
                $partition = " partition (".implode(',',$all_month).")";
            }
        }


        // 分组
        $group_by = '';
        $sql = "select b.tg_type,b.os_id,b.date,sum(b.click) as click,sum(b.impression) as imp,sum(b.new) as new,sum(b.cost) as cost,app.app_name,c.china_name,p.platform_name from {$basic_tg_table} {$partition} b
        left join c_app app on b.app_id = app.id
        left join (select distinct platform_id,platform_name from c_platform) p on b.platform_id = p.platform_id
        left join c_country_standard c on b.country_id = c.id
        ".$where.' group by b.app_id,b.os_id,b.country_id,b.platform_id';
        //日期，应用，国家，推广类型，总新增，自然新增，非自然新增，平台，设备，激活，展示，点击，点击率，点击-激活率，展示-激活率。
        $total_data = Db::select($sql);
        $total_data = Service::data($total_data);
        $array =[];
        foreach ($total_data as $key => $value) {
            $array[$key]['date'] =$value['date'];
            $array[$key]['app_name'] =$value['app_name'];
            $array[$key]['country'] = $value['china_name'] ? $value['china_name'] : '未知国家';
            // 推广类型 1,买量 2,广告推广
            if($value['tg_type'] == 1){
                $type ='买量推广';
            }else{
                $type ='广告推广';
            }
            $array[$key]['promotion_type'] = $type;
            $array[$key]['plat'] = $value['platform_name'];

            // 设备类型 1,phone 2,pad 3,未知
//            if ($value['device'] == 1){
//               $device = 'phone';
//            }elseif ($value['device'] == 2){
//                $device = 'pad';
//            }else{
//                $device = '未知';
//            }
//            $array[$key]['device'] = $device;
            // 系统
            if ($value['os_id'] == 1){
                $os_id = 'iOS';
            }elseif ($value['os_id'] == 2){
                $os_id = 'Android';
            }else{
                $os_id = '未知';
            }
            $array[$key]['os'] = $os_id;
            $array[$key]['activate'] =$value['new'] ? $value['new'] : 0;
            $array[$key]['exhibition'] =$value['imp'] ? $value['imp'] : 0;
            $array[$key]['click'] =$value['click'] ? $value['click'] : 0;
            $array[$key]['click_rate'] = $value['imp'] ? round($value['click']/$value['imp']*100,2).'%' : '0.00%';
            $array[$key]['click_active_rate'] = $value['click'] ? round($value['new']/$value['click']*100,2).'%' : '0.00%';//点击-激活率
            $array[$key]['exhibition_active_rate'] = $value['imp'] ? round($value['new']/$value['imp']*100,2).'%' : '0.00%';//展示-激活率
            $array[$key]['cpi'] = $value['new'] ? round(($value['cost']/$value['new']),2) : '0.00';//cpi 单价

        }

        $data['table_list'] = $array;

        ApiResponseFactory::apiResponse($data,[]);
    }

    /**
     * [dataPromotionPopulation 国家或地区排名]
     * @param  [type] $params [description]
     * @return [type]         [description]
     */
    public static function dataPromotionRranking($params){
         //验证用户是否有权限登录
        $userid = $_SESSION['erm_data']['guid'];;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        //验证用户是否有权限登录
        $map1 = [];
        $map1['id'] = $userid;
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        // // //返回用户下用权限列表
         $app_permission = [];
        if($userInfo[0]['app_permission'] != -2){
            $app_permission = explode(',', $userInfo[0]['app_permission']);
        }

        $currency_type_id = isset($params['currency_type_id']) ? $params['currency_type_id'] : 60;
        $basic_tg_table = 'zplay_basic_tg_report_total';
        if ($currency_type_id == 60){
            $basic_tg_table = 'zplay_basic_tg_report_total_usd';
        }

        $data = [];

        // 查询条件
        $get_game = isset($params['app_select']) ? $params['app_select'] : '';
        $os_id = isset($params['os_id']) ? $params['os_id'] : '';

        $where = " where 1=1 ";
        $game_id_list = '';
        if($get_game){
            $where .= " and b.app_id  = {$get_game}";
        }elseif($app_permission){
            $game_id_list = implode(',',$app_permission);
            $where .= " and b.app_id in($game_id_list)";
        }

        if ($os_id){
            $where .= " and b.os_id  = {$os_id}";
        }

        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        if($company == 9){
            $where .= " and b.company_id = 9 ";
        }elseif($company == 1 ){
            $where .= " and b.company_id != 9 ";
        }

        // 分区查询
        $start_time = isset($params['start_time']) ? $params['start_time'] : date('Y-m-d',strtotime("-7 days"));
        $end_time = isset($params['end_time']) ? $params['end_time'] : date('Y-m-d');
        $where .= " and date between '{$start_time}' and '{$end_time}'";

        // 分区查询
        $partition = '';
        $all_month_arr = Service::dateMonthsSections($start_time,$end_time);
        $all_month = [];
        if ($all_month_arr){
            foreach ($all_month_arr as $month_srt){
                $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
            }
            if ($all_month){
                $partition = " partition (".implode(',',$all_month).")";
            }
        }


        // 分组
        $group_by = '';
        $sql = "select c.full_name as name_en,c.china_name as name_cn,sum(new_nature) as add_natural,sum(new_nonature) as add_unnatural from {$basic_tg_table} {$partition} b
        left join c_country_standard c on b.country_id = c.id
        ".$where.' group by b.country_id';

        //日期，应用，国家，推广类型，总新增，自然新增，非自然新增，平台，设备，激活，展示，点击，点击率，点击-激活率，展示-激活率。
        $total_data = Db::select($sql);
        $total_data = Service::data($total_data);

        $data['list'] = $total_data;

        ApiResponseFactory::apiResponse($data,[]);
    }


    /**
     * 三方平台及数据平台初始化
     */
    public static function getCheckInit($params)
    {
        $userid = $_SESSION['erm_data']['guid'];
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }

        // todo 待确认
        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;

        $data = [];
        //用户权限
        $map1['id'] = $userid;
        //验证用户是否有权限登录
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        //返回用户下用权限列表
        $app_permission = [];
        if($userInfo[0]['app_permission'] != -2){
            $app_permission = explode(',', $userInfo[0]['app_permission']);
        }

        $timeGranularity['select_id'] = '3';
        $timeGranularity['data'] = array(array('id'=>'1','value'=>'年'),array('id'=>'2','value'=>'月'),array('id'=>'3','value'=>'日'));
        $data['time_granularity'] = $timeGranularity;
        $timeCutBay['select_id']='1';
        $timeCutBay['data'] = array(array('id'=>'1','value'=>'时间序列'),array('id'=>'2','value'=>'时间段'));
        $data['time_cutbay_id'] = $timeCutBay;
        $data['time'] = array('start_time'=>date('Y-m-d',strtotime('-2 day')),'end_time'=>date('Y-m-d',strtotime('-2 day')));

        $app_list = DB::table("c_app")->select(['app_id as id','app_name as value','app_id','release_region_id','os_id']);
        if ($app_permission){
            $app_list->whereIn('id',$app_permission);
        }
        if($company == 9){
            $app_list->where('company_id',$company);
        }elseif($company == 1 ){
            $app_list->whereNotIn('company_id',[9]);
        }
        $app_list = $app_list->groupBy(['id','app_name'])->where('status',1)->orderBy('app_full_name')->get();
        $app_list = Service::data($app_list);
        if ($app_list) {
            foreach ($app_list as $app_k => $app_v) {

                // 发行区域ID(1,全球;2,国外;3,国内;)
                if ($app_v['release_region_id'] == 1) {
                    $release_region_id = '全球-';
                } elseif ($app_v['release_region_id'] == 2) {
                    $release_region_id = '国外-';
                } elseif ($app_v['release_region_id'] == 3) {
                    $release_region_id = '国内-';
                } else {
                    $release_region_id = '未知区域-';
                }

                // 操作系统(1,ios;2,Android;3,h5;4,Amazon;)
                if ($app_v['os_id'] == 1) {
                    $os_id = 'ios-';
                } elseif ($app_v['os_id'] == 2) {
                    $os_id = 'Android-';
                } elseif ($app_v['os_id'] == 3) {
                    $os_id = 'h5-';
                } elseif ($app_v['os_id'] == 4) {
                    $os_id = 'Amazon-';
                } else {
                    $os_id = '未知系统-';
                }

                $app_v["value"] = $release_region_id . $os_id . $app_v["value"] . '-' . $app_v['app_id'];
                unset($app_v['app_id']);
                unset($app_v['os_id']);
                unset($app_v['release_region_id']);
                $app_list[$app_k] = $app_v;
            }
        }
        // platform
        $platform_data = DB::table("c_platform")->select(['platform_name as value','platform_id as id','platform_type_id'])->where(['status' => 1])->groupBy(['platform_id','platform_name','platform_type_id'])->orderBy('platform_type_id')->get();
        $platform_data = Service::data($platform_data);
        if($platform_data) {
            foreach ($platform_data as $key => $value) {
                if ($value['platform_type_id'] == 1) {
//                    $value["value"] = '统计-' . $value["value"] . '-' . $value["id"];
//                    unset($value['platform_type_id']);
//                    $platform_data[$key] = $value;
                    unset($platform_data[$key]);
                } elseif ($value['platform_type_id'] == 2) {
                    $value["value"] = '广告-' . $value["value"] . '-' . $value["id"];
                    unset($value['platform_type_id']);
                    $platform_data[$key] = $value;
                } elseif ($value['platform_type_id'] == 3) {
//                    $value["value"] = '计费-' . $value["value"] . '-' . $value["id"];
//                    unset($value['platform_type_id']);
//                    $platform_data[$key] = $value;
                    unset($platform_data[$key]);
                } elseif ($value['platform_type_id'] == 4) {
                    $value["value"] = '推广-' . $value["value"] . '-' . $value["id"];
                    unset($value['platform_type_id']);
                    $platform_data[$key] = $value;
                } elseif ($value['platform_type_id'] == 5) {
                    unset($platform_data[$key]);
                }
            }
        }
        sort($platform_data);
        $condition_list['app_id'] = 1;
        $condition_list['data'] = [
            [
                "id" => 1,
                "value" => "应用",
                "sub" => $app_list
            ],
            [
                "id" => 2,
                "value" => "操作系统",
                "sub" => [
                    ['id' => 1,"value" => "iOS"],
                    ['id' => 2,"value" => "Android"],
                    ['id' => 3,"value" => "H5"],
                    ['id' => 4,"value" => "Amazon"],
                    ['id' => 5,"value" => "其他"],
                ]
            ],
            [
                "id" => 3,
                "value" => "平台名称",
                "sub" => $platform_data
            ],
        ];
        $data['condition_data'] = $condition_list;


        ApiResponseFactory::apiResponse($data,[]);
    }

    /**
     * 三方平台及数据平台页面
     * @param $params
     */
    public static function getCheckData($params){

        // 用户ID
        $userid = isset($params['guid']) ? $params['guid'] : $_SESSION['erm_data']['guid'];
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        // 开始结束时间
        $stime = isset($params['start_time']) ? $params['start_time'] : '';
        $etime = isset($params['end_time']) ? $params['end_time'] : '';
        if(!$stime || !$etime){
            ApiResponseFactory::apiResponse([],[],751);
        }
        // 日 月 年
        $time_granularity_id = isset($params['time_granularity_id']) ? $params['time_granularity_id'] : 3;
        // 时间序列 时间段
        $time_cutbay_id = isset($params['time_cutbay_id']) ? $params['time_cutbay_id'] : 1;

        $search_table = 'zplay_basic_handwork_daily';

        $sql =' where 1=1 ';
        $sql .=" and platform_id != 'ptg31' ";

        //显示维度字段
        $orderby = '';
        $groupby = '';
        $select = '';

        //返回用户下可查询的应用ID
        $map1['id'] = $userid;
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        $power = ''; // 为空 则拥有全部查询权限
        if($userInfo[0]['app_permission'] != -2){
            $power = $userInfo[0]['app_permission'];
        }

        $app_info_list = [];
        $app_os_list = [];
        $app_plat_list = [];
        $table_title = [];
        $app = isset($params['app_id']) ? $params['app_id'] : '';
        $table_title['date_time'] = '日期';
        if($app){
            $table_title['app_id'] = '应用';
            $arr_app = [];
            if ($app != -2){
                $arr_app = explode(',',$app);
                $app = implode("','",$arr_app);
                $sql .=" and app_id in ('".$app."') ";
            }elseif($power){
                $arr_app = explode(',',$power);
            }
            $groupby .= ' group by app_id ';
            $select .= ' app_id';

            $app_list = DB::table("c_app")->select(['app_id as id','app_name as value','app_id','release_region_id','os_id']);
            if ($arr_app){
                $app_list->whereIn('app_id',$arr_app);
            }
            $app_list = $app_list->groupBy(['id','app_name'])->where('status',1)->orderBy('app_full_name')->get();
            $app_list = Service::data($app_list);
            if ($app_list) {
                foreach ($app_list as $app_k => $app_v) {

                    // 发行区域ID(1,全球;2,国外;3,国内;)
                    if ($app_v['release_region_id'] == 1) {
                        $release_region_id = '全球-';
                    } elseif ($app_v['release_region_id'] == 2) {
                        $release_region_id = '国外-';
                    } elseif ($app_v['release_region_id'] == 3) {
                        $release_region_id = '国内-';
                    } else {
                        $release_region_id = '未知区域-';
                    }

                    // 操作系统(1,ios;2,Android;3,h5;4,Amazon;)
                    if ($app_v['os_id'] == 1) {
                        $os_id = 'ios-';
                    } elseif ($app_v['os_id'] == 2) {
                        $os_id = 'Android-';
                    } elseif ($app_v['os_id'] == 3) {
                        $os_id = 'h5-';
                    } elseif ($app_v['os_id'] == 4) {
                        $os_id = 'Amazon-';
                    } else {
                        $os_id = '未知系统-';
                    }

                    $app_v["value"] = $release_region_id . $os_id . $app_v["value"] . '-' . $app_v['app_id'];
                    unset($app_v['app_id']);
                    unset($app_v['os_id']);
                    unset($app_v['release_region_id']);
                    $app_list[$app_k] = $app_v;
                }
                $app_info_list = $app_list;
            }
        }elseif($power){
            $arr_app = explode(',',$power);
            $power = implode("','",$arr_app);
            $sql .=" and app_id in ('".$power."') ";

        }

        $os = isset($params['os_id']) ? $params['os_id'] : '';
        if ($os){
            $table_title['os_id'] = '操作系统';
            if ($os != -2){
                $sql .=" and os_id in ({$os}) ";
            }
            if(!$groupby) {
                $groupby .= ' group by os_id ';
            }else{
                $groupby .= ' ,os_id ';
            }

            if(!$select) {
                $select .= ' os_id ';
            }else{
                $select .= ' ,os_id ';
            }

            $app_os_list = [
                ['id' => 1,"value" => "iOS"],
                ['id' => 2,"value" => "Android"],
                ['id' => 3,"value" => "H5"],
                ['id' => 4,"value" => "Amazon"],
                ['id' => 5,"value" => "其他"],
            ];

        }

        $platform = isset($params['platform_id']) ? $params['platform_id'] : '';
        if ($platform) {
            $table_title['platform_id'] = '平台名称';
            if ($platform != -2){
                $platform = implode("','",explode(',',$platform));
                $sql .= " and platform_id in ('".$platform."') ";
            }
            if (!$groupby) {
                $groupby .= ' group by platform_id ';
            } else {
                $groupby .= ' ,platform_id ';
            }

            if (!$select) {
                $select .= ' platform_id ';
            } else {
                $select .= ' ,platform_id ';

            }

            // platform
            $platform_data = DB::table("c_platform")->select(['platform_name as value','platform_id as id','platform_type_id'])->where(['status' => 1])->groupBy(['platform_id','platform_name','platform_type_id'])->orderBy('platform_type_id')->get();
            $platform_data = Service::data($platform_data);
            if($platform_data) {
                foreach ($platform_data as $key => $value) {
                    if ($value['platform_type_id'] == 1) {
                        unset($platform_data[$key]);
                    } elseif ($value['platform_type_id'] == 2) {
                        $value["value"] = '广告-' . $value["value"] . '-' . $value["id"];
                        unset($value['platform_type_id']);
                        $platform_data[$key] = $value;
                    } elseif ($value['platform_type_id'] == 3) {
                        unset($platform_data[$key]);
                    } elseif ($value['platform_type_id'] == 4) {
                        $value["value"] = '推广-' . $value["value"] . '-' . $value["id"];
                        unset($value['platform_type_id']);
                        $platform_data[$key] = $value;
                    } elseif ($value['platform_type_id'] == 5) {
                        unset($platform_data[$key]);
                    }
                }
                $app_plat_list = $platform_data;
            }
        }


        // 数据指标
        $target_id = isset($params['target_id']) ? $params['target_id'] : '';
        if(!$target_id) ApiResponseFactory::apiResponse([],[],1008);
        $target_id = explode(',',$target_id);
        foreach ($target_id as $target){
            if($target == 1){
                $table_title['handwork_income'] = '三方平台收入';
            }elseif($target == 2){
                $table_title['handwork_cost'] = '三方平台成本';
            }elseif($target == 3){
                $table_title['income'] = '数据平台收入';
            }elseif($target == 4){
                $table_title['cost'] = '数据平台成本';
            }elseif($target == 5){
                $table_title['diff_income'] = '收入差额';
            }elseif($target == 6){
                $table_title['diff_income_rate'] = '收入差额率';
            }elseif($target == 7){
                $table_title['diff_cost'] = '成本差额';
            }elseif($target == 8){
                $table_title['diff_cost_rate'] = '成本差额率';
            }
        }

        if(!$select) {
            $select .= " sum(handwork_income) as handwork_income ,sum(handwork_cost) as handwork_cost,sum(income) as income,sum(cost) as cost";
        }else{
            $select .= " ,sum(handwork_income) as handwork_income ,sum(handwork_cost) as handwork_cost,sum(income) as income,sum(cost) as cost";
        }
        $startTime = str_replace('-', '', $params['start_time']);
        $endTime = str_replace('-', '', $params['end_time']);
        $time_sql = " and date between '{$startTime}' and '{$endTime}'";

        // 分区查询
//        $partition = '';
//        $all_month_arr = Service::dateMonthsSections($params['start_time'],$params['end_time']);
//        $all_month = [];
//        if ($all_month_arr){
//            foreach ($all_month_arr as $month_srt){
//                $all_month[] = 'basicmonth'.str_replace('-','',$month_srt);
//            }
//            if ($all_month){
//                $partition = " partition (".implode(',',$all_month).")";
//            }
//        }

        $date_time_column = '';
        if($time_cutbay_id == 1){//时间序列
            if($time_granularity_id == 3){//日
                if(!$groupby)
                    $groupby = " group by date";
                else $groupby.=" ,date";
                $date_time_column='  date';
            }
            if($time_granularity_id == 2){//月
                if(!$groupby)
                    $groupby = " group by SUBSTR(date, 1,7)";
                else $groupby.=" ,SUBSTR(date, 1,7)";
                $date_time_column="   SUBSTR(date, 1,7) ";
            }
            if($time_granularity_id == 1){//年
                if(!$groupby)
                    $groupby = " group by SUBSTR(date, 1,4 )";
                else $groupby.=" ,SUBSTR(date, 1,4 )";
                $date_time_column="  SUBSTR(date, 1,4 )  ";
            }
            $date_orderby = "  {$date_time_column},";
        }else {

            if($time_granularity_id == 3){//日
                //	if(!$groupby)
                //			$groupby = " group by '$startTime-$endTime'";
                //		else $groupby.=",'$startTime-$endTime'";
            }
            if($time_granularity_id == 2){//月
                $startTime = date('Ym',strtotime($startTime));
                $endTime = date('Ym',strtotime($endTime));
                //	if(!$groupby)
                //		$groupby = " group by SUBSTR(date_time, 1,6 )";
                //	else $groupby.=" ,SUBSTR(date_time, 1,6 )";

            }
            if($time_granularity_id == 1){//年
                $startTime = date('Y',strtotime($startTime));
                $endTime = date('Y',strtotime($endTime));
                //	if(!$groupby)
                //		$groupby = " group by SUBSTR(date_time, 1,4 )";
                //	else $groupby.=" ,SUBSTR(date_time, 1,4 )";
            }
            $date_orderby = "  $startTime-$endTime,";
            if(!$groupby)
                $groupby = " group by '$startTime-$endTime'";
            else $groupby.=",'$startTime-$endTime'";
            $date_time_column="  '$startTime-$endTime' ";
        }
        $pageSize = isset($params['size']) ? $params['size'] : 99999;
        $p = isset($params['page']) ? $params['page'] : 1;


        //点击搜索按钮上面的排序时
        if(isset($params['sort_list']) && $params['sort_list']){
            $sortColumn = $params['sort_list'];
            if($sortColumn){
                $date_orderby = '';
                $orderby=" order by ";
                $by='';
                $g = '';

                if (!is_array($sortColumn)) $sortColumn = json_decode($sortColumn,true);
                foreach ($sortColumn as $sColumn){
                    if($sColumn['id'] == "date_time"){
                        $by .= "  {$date_time_column} {$sColumn['type']} ,";
                    }elseif($sColumn['id'] == 1){
                        $by .= "  app_id {$sColumn['type']} ,";
                    }elseif($sColumn['id'] == 2){
                        $by .= "  os_id {$sColumn['type']} ,";
                    }elseif($sColumn['id'] == 3){
                        $by .= "  platform_id {$sColumn['type']} ,";
                    }
                }

                $by = rtrim($by,',');
                $orderby .= $by;
            }
        }

        if(!$orderby)
            $orderby=" order by ";
        else $orderby.=" ,";
        if(trim($orderby) == trim("order by  ,"))
            $orderby = rtrim($orderby,',');
        if ($date_orderby)
            $date_orderby = rtrim($date_orderby,',');
        if ($orderby)
            $orderby = rtrim($orderby,',');

//        var_dump($sql,$select,$time_sql,$groupby,$orderby,$date_orderby);die;

        $searchSql ="select SQL_CALC_FOUND_ROWS a.* from (select   ".$date_time_column. " as date_time , ".$select." from {$search_table} ". $sql.$time_sql.$groupby.$orderby.$date_orderby.")a  ";
        $countSql = "select count(*) c from (select $search_table.* from {$search_table} ".$sql.$time_sql.$groupby.")a";

        $start = ($p-1) * $pageSize;
        $end = $p * $pageSize;
        $searchSql_p = $searchSql." limit {$start},{$pageSize}";
//        echo $searchSql;
//        echo $searchSql_p;
//        die;


        $all_data = [];

        $answer = [];
        $answer = DB::select($searchSql_p);
        $answer = Service::data($answer);

        if ($answer){
            foreach ($answer as $answer_key => $answer_value ){
                if ($app_info_list){
                    foreach ($app_info_list as $app_info){
                        if (isset($answer_value['app_id']) && $answer_value['app_id'] == $app_info['id']){
                            $answer_value['app_id'] = $app_info['value'];
                        }
                    }
                }

                if ($app_os_list){
                    foreach ($app_os_list as $os_info){
                        if (isset($answer_value['os_id']) && $answer_value['os_id'] == $os_info['id']){
                            $answer_value['os_id'] = $os_info['value'];
                        }
                    }
                }

                if ($app_plat_list){
                    foreach ($app_plat_list as $plat_info){
                        if (isset($answer_value['platform_id']) && $answer_value['platform_id'] == $plat_info['id']){
                            $answer_value['platform_id'] = $plat_info['value'];
                        }
                    }
                }
                $answer_value['handwork_income'] = round($answer_value['handwork_income'],2);
                $answer_value['income'] = round($answer_value['income'],2);
                $answer_value['handwork_cost'] = round($answer_value['handwork_cost'],2);
                $answer_value['cost'] = round($answer_value['cost'],2);
                $answer_value['diff_income'] = round($answer_value['handwork_income'] - $answer_value['income'],2);
                $answer_value['diff_income_rate'] = $answer_value['handwork_income'] > 0 ? round(($answer_value['handwork_income'] - $answer_value['income'])/$answer_value['handwork_income'] * 100,2) : 0;
                $answer_value['diff_cost'] = round($answer_value['handwork_cost'] - $answer_value['cost'],2);
                $answer_value['diff_cost_rate'] = $answer_value['handwork_cost'] > 0 ? round(($answer_value['handwork_cost'] - $answer_value['cost'])/$answer_value['handwork_cost'] * 100,2) : 0;

                $answer[$answer_key] = $answer_value;
            }
        }

        $table_list = [];
        if ($answer){
            foreach ($answer as $aa_k => $aa_v) {
                foreach ($table_title as $t_k => $title){
                    if (isset($aa_v[$t_k])){
                        $table_list[$aa_k][] = $aa_v[$t_k];
                    }
                }
            }
        }
        if(isset($params['is_export'])){
            $report_name = isset($params['report_name']) ? $params['report_name'] : "综合查询";
            $title = iconv('utf-8','gb2312',implode(',', array_values($table_title)));
            $values = is_array($table_list) ? $table_list : [];
            $string =Service::csv_output_str($title."\n", $values);
            $filename = iconv('utf-8','gb2312',$report_name).'-'.date('Ymd').'.csv'; //设置文件名
            Service::export_csv($filename,$string); //导出
            exit;

        }else{
            $all_data['table_title'] = array_values($table_title);
            $all_data['table_list'] = $table_list;

            $c_answer = DB::select($countSql);
            $c_answer = Service::data($c_answer);
            $count = $c_answer['0']['c'];

            $pageAll = ceil($count/$pageSize);
            $all_data['total'] = $count;
            $all_data['page_total'] = $pageAll;

            ApiResponseFactory::apiResponse($all_data,[]);
        }


    }


    /**
     * 三方平台及数据平台页面 数据更新
     */
    public static function updateCheckData($params){
        
        set_time_limit(0);
        // 用户ID
        $date = isset($params['date_time']) ? $params['date_time'] : date('Y-m');
        if(!$date){
            ApiResponseFactory::apiResponse([],[],809);
        }

        $result_str = '';

        DB::beginTransaction();

        $count = DB::select(" select count(1) as count from zplay_basic_handwork_daily a WHERE
a.date in (
select distinct all_date.date from (
SELECT date from zplay_ad_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m') = '$date' and app_id is not null  GROUP BY date
union all 
SELECT date from zplay_tg_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m') = '$date' and app_id is not null  GROUP BY date
) all_date
) and
a.app_id in (
        select distinct all_app.app_id from (
SELECT app_id from zplay_ad_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m') = '$date' and app_id is not null  GROUP BY app_id
union all 
SELECT app_id from zplay_tg_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m') = '$date' and app_id is not null  GROUP BY app_id
) all_app
       ) and
a.platform_id in (select distinct all_app.platform_id from (
SELECT platform_id from zplay_ad_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m') = '$date' and app_id is not null  GROUP BY platform_id
union all 
SELECT platform_id from zplay_tg_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m') = '$date' and app_id is not null  GROUP BY platform_id
) all_app
        ); ");
        $count = Service::data($count);
        if( $count && $count[0]['count'] > 0 ){
            $result_str.= '原数据条数：'.$count[0]['count'].'；';
            $del = DB::delete(" delete from zplay_basic_handwork_daily WHERE
date in (
select distinct all_date.date from (
SELECT date from zplay_ad_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m') = '$date' and app_id is not null  GROUP BY date
union all 
SELECT date from zplay_tg_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m') = '$date' and app_id is not null  GROUP BY date
) all_date
) and
app_id in (
        select distinct all_app.app_id from (
SELECT app_id from zplay_ad_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m') = '$date' and app_id is not null  GROUP BY app_id
union all 
SELECT app_id from zplay_tg_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m') = '$date' and app_id is not null  GROUP BY app_id
) all_app
       ) and
platform_id in (select distinct all_app.platform_id from (
SELECT platform_id from zplay_ad_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m') = '$date' and app_id is not null  GROUP BY platform_id
union all 
SELECT platform_id from zplay_tg_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m') = '$date' and app_id is not null  GROUP BY platform_id
) all_app
        ); ");
            if(!$del){
                DB::rollBack();
            }
            $result_str.= '删除数据条数：'.$del.'；';
        }

        $sql = "insert into zplay_basic_handwork_daily( date,
        platform_id,
        app_id,
        os_id,
        handwork_income,
        income,
        handwork_cost,
        cost,
        create_time)
        SELECT  s.date,
        s.platform_id,
        s.app_id,
        s.os_id,
        sum(s.handwork_incom)AS handwork_incom,
        sum(s.incom) AS income,
        sum(s.handwork_cost)AS handwork_cost,
        sum(s.cost) AS cost, 
        now() AS create_time
        from  (SELECT
        date,
        platform_id,
        app_id,
        os_id,
        sum(income) + sum(biding_income) AS handwork_incom,
        0 AS incom,
        0 AS handwork_cost,
        0 AS cost
        FROM
        zplay_ad_handwork_daily
        WHERE
        DATE_FORMAT(create_time, '%Y-%m') = '$date'
        AND app_id IS NOT NULL
        GROUP BY
        date,
        platform_id,
        app_id
        union all 
        SELECT
        date,
        platform_id,
        app_id,
        os_id,
        0 AS handwork_incom,
        0 AS incom,
        sum(cost) AS handwork_cost,
        0 AS cost
        FROM
        zplay_tg_handwork_daily
        WHERE
        DATE_FORMAT(create_time, '%Y-%m') = '$date'
        AND app_id IS NOT NULL
        GROUP BY
        date,
        platform_id,
        app_id
        union all
        SELECT
        a.date,
        a.platform_id,
        a.app_id,
        b.os_id as os_id,
        0 AS handwork_incom,
        sum(a.earning) AS incom,
        0 AS handwork_cost,
        0 AS cost
        FROM
        zplay_ad_report_daily a ,c_app b
        WHERE
        a.app_id = b.app_id AND
        a.date in (SELECT date from zplay_ad_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m') = '$date' and app_id is not null  GROUP BY date) and 
        a.app_id in (SELECT app_id from zplay_ad_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m') = '$date' and app_id is not null  GROUP BY app_id) and
        a.platform_id in (SELECT platform_id from zplay_ad_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m') = '$date' and app_id is not null  GROUP BY platform_id) 
        and  a.earning!=0 and statistics = 0  and flow_type  = 1 and b.`company_id` <> 9
        GROUP BY
        date,
        platform_id,
        app_id
        union all
        SELECT
        a.date,
        a.platform_id,
        a.app_id,
        b.os_id as os_id,
        0 AS handwork_incom,
        0 AS incom,
        0 AS handwork_cost,
        sum(a.cost) AS cost
        FROM
        zplay_tg_report_daily a ,c_app b
        WHERE
        a.app_id = b.app_id AND
        a.date in (SELECT date from zplay_tg_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m') = '$date' and app_id is not null  GROUP BY date) and 
        a.app_id in (SELECT app_id from zplay_tg_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m') = '$date' and app_id is not null  GROUP BY app_id) and
        a.platform_id in (SELECT platform_id from zplay_tg_handwork_daily WHERE DATE_FORMAT(create_time, '%Y-%m') = '$date' and app_id is not null  GROUP BY platform_id) 
        and  a.cost!=0 and b.`company_id` <> 9
        GROUP BY
        date,
        platform_id,
        app_id) as s GROUP BY
        s.date,
        s.platform_id,
        s.app_id";
        $insert_res = DB::insert($sql);
        if (!$insert_res){
            DB::rollBack();
        }
        DB::commit();

        ApiResponseFactory::apiResponse([$result_str],[]);
    }


    /**
     * 开发者分成数据接口
     * @param $params
     */
    public static function getDeveloperData($params){

        // todo 用户ID
        $userid = isset($params['guid']) ? $params['guid'] : $_SESSION['erm_data']['guid'];
//        $userid = 2;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        // 开始结束时间
        $stime = isset($params['start_time']) ? $params['start_time'] : '';
        $etime = isset($params['end_time']) ? $params['end_time'] : '';
        if(!$stime || !$etime){
            ApiResponseFactory::apiResponse([],[],751);
        }
        // 日 月 年
        $time_granularity_id = isset($params['time_granularity_id']) ? $params['time_granularity_id'] : 3;
        // 时间序列 时间段
        $time_cutbay_id = isset($params['time_cutbay_id']) ? $params['time_cutbay_id'] : 1;

        // todo 开发者分成数据表名称
        $search_table = 'zplay_divide_develop';

        $sql =' where 1=1 ';

        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        if($company == 9){
            $sql .= " and game_creator = 9 ";
        }elseif($company == 1 ){
            $sql .= " and game_creator != 9 ";
        }

        //显示维度字段
        $orderby = '';
        $groupby = '';
        $select = '';

        //返回用户下可查询的应用ID
        $map1['id'] = $userid;
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        $power_list = ''; // 为空 则拥有全部查询权限
        $power = []; // 为空 则拥有全部查询权限
        if($userInfo[0]['app_permission'] != -2){
            $power = $userInfo[0]['app_permission'];
//            if ($power_list){
//                $apps = DB::select("select id,app_id from c_app where id in ($power_list)");
//                $apps = Service::data($apps);
//                if ($apps){
//                    foreach ($apps as $app_id_info){
//                        $power[] = $app_id_info['app_id'];
//                    }
//                }
//            }
        }

        $app_info_list = [];
        $developer_data_list = [];
        $table_title = [];
        $app = isset($params['app_id']) ? $params['app_id'] : -2;
        $table_title['date_time'] = '日期';
        if($app){
            $table_title['app_name'] = '应用';
            $arr_app = [];
            if ($app != -2){
                $arr_app = $app;
//                $arr_app = explode(',',$app);
//                $app = implode("','",$arr_app);
                $sql .=" and app_id in ({$arr_app}) ";
            }elseif($power){
                $arr_app = $power;
//                $power = implode("','",$arr_app);
                $sql .=" and app_id in ($power) ";
            }
            $groupby .= ' group by app_id ';
            $select .= ' app_id,app_name';

            $app_list = DB::table("c_app")->select(['id as id','app_name as value','app_id','release_region_id','os_id']);
            if ($arr_app){
                $app_list->whereIn('id',explode(',',$arr_app));
            }
            $app_list = $app_list->groupBy(['id','app_name'])->where('status',1)->orderBy('app_full_name')->get();
            $app_list = Service::data($app_list);
            if ($app_list) {
                foreach ($app_list as $app_k => $app_v) {

                    // 发行区域ID(1,全球;2,国外;3,国内;)
                    if ($app_v['release_region_id'] == 1) {
                        $release_region_id = '全球-';
                    } elseif ($app_v['release_region_id'] == 2) {
                        $release_region_id = '国外-';
                    } elseif ($app_v['release_region_id'] == 3) {
                        $release_region_id = '国内-';
                    } else {
                        $release_region_id = '未知区域-';
                    }

                    // 操作系统(1,ios;2,Android;3,h5;4,Amazon;)
                    if ($app_v['os_id'] == 1) {
                        $os_id = 'ios-';
                    } elseif ($app_v['os_id'] == 2) {
                        $os_id = 'Android-';
                    } elseif ($app_v['os_id'] == 3) {
                        $os_id = 'h5-';
                    } elseif ($app_v['os_id'] == 4) {
                        $os_id = 'Amazon-';
                    } else {
                        $os_id = '未知系统-';
                    }

                    $app_v["value"] = $release_region_id . $os_id . $app_v["value"] . '-' . $app_v['app_id'];
                    unset($app_v['app_id']);
                    unset($app_v['os_id']);
                    unset($app_v['release_region_id']);
                    $app_list[$app_k] = $app_v;
                }
                $app_info_list = $app_list;
            }
        }elseif($power){
            $arr_app = $power;
            $power = implode("','",$arr_app);
            $sql .=" and app_id in ('".$power."') ";

        }


        $developer_id = isset($params['developer_id']) ? $params['developer_id'] : -2;
        if ($developer_id) {
            $table_title['developer_name'] = '开发者名称';
            if ($developer_id != -2){
                $sql .=" and developer_id in ({$developer_id}) ";
            }

            if (!$groupby) {
                $groupby .= ' group by developer_id ';
            } else {
                $groupby .= ' ,developer_id ';
            }

            if (!$select) {
                $select .= ' developer_id, developer_name';
            } else {
                $select .= ' ,developer_id, developer_name ';
            }

            // platform
            $developer_data = DB::table("c_developer")->select(['developer_name as value','id as id','developer_id'])->orderBy('developer_name')->get();
            $developer_data = Service::data($developer_data);
            if($developer_data) {
                foreach ($developer_data as $dev_key => $value) {
                    $value["value"] = $value["value"] . '-' . $value["developer_id"];
                    unset($value['developer_id']);
                    $developer_data[$dev_key] = $value;
                }
                $developer_data_list = $developer_data;
            }
        }

        $table_title['new_user'] = '新增用户';
        $table_title['active_user'] = '活跃用户';
        $table_title['ff_income'] = '付费收入';
        $table_title['ad_income'] = '广告收入';
        $table_title['tg_cost'] = '推广成本';
        $table_title['develop_cost'] = '开发者可分配利润';

        // todo select sql 拼接
        $select .= " ,sum(new_user) as new_user ,sum(active_user) as active_user,sum(ff_income_taxAfter) as ff_income,sum(ad_income_taxAfter) as ad_income,sum(tg_cost) as tg_cost,sum(develop_cost_taxAfter) as develop_cost ";

        $startTime = $params['start_time'];
        $endTime = $params['end_time'];
        $time_sql = " and date between '{$startTime}' and '{$endTime}'";

        // 时间段区分
        $date_time_column = '';
        if($time_cutbay_id == 1){//时间序列
            if($time_granularity_id == 3){//日
                if(!$groupby)
                    $groupby = " group by date";
                else $groupby.=" ,date";
                $date_time_column='  date';
            }
            if($time_granularity_id == 2){//月
                if(!$groupby)
                    $groupby = " group by SUBSTR(date, 1,7)";
                else $groupby.=" ,SUBSTR(date, 1,7)";
                $date_time_column="   SUBSTR(date, 1,7) ";
            }
            if($time_granularity_id == 1){//年
                if(!$groupby)
                    $groupby = " group by SUBSTR(date, 1,4 )";
                else $groupby.=" ,SUBSTR(date, 1,4 )";
                $date_time_column="  SUBSTR(date, 1,4 )  ";
            }
            $date_orderby = "  {$date_time_column} desc,sum(develop_cost_taxAfter) desc,";
        }else {

            if($time_granularity_id == 3){//日
                //	if(!$groupby)
                //			$groupby = " group by '$startTime-$endTime'";
                //		else $groupby.=",'$startTime-$endTime'";
            }
            if($time_granularity_id == 2){//月
                $startTime = date('Ym',strtotime($startTime));
                $endTime = date('Ym',strtotime($endTime));
                //	if(!$groupby)
                //		$groupby = " group by SUBSTR(date_time, 1,6 )";
                //	else $groupby.=" ,SUBSTR(date_time, 1,6 )";

            }
            if($time_granularity_id == 1){//年
                $startTime = date('Y',strtotime($startTime));
                $endTime = date('Y',strtotime($endTime));
                //	if(!$groupby)
                //		$groupby = " group by SUBSTR(date_time, 1,4 )";
                //	else $groupby.=" ,SUBSTR(date_time, 1,4 )";
            }
            $date_orderby = "  $startTime-$endTime desc,sum(develop_cost_taxAfter) desc,";
            if(!$groupby)
                $groupby = " group by '$startTime-$endTime'";
            else $groupby.=",'$startTime-$endTime'";
            $date_time_column="  '$startTime~$endTime' ";
        }
        $pageSize = isset($params['size']) ? $params['size'] : 99999;
        $p = isset($params['page']) ? $params['page'] : 1;


        // 列表字段排序
        if(isset($params['sort_list']) && $params['sort_list']){
            $sortColumn = $params['sort_list'];
            if($sortColumn){
                $date_orderby = '';
                $orderby=" order by ";
                $by='';
                $g = '';

                if (!is_array($sortColumn)) $sortColumn = json_decode($sortColumn,true);
                foreach ($sortColumn as $sColumn){
                    if($sColumn['id'] == "date_time"){
                        $by .= "  {$date_time_column} {$sColumn['type']} ,";
                    }else{
                        $by .= "  {$sColumn['id']} {$sColumn['type']} ,";
                    }
                }

                $by = rtrim($by,',');
                $orderby .= $by;
            }
        }

        if(!$orderby)
            $orderby=" order by ";
        else $orderby.=" ,";
        if(trim($orderby) == trim("order by  ,"))
            $orderby = rtrim($orderby,',');
        if ($date_orderby)
            $date_orderby = rtrim($date_orderby,',');
        if ($orderby)
            $orderby = rtrim($orderby,',');

//        var_dump($sql,$select,$time_sql,$groupby,$orderby,$date_orderby);die;

        $one_sql = "select   ".$date_time_column. " as date_time , ".$select." from {$search_table} ". $sql.$time_sql.$groupby.$orderby.$date_orderby;

        $searchSql = "select SQL_CALC_FOUND_ROWS a.* from (".$one_sql.")a  ";
        $countSql = "select count(*) c from (select $search_table.* from {$search_table} ".$sql.$time_sql.$groupby.")a";

        $start = ($p-1) * $pageSize;
        $end = $p * $pageSize;
        $searchSql_p = $searchSql." limit {$start},{$pageSize}";

        // todo 查询sql
//        echo $searchSql;
//        echo $searchSql_p;
//        die;

        $all_data = [];

        $answer = [];
        $answer = DB::select($searchSql_p);
        $answer = Service::data($answer);


        if ($answer){
            foreach ($answer as $answer_key => $answer_value ){
                if ($app_info_list){
                    foreach ($app_info_list as $app_info){
                        if (isset($answer_value['app_id']) && $answer_value['app_id'] == $app_info['id']){
                            $answer_value['app_name'] = $app_info['value'];
                        }
                    }
                }

                if ($developer_data_list){
                    foreach ($developer_data_list as $developer_info){
                        if (isset($answer_value['developer_id']) && $answer_value['developer_id'] == $developer_info['id']){
                            $answer_value['developer_name'] = $developer_info['value'];
                        }
                    }
                }

//                if($time_granularity_id == 3){//日
//                    $answer_value['date_time'] = $startTime.'~'.$endTime;
//                }
//                if($time_granularity_id == 2){//月
//                    $startTime = date('Y-m',strtotime($startTime));
//                    $endTime = date('Y-m',strtotime($endTime));
//                    $answer_value['date_time'] = $startTime.'~'.$endTime;
//
//                }
//                if($time_granularity_id == 1){//年
//                    $startTime = date('Y',strtotime($startTime));
//                    $endTime = date('Y',strtotime($endTime));
//                    $answer_value['date_time'] = $startTime.'~'.$endTime;
//                }

                $answer[$answer_key] = $answer_value;
            }
        }

        $table_list = [];
        if ($answer){
            foreach ($answer as $aa_k => $aa_v) {
                foreach ($table_title as $t_k => $title){
                    if (isset($aa_v[$t_k])){
                        $table_list[$aa_k][] = $aa_v[$t_k];
                    }
                }
            }
        }

        // 合计
        $table_total_sql = "select   ".$date_time_column. " as date_time , ".$select." from {$search_table} ". $sql.$time_sql;

        $total_answer = [];
        $total_answer = DB::select($table_total_sql);
        $total_answer = Service::data($total_answer);
        if ($total_answer){
            foreach ($total_answer as $total_key => $total_data){

                if (!$total_data['date_time']){
                    $total_answer = [];
                    break;
                }

                if($time_granularity_id == 3){//日
                    $total_data['date_time'] = $startTime.'~'.$endTime;
                }
                if($time_granularity_id == 2){//月
                    $startTime = date('Y-m',strtotime($startTime));
                    $endTime = date('Y-m',strtotime($endTime));
                    $total_data['date_time'] = $startTime.'~'.$endTime;

                }
                if($time_granularity_id == 1){//年
                    $startTime = date('Y',strtotime($startTime));
                    $endTime = date('Y',strtotime($endTime));
                    $total_data['date_time'] = $startTime.'~'.$endTime;
                }

                if (isset($total_data['app_id'])){
                    $total_data['app_id'] = '-';
                    $total_data['app_name'] = '-';
                }

                if (isset($total_data['developer_id'])){
                    $total_data['developer_id'] = '-';
                    $total_data['developer_name'] = '-';
                }

                $total_answer[$total_key] = $total_data;
            }
        }


        if(isset($params['is_export'])){
            $report_name = isset($params['report_name']) ? $params['report_name'] : "开发者分成数据";
            $title = iconv('utf-8','gb2312',implode(',', array_values($table_title)));

            $new_answer = DB::select($searchSql);
            $new_answer = Service::data($new_answer);

            $table_list = [];
            if ($new_answer){
                foreach ($new_answer as $new_answer_key => $new_answer_value ){
                    if ($app_info_list){
                        foreach ($app_info_list as $app_info){
                            if (isset($new_answer_value['app_id']) && $new_answer_value['app_id'] == $app_info['id']){
                                $new_answer_value['app_name'] = $app_info['value'];
                            }
                        }
                    }

                    if ($developer_data_list){
                        foreach ($developer_data_list as $developer_info){
                            if (isset($new_answer_value['developer_id']) && $new_answer_value['developer_id'] == $developer_info['id']){
                                $new_answer_value['developer_name'] = $developer_info['value'];
                            }
                        }
                    }
                    $new_answer[$new_answer_key] = $new_answer_value;
                }

                foreach ($new_answer as $aa_k => $aa_v) {
                    foreach ($table_title as $t_k => $title_1){
                        if (key_exists($t_k,$aa_v)){
                            $table_list[$aa_k][] = $aa_v[$t_k];
                        }
                    }
                }
            }

            $values = is_array($table_list) ? $table_list : [];
            $string =Service::csv_output_str($title."\n", $values);
            $filename = iconv('utf-8','gb2312',$report_name).'-'.date('Ymd').'.csv'; //设置文件名
            Service::export_csv($filename,$string); //导出
            exit;

        }else{
            $all_data['table_list'] = $answer;

            $c_answer = DB::select($countSql);
            $c_answer = Service::data($c_answer);
            $count = $c_answer['0']['c'];

            $pageAll = ceil($count/$pageSize);
            $all_data['table_total'] = $total_answer;
            $all_data['total'] = $count;
            $all_data['page_total'] = $pageAll;

            ApiResponseFactory::apiResponse($all_data,[]);
        }


    }


    /**
     * 开发者分成数据接口
     * @param $params
     */
    public static function getDeveloperLine($params){

        // todo 用户ID
        $userid = isset($params['guid']) ? $params['guid'] : $_SESSION['erm_data']['guid'];
//        $userid = 2;
        if(!$userid){
            ApiResponseFactory::apiResponse([],[],741);
        }
        session_write_close();
        // 开始结束时间
        $stime = isset($params['start_time']) ? $params['start_time'] : '';
        $etime = isset($params['end_time']) ? $params['end_time'] : '';
        if(!$stime || !$etime){
            ApiResponseFactory::apiResponse([],[],751);
        }
        // 日 月 年
        $time_granularity_id = isset($params['time_granularity_id']) ? $params['time_granularity_id'] : 3;
        // 时间序列 时间段
        $time_cutbay_id = isset($params['time_cutbay_id']) ? $params['time_cutbay_id'] : 1;

        // todo 开发者分成数据表名称
        $search_table = 'zplay_divide_develop';

        $sql =' where 1=1 ';

        $company = isset($params['user_company_id']) ? $params['user_company_id'] : 1;
        if($company == 9){
            $sql .= " and game_creator = 9 ";
        }elseif($company == 1 ){
            $sql .= " and game_creator != 9 ";
        }

        //显示维度字段
        $orderby = '';
        $groupby = '';
        $select = '';

        //返回用户下可查询的应用ID
        $map1['id'] = $userid;
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        $power_list = ''; // 为空 则拥有全部查询权限
        $power = []; // 为空 则拥有全部查询权限
        if($userInfo[0]['app_permission'] != -2){
            $power = $userInfo[0]['app_permission'];
//            if ($power_list){
//                $apps = DB::select("select id,app_id from c_app where id in ($power_list)");
//                $apps = Service::data($apps);
//                if ($apps){
//                    foreach ($apps as $app_id_info){
//                        $power[] = $app_id_info['app_id'];
//                    }
//                }
//            }
        }

        $table_title = [];
        $app = isset($params['app_id']) ? $params['app_id'] : -2;
        $table_title['date_time'] = '日期';
        if($app){
            $table_title['app_name'] = '应用';
            $arr_app = [];
            if ($app != -2){
//                $arr_app = explode(',',$app);
//                $app = implode("','",$arr_app);
                $sql .=" and app_id in ({$app}) ";
            }elseif($power){
                $arr_app = $power;
//                $power = implode("','",$arr_app);
                $sql .=" and app_id in ($arr_app) ";
            }
//            $groupby .= ' group by app_id ';
//            $select .= ' app_id';
        }


        $developer_id = isset($params['developer_id']) ? $params['developer_id'] : -2;
        if ($developer_id) {
            $table_title['developer_name'] = '开发者名称';
            if ($developer_id != -2){
                $sql .=" and developer_id in ({$developer_id}) ";
            }

//            if (!$groupby) {
//                $groupby .= ' group by developer_id ';
//            } else {
//                $groupby .= ' ,developer_id ';
//            }

//            if (!$select) {
//                $select .= ' developer_id ';
//            } else {
//                $select .= ' ,developer_id ';
//            }

        }


        // todo select sql 拼接
        $select .= " sum(new_user) as new_user ,sum(active_user) as active_user,sum(ff_income_taxAfter) as ff_income,sum(ad_income_taxAfter) as ad_income,sum(tg_cost) as tg_cost,sum(develop_cost_taxAfter) as develop_cost ";

        $startTime = $params['start_time'];
        $endTime = $params['end_time'];
        $time_sql = " and date between '{$startTime}' and '{$endTime}'";



        // 当前时间段 查询
        $start_time = isset($params['start_time']) ? $params['start_time'] : date('Y-m-d',strtotime("-7 days"));
        $end_time = isset($params['end_time']) ? $params['end_time'] : date('Y-m-d');

        // 日期相差时间段
        $all_month_arr = Service::dateMonthsSections($start_time,$end_time);
        $time_period = Service::timePeriod($start_time,$end_time);
        $date_target_arr = []; // 日期时间段
        // 时间段区分
        $date_time_column = '';
        if($time_cutbay_id == 1){//时间序列
            if($time_granularity_id == 3){//日
                if(!$groupby)
                    $groupby = " group by date";
                else $groupby.=" ,date";
                $date_time_column='  date';

                for ($d = 0; $d <= $time_period; $d++){
                    $date_target_arr[date('Y-m-d',strtotime("{$start_time} +$d days"))] = [];
                }
            }
            if($time_granularity_id == 2){//月
                if(!$groupby)
                    $groupby = " group by SUBSTR(date, 1,7)";
                else $groupby.=" ,SUBSTR(date, 1,7)";
                $date_time_column="   SUBSTR(date, 1,7) ";

                for ($m = 0;$m < count($all_month_arr); $m++){
                    $date_target_arr[date('Y-m',strtotime("{$start_time} +$m month"))] = [];
                }
            }
            if($time_granularity_id == 1){//年
                if(!$groupby)
                    $groupby = " group by SUBSTR(date, 1,4 )";
                else $groupby.=" ,SUBSTR(date, 1,4 )";
                $date_time_column="  SUBSTR(date, 1,4 )  ";

                $start_year = date('Y',strtotime($start_time));
                $end_year = date('Y',strtotime($end_time));
                $year_period = $end_year - $start_year;
                for ($y = 0;$y <= $year_period; $y++){
                    $date_target_arr[date('Y',strtotime("{$start_time} +$y year"))] = [];
                }

            }
            $date_orderby = "  {$date_time_column},";
        }else {

            if($time_granularity_id == 3){//日
                //	if(!$groupby)
                //			$groupby = " group by '$startTime-$endTime'";
                //		else $groupby.=",'$startTime-$endTime'";

                for ($d = 0; $d <= $time_period; $d++){
                    $date_target_arr[date('Y-m-d',strtotime("{$start_time} +$d days"))] = [];
                }

            }
            if($time_granularity_id == 2){//月
                $startTime = date('Ym',strtotime($startTime));
                $endTime = date('Ym',strtotime($endTime));
                //	if(!$groupby)
                //		$groupby = " group by SUBSTR(date_time, 1,6 )";
                //	else $groupby.=" ,SUBSTR(date_time, 1,6 )";

                for ($m = 0;$m <= count($all_month_arr); $m++){
                    $date_target_arr[date('Y-m',strtotime("{$start_time} +$m month"))] = [];
                }

            }
            if($time_granularity_id == 1){//年
                $startTime = date('Y',strtotime($startTime));
                $endTime = date('Y',strtotime($endTime));
                //	if(!$groupby)
                //		$groupby = " group by SUBSTR(date_time, 1,4 )";
                //	else $groupby.=" ,SUBSTR(date_time, 1,4 )";

                $start_year = date('Y',strtotime($start_time));
                $end_year = date('Y',strtotime($end_time));
                $year_period = $end_year - $start_year;
                for ($y = 0;$y <= $year_period; $y++){
                    $date_target_arr[date('Y',strtotime("{$start_time} +$y year"))] = [];
                }

            }
            $date_orderby = "  $startTime-$endTime,";
            if(!$groupby)
                $groupby = " group by '$startTime-$endTime'";
            else $groupby.=",'$startTime-$endTime'";
            $date_time_column="  '$startTime-$endTime' ";
        }
        $pageSize = isset($params['size']) ? $params['size'] : 99999;
        $p = isset($params['page']) ? $params['page'] : 1;


        // 列表字段排序
        if(isset($params['sort_list']) && $params['sort_list']){
            $sortColumn = $params['sort_list'];
            if($sortColumn){
                $date_orderby = '';
                $orderby=" order by ";
                $by='';
                $g = '';

                if (!is_array($sortColumn)) $sortColumn = json_decode($sortColumn,true);
                foreach ($sortColumn as $sColumn){
                    if($sColumn['id'] == "date_time"){
                        $by .= "  {$date_time_column} {$sColumn['type']} ,";
                    }else{
                        $by .= "  {$sColumn['id']} {$sColumn['type']} ,";
                    }
                }

                $by = rtrim($by,',');
                $orderby .= $by;
            }
        }

        if(!$orderby)
            $orderby=" order by ";
        else $orderby.=" ,";
        if(trim($orderby) == trim("order by  ,"))
            $orderby = rtrim($orderby,',');
        if ($date_orderby)
            $date_orderby = rtrim($date_orderby,',');
        if ($orderby)
            $orderby = rtrim($orderby,',');

//        var_dump($sql,$select,$time_sql,$groupby,$orderby,$date_orderby);die;

        $searchSql ="select   ".$date_time_column. " as date_time , ".$select." from {$search_table} ". $sql.$time_sql.$groupby;

        // todo 查询sql
//        var_dump($date_target_arr);
//        echo $searchSql;
//        die;


        $all_data = [];
        $answer = [];
        $answer = DB::select($searchSql);
        $answer = Service::data($answer);


        $chartx = [];
        $user_data = [];
        $revenue_data = [];

        if ($answer){

            // chartx
            foreach ($date_target_arr as $dtaak => $dtaav){
                $chartx[] = $dtaak;
            }

            $add_user_total = 0;
            $active_user_total = 0;
            $income_total = 0;
            $cost_total = 0;
            $add_user_list = [];
            $active_user_list = [];
            $income_list = [];
            $cost_list = [];

            foreach($answer as $chart_data) {
                $add_user_total += intval($chart_data['new_user']);
                $active_user_total += intval($chart_data['active_user']);
                $income_total += floatval($chart_data['ff_income']) + floatval($chart_data['ad_income']);
                $cost_total += floatval($chart_data['tg_cost']);
            }

            $add_user_list_old = [];
            $active_user_list_old = [];
            $income_list_old = [];
            $cost_list_old = [];
            foreach ($date_target_arr as $dtak => $dtav){
                foreach($answer as $chart_data) {
                    if ($dtak == $chart_data['date_time']) {
                        $add_user_list_old[$dtak] = intval($chart_data['new_user']);
                        $active_user_list_old[$dtak] = intval($chart_data['active_user']);
                        $income_list_old[$dtak] = floatval($chart_data['ff_income']) + floatval($chart_data['ad_income']);
                        $cost_list_old[$dtak] = floatval($chart_data['tg_cost']);
                        break;
                    }
                }
            }


            foreach ($date_target_arr as $ddtak => $ddtav){
                if (key_exists($ddtak,$add_user_list_old)){
                    $add_user_list[] = $add_user_list_old[$ddtak];
                }else{
                    $add_user_list[] = 0;
                }

                if (key_exists($ddtak,$active_user_list_old)){
                    $active_user_list[] = $active_user_list_old[$ddtak];
                }else{
                    $active_user_list[] = 0;
                }

                if (key_exists($ddtak,$income_list_old)){
                    $income_list[] = round($income_list_old[$ddtak],2);
                }else{
                    $income_list[] = 0;
                }

                if (key_exists($ddtak,$cost_list_old)){
                    $cost_list[] = round($cost_list_old[$ddtak],2);
                }else{
                    $cost_list[] = 0;
                }
            }

            $user_data['add_user_total'] = $add_user_total;
            $user_data['active_user_total'] = $active_user_total;
            $user_data['chartx'] = $chartx;
            $user_data['add_user_list'] = $add_user_list;
            $user_data['active_user_list'] = $active_user_list;

            $revenue_data['income_total'] = round($income_total,2);
            $revenue_data['cost_total'] = round($cost_total,2);
            $revenue_data['chartx'] = $chartx;
            $revenue_data['income_list'] = $income_list;
            $revenue_data['cost_list'] = $cost_list;

            $all_data['user_data'] = $user_data;
            $all_data['revenue_data'] = $revenue_data;
        }

        ApiResponseFactory::apiResponse($all_data,[]);


    }



}
