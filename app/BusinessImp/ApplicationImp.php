<?php

namespace App\BusinessImp;

use App\BusinessLogic\ApplicationLogic;
use App\BusinessLogic\ChannelLogic;
use App\BusinessLogic\CommonLogic;
use App\BusinessLogic\DeveloperLogic;
use App\BusinessLogic\OperationLogLogic;
use App\BusinessLogic\PlatformLogic;
use App\Common\ApiResponseFactory;
use App\Common\Service;
use function GuzzleHttp\Psr7\str;
use Illuminate\Support\Facades\App;
use Illuminate\Support\Facades\DB;
use App\BusinessLogic\UserLogic;


class ApplicationImp extends ApiBaseImp
{
    /**
     * 应用信息列表
     * @param $params array 请求数据
     */
    public static function getAppList($params)
    {
        $map = [];
        $map1['user_account'] = $_SESSION['erm_data']['email'];
        $map1['id'] = $_SESSION['erm_data']['guid'];
        //验证用户是否有权限登录
        $userInfo = UserLogic::Userlist($map1)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],741);
        //返回用户下用权限列表
        if($userInfo[0]['app_permission'] != -2){
            $app_permission = explode(',', $userInfo[0]['app_permission']);
            $map['in'] = ['c_app.id', $app_permission];
        }
        // 查询条件
        $search_name = isset($params['search_name']) ? $params['search_name'] : ''; // 应用名称 或者 应用全称
        $os_id = isset($params['os_id']) ? $params['os_id'] : ''; // 操作系统
        $release_region_id = isset($params['release_region_id']) ? $params['release_region_id'] : ''; // 发行地区
        $company_id = isset($params['company_id']) ? $params['company_id'] : ''; // 发行公司
        $app_category_id = isset($params['app_category_id']) ? $params['app_category_id'] : ''; // 应用大类
        $user_company_id = isset($params['user_company_id']) ? $params['user_company_id'] : 1; // 账号所属公司
        $page = isset($params['page']) ? $params['page'] : 1 ;
        $page_size = isset($params['size']) ? $params['size'] : 9000 ;
        if($user_company_id ==9){
            $map['in'] =['c_app.company_id',['9']];
        }else{
            $map['notin'] =['c_app.company_id',['9']];
        }

        // 获取数据

        if ($search_name) $map['like'][] = ['c_app.app_name','like', $search_name];
        if ($search_name) $map['like'][] = ['c_app.app_full_name','like', $search_name];
        if ($search_name) $map['like'][] = ['c_app.app_id','like', $search_name];

        if ($os_id) $map['c_app.os_id'] = $os_id;
        if ($release_region_id) $map['c_app.release_region_id'] = $release_region_id;
        if ($company_id) $map['c_app.company_id'] = $company_id;
        if ($app_category_id) $map['c_app.app_category_id'] = $app_category_id;

        $map['c_app.status'] = 1;

        $fields = ['c_app.*','c_developer.developer_name','c_zplay_company.company_name','c_app_category.app_category_name'];

        $map['leftjoin'] = [
            ['c_developer','c_developer.id', 'c_app.developer_id'],
            ['c_zplay_company','c_zplay_company.id', 'c_app.company_id'],
            ['c_app_category','c_app_category.id', 'c_app.app_category_id']
        ];
        $app_list = ApplicationLogic::getApplicationList($map,$fields)->forPage($page,$page_size)->orderby("c_app.id","desc")->get();
        $app_list = Service::data($app_list);
        if (!$app_list) ApiResponseFactory::apiResponse([],[],1000);

        foreach ($app_list as $key => $app_info){
            $app_channel_id = $app_info['id'];
            if ($app_info['online_time']) $app_list[$key]['online_time'] = date('Y-m-d',$app_info['online_time']);
            // 获取渠道计费分成信息
            $divide_map['type'] = 1;
            $divide_map['app_channel_id'] = $app_channel_id;
            $developer_divide_list = ChannelLogic::getChannelDivideMapping($divide_map)->orderby("create_time","desc")->first();
            $developer_divide_list = Service::data($developer_divide_list);
            if ($developer_divide_list){
                $app_list[$key]['divide_billing'] = $developer_divide_list['divide_billing'];
                $app_list[$key]['divide_ad'] = $developer_divide_list['divide_ad'];
                $app_list[$key]['divide_cost'] = $developer_divide_list['divide_cost'];
                $app_list[$key]['effective_date'] = isset($developer_divide_list['effective_date']) ? $developer_divide_list['effective_date'] : '';
            }
        }

        // 获取数据总数
        $total = ApplicationLogic::getApplicationList($map)->count();

        $back_data=[
            'table_list'=>$app_list,
            'total'=> $total,
            'page_total'=> ceil($total / $page_size),
        ];

        ApiResponseFactory::apiResponse($back_data,[]);
    }

    /**
     * 编辑添加应用
     * @param $params array 请求数据
     */
    public static function addApp($params)
    {

        // 必填参数判断
        $condition = ['app_name', 'app_full_name', 'developer_id', 'app_type_id', 'company_id', 'release_region_id', 'os_id', 'app_category_id', 'divide_billing', 'divide_ad', 'divide_cost'];

        $data = Service::checkField($params, $condition, ApplicationLogic::$tableFieldName);

        $id = isset($params['id']) ? $params['id'] : ''; // 应用自增ID

        // 应用全称校验逻辑
        $app_full_name = $data['app_full_name']; // 应用英文全称
        $app_full_name = Service::mergeSpaces(trim($app_full_name)); // 多空格只保留一个
        $app_full_name = explode(' ',$app_full_name);
        foreach ($app_full_name as $every_words){
//            $regex = '/^[A-Z]([a-z0-9\s\-_!()]*)$/sU';
            $regex = "/^[A-Z0-9]([a-zA-Z0-9\.\:\：\'\s\-_!()]*)$/sU";
            if(!preg_match($regex, $every_words)) ApiResponseFactory::apiResponse([],[],620);
        }
        $app_full_name = implode(' ',$app_full_name);
        $upper_case = ucwords($app_full_name); // 首字母转大写
        if ($upper_case != $app_full_name) ApiResponseFactory::apiResponse([],[],619);

        $divide_billing = isset($data['divide_billing']) ? trim($data['divide_billing']) : ''; // 计费分成比例
        $divide_ad = isset($data['divide_ad']) ? trim($data['divide_ad']) : ''; // 广告分成比例
        $divide_cost = isset($data['divide_cost']) ? trim($data['divide_cost']) : ''; // 成本分成比例

        // 判断是否必填
//        if (!$divide_billing) ApiResponseFactory::apiResponse([],[],610);
        if (!is_numeric($divide_billing)) ApiResponseFactory::apiResponse([],[],694);
        $divide_billing = floatval($divide_billing);
        // 判断计费分成 小数点位数
        $divide_billing_arr = explode('.',$divide_billing);
        if (isset($divide_billing_arr[1]) && strlen($divide_billing_arr[1]) > 2){
            ApiResponseFactory::apiResponse([],[],696);
        }
        // 判断是否是在0～100之间
        if ($divide_billing < 0 || $divide_billing > 100) ApiResponseFactory::apiResponse([],[],691);


//        if (!$divide_ad ) ApiResponseFactory::apiResponse([],[],611);
        if (!is_numeric($divide_ad) ) ApiResponseFactory::apiResponse([],[],695);
        $divide_ad = floatval($divide_ad);
        // 判断广告分成 小数点位数
        $divide_ad_arr = explode('.',$divide_ad);
        if (isset($divide_ad_arr[1]) && strlen($divide_ad_arr[1]) > 2){
            ApiResponseFactory::apiResponse([],[],697);
        }
        if ($divide_ad < 0 || $divide_ad > 100) ApiResponseFactory::apiResponse([],[],692);

//        if (!$divide_cost) ApiResponseFactory::apiResponse([],[],612);
        if (!is_numeric($divide_cost) ) ApiResponseFactory::apiResponse([],[],698);
        $divide_cost = floatval($divide_cost);
        // 判断成本分成 小数点位数
        $divide_cost_arr = explode('.',$divide_cost);
        if (isset($divide_cost_arr[1]) && strlen($divide_cost_arr[1]) > 2){
            ApiResponseFactory::apiResponse([],[],699);
        }
        if ($divide_cost < 0 || $divide_cost > 100) ApiResponseFactory::apiResponse([],[],693);
        
        $data['app_full_name'] = $app_full_name;
        // 发行小组
        $release_group = isset($params['release_group']) ? $params['release_group'] : ''; // 发行小组
        if ($release_group) $data['release_group'] = $release_group;

        // 上线时间
        $online_time = isset($params['online_time']) ? $params['online_time'] : ''; // 发行小组
        if ($online_time) {
            $data['online_time'] = strtotime($online_time);
        }
        $update_time = date('Y-m-d H:i:s');
        $data['update_time'] = $update_time;
        $create_time = date('Y-m-d H:i:s');
        $data['create_time'] = $create_time;

        $is_jailbreak = isset($params['is_jailbreak']) ? $params['is_jailbreak'] : 0; // 是否为越狱

        // 判断 编辑? 还是 创建?
        if ($id){ // 编辑

            // 获取当前ID的数据
            $map = [];
            $map['c_app.id'] = $id;
            $fields = ['c_app.*','c_developer.developer_name','c_zplay_company.company_name','c_app_category.app_category_name'];

            $map['leftjoin'] = [
                ['c_developer','c_developer.id', 'c_app.developer_id'],
                ['c_zplay_company','c_zplay_company.id', 'c_app.company_id'],
                ['c_app_category','c_app_category.id', 'c_app.app_category_id']
            ];
            $old_data = ApplicationLogic::getApplicationList($map,$fields)->first();
            $old_data = Service::data($old_data);
            $old_data['online_time'] = date('Y/m/d',$old_data['online_time']);
            // 获取开发者计费分成信息
            $divide_map['type'] = 1;
            $app_divide_list = ChannelLogic::getChannelDivideMapping($divide_map)->orderby("create_time","desc")->first();
            $app_divide_list = Service::data($app_divide_list);
            if ($app_divide_list){
                $old_data['divide_billing'] = $app_divide_list['divide_billing'];
                $old_data['divide_ad'] = $app_divide_list['divide_ad'];
                $old_data['divide_cost'] = $app_divide_list['divide_cost'];
                $old_data['effective_date'] = date('Y/m/d',strtotime($app_divide_list['effective_date']));
            }else{
                $old_data['divide_billing'] = '';
                $old_data['divide_ad'] = '';
                $old_data['divide_cost'] = '';
                $old_data['effective_date'] = '';
            }

//            $old_data_genera = $old_data['app_full_name'];
            $app_id = isset($params['app_id']) ? $params['app_id'] : ''; // 应用ID
            if (!$app_id) ApiResponseFactory::apiResponse([],[],617);
            $data['app_id'] = $app_id;
            $data['is_jailbreak'] = $is_jailbreak;

            $effective_date = isset($params['effective_date']) ? $params['effective_date'] : ''; // 生效时间
            if (!$effective_date) ApiResponseFactory::apiResponse([],[],618);

            // 开启事物 保存数据
            DB::beginTransaction();

            //校验应用名称、应用全称、发行地区、操作系统不能重复
            $map = [];
            $map['app_name'] = $data['app_name'];
            $map['app_full_name'] = $data['app_full_name'];
            $map['release_region_id'] = $data['release_region_id'];
            $map['os_id'] = $data['os_id'];
            $map[] = ['id','<>',$id];

            $app_info = ApplicationLogic::getApplicationList($map)->first();
            $app_info = Service::data($app_info);
            if ($app_info){ // 应用信息已经重复
                DB::rollBack();
                ApiResponseFactory::apiResponse([],[],613);
            }

            // 更新应用信息
            unset($data['divide_billing']);
            unset($data['divide_ad']);
            unset($data['divide_cost']);
            unset($data['app_name']);
            unset($data['app_id']);
            //unset($data['app_full_name']);
            $bool = ApplicationLogic::updateApplication($id, $data);
            if (!$bool){
                DB::rollBack();
                ApiResponseFactory::apiResponse([],[],614);
            }

            // 维护渠道分成关系
            if (isset($divide_billing) && isset($divide_ad) && isset($divide_cost)){
                $mapping_data = [];
                $mapping_data['app_channel_id'] = $id;
                $mapping_data['divide_billing'] = $divide_billing;
                $mapping_data['divide_ad'] = $divide_ad;
                $mapping_data['divide_cost'] = $divide_cost;
                $mapping_data['effective_date'] = date('Y-m-d', strtotime($effective_date));
                $mapping_data['type'] = 1;
                $mapping_data['create_time'] = $create_time;
                $result = ChannelLogic::addChannelDivideMapping($mapping_data);
                if (!$result){
                    DB::rollBack();
                    ApiResponseFactory::apiResponse([],[],615);
                }
            }

            // 维护应用二级大类
            if (isset($data['app_full_name']) && $data['app_full_name']){
                $genera_map['app_genera_name'] = $data['app_full_name'];
                $genera_info = ApplicationLogic::selectAppGenera($genera_map);
                $genera_info = Service::data($genera_info);
                if (!$genera_info){
                    $insert_data['app_genera_name'] = $data['app_full_name'];
                    $insert_res = ApplicationLogic::insertAppGenera($insert_data);
                    if (!$insert_res){
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([],[],839);
                    }
                }
            }
            
            DB::commit();
            //删除不可以编辑的字段
            unset($old_data['app_name']);
            unset($old_data['app_id']);
            unset($old_data['app_full_name']);
            // 保存日志
            OperationLogImp::saveOperationLog(2,2, $params, $old_data);
            ApiResponseFactory::apiResponse($id,[]);

        }else{ // 创建
            //获取当前用户的 uid
            $cookieinfo = $_SESSION['erm_data'];
            $uid = $cookieinfo['guid'];

            // 应用ID命名规则 游戏类型+游戏发行渠道+开发者编号+开发者下游戏编号

            $app_id = '';
            // 应用类型ID(1,网游;2,单机;3,H5)
            $app_type_id = $data['app_type_id'];
            // 操作系统(1,ios;2,Android;3,h5;4,Amazon;)
            $os_id = $data['os_id'];
            // 发行区域ID(1,全球;2,国外;3,国内;)
            $release_region_id = $data['release_region_id'];

            if($app_type_id == 3){
                // H5
                $app_id .= 'h5';
            }else if ($app_type_id == 1){
                // 网游
                $app_id .= 'w';
                if ($os_id == 1){
                    // ios
                    if ($is_jailbreak){
                        // 越狱
                        $app_id .= 'o';
                    }else{
                        // 非越狱
                        $app_id .= 'i';
                    }
                }elseif ($os_id == 2){
                    // Android
                    if ($release_region_id == 1){
                        // 国外
                        $app_id .= 'g';
                    }elseif($release_region_id == 3){
                        // 国内
                        $app_id .= 'a';
                    }
                }elseif ($os_id == 4){
                    // Amazon
                    $app_id .= 'z';
                }
            }elseif ($app_type_id == 2){
                // 单机
                $app_id .= 'g';
                if ($os_id == 1){
                    // ios
                    if ($is_jailbreak){
                        // 越狱
                        $app_id .= 'o';
                    }else{
                        // 非越狱
                        $app_id .= 'i';
                    }
                }elseif ($os_id == 2){
                    // Android
                    if ($release_region_id == 1){
                        // 国外
                        $app_id .= 'g';
                    }else
                        if($release_region_id == 3){
                        // 国内
                        $app_id .= 'a';
                    }
                }elseif ($os_id == 4){
                    // Amazon
                    $app_id .= 'z';
                }
            }

            // 开发者信息自增ID 编号
            $developer_id = $data['developer_id'];

            if ($developer_id < 10){
                $app_id .= '00' . strval($developer_id);
            }else if($developer_id < 100){
                $app_id .= '0' . strval($developer_id);
            }else{
                $app_id .= strval($developer_id);
            }

            // 获取当前开发者下应用数量
            $dev_app_map['developer_id'] = $developer_id;
            $dev_app_count = ApplicationLogic::getApplicationList($dev_app_map)->count();

            if ($dev_app_count){
                $dev_app_count = $dev_app_count + 1;
                if ($dev_app_count < 10){
                    $app_id .= '00' . strval($dev_app_count);
                }else if($dev_app_count < 100){
                    $app_id .= '0' . strval($dev_app_count);
                }else{
                    $app_id .= strval($dev_app_count);
                }
            }else{
                $app_id .= '001';
            }

            if (!$app_id) ApiResponseFactory::apiResponse([],[],515);
            $data['app_id'] = $app_id;
            $data['is_jailbreak'] = $is_jailbreak;

            // 开启事物 保存数据
            DB::beginTransaction();

            //校验应用名称、应用全称、发行地区、操作系统不能重复
            $map = [];
            $map['app_name'] = $data['app_name'];
            $map['app_full_name'] = $data['app_full_name'];
            $map['release_region_id'] = $data['release_region_id'];
            $map['os_id'] = $data['os_id'];

            $app_info = ApplicationLogic::getApplicationList($map)->first();
            $app_info = Service::data($app_info);
            if ($app_info){ // 应用信息已经重复
                DB::rollBack();
                ApiResponseFactory::apiResponse([],[],613);
            }

            // 保存应用数据
            $new_id = ApplicationLogic::addApplication($data);
            if (!$new_id){
                DB::rollBack();
                ApiResponseFactory::apiResponse([],[],614);
            }

            // 维护application字典表
            $application = [];
            $application['app_name'] = $data['app_name'];
            $application['gameid'] = $app_id;
            $application['app_id'] = $new_id;
            $application['new_app_id'] = $app_id;
            DB::table('application')->insert($application);

            // 维护渠道分成关系
            if (isset($divide_billing) && isset($divide_ad) && isset($divide_cost)){
                $mapping_data = [];
                $mapping_data['app_channel_id'] = $new_id;
                $mapping_data['divide_billing'] = $divide_billing;
                $mapping_data['divide_ad'] = $divide_ad;
                $mapping_data['divide_cost'] = $divide_cost;
                $mapping_data['type'] = 1;
                $mapping_data['effective_date'] = date('Y-m-d', strtotime($create_time));
                $mapping_data['create_time'] = $create_time;
                $result = ChannelLogic::addChannelDivideMapping($mapping_data);
                if (!$result){
                    DB::rollBack();
                    ApiResponseFactory::apiResponse([],[],615);
                }
            }

            // 维护应用二级大类
            if (isset($data['app_full_name']) && $data['app_full_name']){
                $genera_map['app_genera_name'] = $data['app_full_name'];
                $genera_info = ApplicationLogic::selectAppGenera($genera_map);
                $genera_info = Service::data($genera_info);
                if (!$genera_info){
                    $insert_data['app_genera_name'] = $data['app_full_name'];
                    $insert_res = ApplicationLogic::insertAppGenera($insert_data);
                    if (!$insert_res){
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([],[],830);
                    }
                }
            }


            $user_map['id']=$uid;
            //获取用户信息
            $userInfo = UserLogic::checkUserAccount($user_map)->first();
            $userInfo =Service::data($userInfo);
            //当普通员工添加应用时候  要更新user表中应用权限  和父级的应用权限
            if($userInfo['app_permission'] !=-2){
                $new_data['app_permission'] = $userInfo['app_permission'].','. $new_id;
                $new_edit_id = UserLogic::userEdit($userInfo['id'],$new_data);

                if (!$new_edit_id ){
                    DB::rollBack();
                    ApiResponseFactory::apiResponse([],[],615);
                }
                // 保存日志
                OperationLogImp::saveOperationLog(2,27,$new_data, $userInfo);

            }
            $parent_map['id']=$userInfo['parent_uid'];
            //获取用户信息
            $parent_info = UserLogic::checkUserAccount($parent_map)->first();
            $old_parent_info =Service::data($parent_info);
            $new_parent_info = [];
            if($old_parent_info){
                if($old_parent_info['app_permission'] !=-2){
                    $new_parent_info['app_permission'] = $old_parent_info['app_permission'].','. $new_id;
                    $parent_id = UserLogic::userEdit($userInfo['parent_uid'],$new_parent_info);
                    if (!$parent_id ){
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([],[],615);
                    }
                    OperationLogImp::saveOperationLog(2,27,$new_parent_info, $old_parent_info);

                }

            }


            DB::commit();

            // 保存日志
            OperationLogImp::saveOperationLog(1,2,$new_id);

            ApiResponseFactory::apiResponse($new_id,[]);

        }

    }

    /**
     * 统计信息配置列表
     * @param $params array 请求数据
     */
    public static function appStatisticList($params)
    {

        $app_id = isset($params['id']) ? $params['id'] : 1 ;
        $page = isset($params['page']) ? $params['page'] : 1 ;
        $page_size = isset($params['size']) ? $params['size'] : 9999 ;
        if (!$app_id) ApiResponseFactory::apiResponse([],[],617);

        // 获取Statistic数据
        $map = [];
        $map['app_id'] = $app_id;
        $app_statistic_list = ApplicationLogic::getStatistic($map)->forPage($page,$page_size)->get();
        $app_statistic_list = Service::data($app_statistic_list);
        if (!$app_statistic_list) ApiResponseFactory::apiResponse([],[],1000);

        foreach ($app_statistic_list as $key => $app_statistic_info){

            unset($app_statistic_info['create_time']);
            unset($app_statistic_info['update_time']);

            // 获取Statistic Version数据
            $map = [];
            $map[] = ['ad_status', '<>', '2'];
            $map[] = ['app_statistic_id', $app_statistic_info['id']];
            $map['leftjoin'] = [
                ['c_channel','c_channel.id', 'c_app_statistic_version.channel_id']
            ];
            $fields = ['c_app_statistic_version.*','c_channel.channel_name'];
            $app_statistic_version_list = ApplicationLogic::getStatisticVersion($map,$fields)->orderBy('id','desc')->get();
            $app_statistic_version_list = Service::data($app_statistic_version_list);
//            if (!$app_statistic_version_list) ApiResponseFactory::apiResponse([],[],1000);

            foreach ($app_statistic_version_list as $k => $app_statistic_version){
                $app_statistic_version['version_release_time'] = date('Ymd', strtotime($app_statistic_version['version_release_time']));
                $app_statistic_info['channel_id'] = $app_statistic_version['channel_id'];
                $app_statistic_info['channel_name'] = $app_statistic_version['channel_name'];
                $app_statistic_info['version'][] = $app_statistic_version;
            }

            $app_statistic_list[$key] = $app_statistic_info;
        }

        $return_app_statistic_list = [];
        foreach ($app_statistic_list as $kkk => $app_statistic_value){
            if ($app_statistic_value['statistic_type'] == 1 ){
                unset($app_statistic_value['channel_id']);
                unset($app_statistic_value['channel_name']);
                unset($app_statistic_value['statistic_type']);
                foreach ($app_statistic_value as $app_statistic_k => $app_statistic_v){
                    if ($app_statistic_v){
                        if ($app_statistic_k == 'version'){
                            foreach ($app_statistic_v as $app_statistic_v_k => $app_statistic_v_v){
                                unset($app_statistic_v_v['channel_id']);
                                unset($app_statistic_v_v['channel_name']);
                                unset($app_statistic_v_v['create_time']);
                                unset($app_statistic_v_v['update_time']);
                                $app_statistic_v[$app_statistic_v_k] = $app_statistic_v_v;
                            }
                        }
                        $return_app_statistic_list['flurry'][$app_statistic_k] = $app_statistic_v;
                    }
                }
            }else if ($app_statistic_value['statistic_type'] == 2 ){
                unset($app_statistic_value['channel_id']);
                unset($app_statistic_value['channel_name']);
                unset($app_statistic_value['statistic_type']);
                foreach ($app_statistic_value as $app_statistic_k => $app_statistic_v){
                    if ($app_statistic_v){
                        if ($app_statistic_k == 'version'){
                            // 同版本，同版本参数，同发布时间配置归类
                            $new_app_statistic_v = [];
                            foreach ($app_statistic_v as $app_statistic_v_k => $app_statistic_v_v){
                                unset($app_statistic_v_v['create_time']);
                                unset($app_statistic_v_v['update_time']);
                                $app_statistic_v_v['unique_key'] = $app_statistic_v_v['app_version'].'_'.$app_statistic_v_v['statistic_version'].'_'.$app_statistic_v_v['version_release_time'];
                                $app_statistic_v[$app_statistic_v_k] = $app_statistic_v_v;
                            }

                            if($app_statistic_v){
                                foreach ($app_statistic_v as $asvk => $asvv){
                                    if(isset($asvv['unique_key']) && isset($new_app_statistic_v[$asvv['unique_key']])){
                                        $new_app_statistic_v[$asvv['unique_key']]['id'][] = $asvv['id'];
                                        $new_app_statistic_v[$asvv['unique_key']]['channel_id'][] = $asvv['channel_id'];
                                        $new_app_statistic_v[$asvv['unique_key']]['channel_name'][] = $asvv['channel_name'];
                                    }else{
                                        $new_sta_id = $asvv['id'];
                                        unset($asvv['id']);
                                        $asvv['id'][] = $new_sta_id;

                                        $new_sta_channel_id = $asvv['channel_id'];
                                        unset($asvv['channel_id']);
                                        $asvv['channel_id'][] = $new_sta_channel_id;

                                        $new_sta_channel_name = $asvv['channel_name'];
                                        unset($asvv['channel_name']);
                                        $asvv['channel_name'][] = $new_sta_channel_name;

                                        $new_sta_unique_key = $asvv['unique_key'];
                                        unset($asvv['unique_key']);

                                        $new_app_statistic_v[$new_sta_unique_key] = $asvv;
                                    }
                                }
                            }


                            if($new_app_statistic_v){
                                foreach ($new_app_statistic_v as $new_app_statistic_key => $new_app_statistic_info){
                                    $new_app_statistic_v[$new_app_statistic_key]['id'] = implode(',',$new_app_statistic_info['id']);
                                    $new_app_statistic_v[$new_app_statistic_key]['channel_id'] = implode(',',$new_app_statistic_info['channel_id']);
                                    $new_app_statistic_v[$new_app_statistic_key]['channel_name'] = implode(',',$new_app_statistic_info['channel_name']);
                                }
                            }

                            $app_statistic_v = array_values($new_app_statistic_v);

                        }


                        $return_app_statistic_list['talking'][$app_statistic_k] = $app_statistic_v;
                    }
                }

                if (isset($return_app_statistic_list['talking']['version'])){
                    $version_channel = $return_app_statistic_list['talking']['version'];
                    unset($return_app_statistic_list['talking']['version']);
                }else{
                    $version_channel = '';
                }
                $return_app_statistic_list['talking']['channel'] = $version_channel;
            }
        }


        ApiResponseFactory::apiResponse($return_app_statistic_list,[]);
    }

    /**
     * 统计信息配置增加 修改
     * @param $params array 请求数据
     */
    public static function createAppStatistic($params)
    {
        // 必填参数判断
        if (!$params) ApiResponseFactory::apiResponse([],[],300);
        $statistic_arr = [];
        $update_time = date('Y-m-d H:i:s');
        $create_time = date('Y-m-d H:i:s');
        foreach ($params as $key => $statistic_info){
            if ($key == 'flurry'){
                $statistic_type = 1;
                if (!isset($statistic_info['id']) || !$statistic_info['id']){
                    // 添加所有信息
                    $condition = ['app_id','api_key', 'statistic_app_name'];
                    $statistic_data = Service::checkField($statistic_info, $condition, ApplicationLogic::$tableFieldName);
                    $app_id = $statistic_data['app_id'];
                    $map = [];
                    $map['id'] = $app_id;
                    $app_info = ApplicationLogic::getApplicationList($map)->first();
                    $app_info = Service::data($app_info);
                    if (!$app_info) ApiResponseFactory::apiResponse([], [], 689);
                    if ($app_info['os_id'] == 1){
                        // ios Android 渠道ID是固定的
                        $statistic_data['channel_id'] = '89';
                    }else if ($app_info['os_id'] == 2){
                        $statistic_data['channel_id'] = '88';
                    }
                    $statistic_data['statistic_type'] = $statistic_type;
                }else{
                    $app_id = $statistic_info['app_id'];
                    $map = [];
                    $map['id'] = $app_id;
                    $app_info = ApplicationLogic::getApplicationList($map)->first();
                    $app_info = Service::data($app_info);
                    if (!$app_info) ApiResponseFactory::apiResponse([], [], 689);
                    if ($app_info['os_id'] == 1){
                        // ios Android 渠道ID是固定的
                        $statistic_data['channel_id'] = '89';
                    }else if ($app_info['os_id'] == 2){
                        $statistic_data['channel_id'] = '88';
                    }
                    $statistic_data['app_statistic_id'] = $statistic_info['id'];
                    $statistic_data['statistic_app_name'] = $statistic_info['statistic_app_name'];
                }

                // 添加版本信息
                $statistic_data['app_version_list'] = [];
                if (isset($statistic_info['app_version_list']) && $statistic_info['app_version_list']){
                    $app_version_list = $statistic_info['app_version_list'];
                    foreach ($app_version_list as $k => $app_version_info){
                        $condition = [ 'app_version', 'statistic_version', 'version_release_time','ad_status'];
                        $app_version_data = Service::checkField($app_version_info, $condition, ApplicationLogic::$tableFieldName);
                        $statistic_data['app_version_list'][$k] = $app_version_data;
                        $statistic_data['app_version_list'][$k]['channel_id'] = $statistic_data['channel_id'];
                    }
                    $statistic_arr[] = $statistic_data;
                }
            }else if($key == 'talkingData'){
                $statistic_type = 2;
                if (!isset($statistic_info['id']) || !$statistic_info['id']){
                    // 添加所有信息
                    $condition = ['app_id', 'td_app_id', 'access_key', 'statistic_app_name'];
                    $statistic_data = Service::checkField($statistic_info, $condition, ApplicationLogic::$tableFieldName);
                    $statistic_data['statistic_type'] = $statistic_type;
                }else{
                    $statistic_data['app_statistic_id'] = $statistic_info['id'];
                    $statistic_data['statistic_app_name'] = $statistic_info['statistic_app_name'];

                }
                // 添加版本信息
                $statistic_data['app_version_list'] = [];
                if (isset($statistic_info['app_version_list']) && $statistic_info['app_version_list']) {
                    $app_version_list = $statistic_info['app_version_list'];
                    foreach ($app_version_list as $app_version_info) {
                        $condition = ['channel_id', 'app_version', 'statistic_version', 'version_release_time', 'ad_status'];
                        $app_version_data = Service::checkField($app_version_info, $condition, ApplicationLogic::$tableFieldName);
                        $channel_id_list = explode(',',$app_version_data['channel_id']);
                        foreach ($channel_id_list as $channel_id){
                            $app_version_data['channel_id'] = $channel_id;
                            $statistic_data['app_version_list'][] = $app_version_data;
                        }
                    }
                    $statistic_arr[] = $statistic_data;
                }
            }
        }

        // 开启事物 保存数据
        DB::beginTransaction();
        foreach ($statistic_arr as $statistic){

            if (isset($statistic['app_version_list']) && $statistic['app_version_list']){
                $app_version_list = $statistic['app_version_list'];

                if (!isset($statistic['app_statistic_id']) || !$statistic['app_statistic_id']) {
                    unset($statistic['app_version_list']);
                    unset($statistic['channel_id']);

                    //校验不能重复
                    $statistic_info = ApplicationLogic::getStatistic($statistic)->first();
                    $statistic_info = Service::data($statistic_info);
                    if ($statistic_info) { // 已经重复
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([], [], 633);
                    }
                    // 保存应用数据
                    $statistic['create_time'] = $create_time;
                    $statistic['update_time'] = $update_time;
                    $new_id = ApplicationLogic::addStatistic($statistic);
                    if (!$new_id) {
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([], [], 631);
                    }
                    OperationLogImp::saveOperationLog(1,13,$new_id);
                }else{
                    $new_id = $statistic['app_statistic_id'];
                }

                // 维护应用版本信息
                if ($app_version_list){
                    foreach ($app_version_list as $app_version){
                        $app_version['statistic_app_name'] = $statistic['statistic_app_name'];
                        $app_version['app_statistic_id'] = $new_id;

                        //校验不能重复
                        $statistic_version_info = ApplicationLogic::getStatisticVersion($app_version)->first();
                        $statistic_version_info = Service::data($statistic_version_info);

                        if ($statistic_version_info){ // 已经重复
                            DB::rollBack();
                            ApiResponseFactory::apiResponse([],[],634);
                        }

                        $app_version['create_time'] = $create_time;
                        $app_version['update_time'] = $update_time;
                        $result = ApplicationLogic::addStatisticVersion($app_version);
                        if (!$result){
                            DB::rollBack();
                            ApiResponseFactory::apiResponse([],[],632);
                        }
                        OperationLogImp::saveOperationLog(1,17,$result);
                    }
                }
            }
        }

        DB::commit();

        ApiResponseFactory::apiResponse([],[]);

    }

    /**
     * 统计信息配置删除
     * @param $params array 请求数据
     */
    public static function changeAppStatisticStatus($params)
    {

        // 必填参数判断
        $ids = isset($params['id']) ? $params['id'] : ''; // 版本信息ID
        $ad_status = isset($params['ad_status']) ? $params['ad_status'] : ''; // 状态ID

        if (!$ids || !isset($params['ad_status'])) ApiResponseFactory::apiResponse([],[],635);

        $update_time = date('Y-m-d H:i:s');
        $data['update_time'] = $update_time;
        $data['ad_status'] = $ad_status;

        // 老数据
        $ids = explode(',',$ids);
        foreach ($ids as $id){
            $map['id'] = $id;
            $fields = ['id','ad_status'];
            $old_data = ApplicationLogic::getStatisticVersion($map, $fields)->first();
            $old_data = Service::data($old_data);

            $bool = ApplicationLogic::changeStatisticVersionStatus($id, $data);
            if (!$bool){
                ApiResponseFactory::apiResponse([],[],636);
            }
            if($ad_status == 2){
                OperationLogImp::saveOperationLog(3,17, $id);
            }else{
                OperationLogImp::saveOperationLog(2,17, $params,$old_data);
            }
        }

        ApiResponseFactory::apiResponse($ids,[]);
    }


    /**
     * 平台动态参数公共接口
     */
    public static function getPlatformConfiglist($params)
    {

        $type = isset($params['type']) ? $params['type'] : '';
        if (!$type) ApiResponseFactory::apiResponse([],[],827);


        // 获取平台下信息 账号
        $map = [];
        if($type == 1){
            $map['platform_type_id'] = 4;
        }elseif ($type == 2){
            $map['platform_type_id'] = 2;
        }elseif ($type == 3){
            $plat_id = isset($params['platform_id']) ? $params['platform_id'] : '';
            if (!$plat_id) ApiResponseFactory::apiResponse([],[],505);
            $map['platform_id'] = $plat_id;
        }
        $map['status'] = 1;

        $fields = ['platform_id', 'platform_name'];
        $platform_list_all = PlatformLogic::getPlatformList($map, $fields)->distinct()->get();
        $platform_list_all = Service::data($platform_list_all);
        if (!$platform_list_all) ApiResponseFactory::apiResponse([],[],1000);

        $return_platform_list = [];
        foreach($platform_list_all  as $platform_list_key => $platform_list_value){
            if (!isset($platform_list[$platform_list_value['platform_id']])) {
                $return_platform_list[$platform_list_value['platform_id']]['id'] = $platform_list_value['platform_id'];
                $return_platform_list[$platform_list_value['platform_id']]['name'] = $platform_list_value['platform_name'];
            }
        }

        // 配置类型(1,推广配置字段;2,广告配置字段;3,平台账号配置字段)
        $map = [];
        $map['conf_type_id'] = $type;
        if ($type == 3){
            if ($plat_id) $map['platform_id'] = $plat_id;
        }
        $dictionary_keys_list = PlatformLogic::getPlatformKeys($map)->get();
        $dictionary_keys_list = Service::data($dictionary_keys_list);

        if ($dictionary_keys_list) {
            foreach ($return_platform_list as $platform_id => $platform_name) {
                foreach ($dictionary_keys_list as $key => $dictionary_keys) {
                    if ($dictionary_keys['platform_id'] == $platform_id) {
                        $platform_keys = [];
                        $platform_keys['key'] = $dictionary_keys['key'];
                        $platform_keys['relative_key'] = $dictionary_keys['relative_key'];
                        $platform_keys['type'] = $dictionary_keys['key_type'];
                        $return_platform_list[$platform_id]['parameter'][] = $platform_keys;
                    }
                }
            }
        }

        if ($type == 1){
            // 处理平台信息
            foreach ($return_platform_list as $platform_id => $return_platform_info){
                $map = [];
                $map['platform_id'] = $platform_id;

                $platform_account_list = PlatformLogic::getPlatformAccountMapping($map)->get();
                $platform_account_list = Service::data($platform_account_list);
                if ($platform_account_list) {
                    foreach ($platform_account_list as $palk => $palv) {
                        $map = [];
                        $map['c_platform_agency_mapping.platform_account_id'] = $palv['id'];
                        $map['c_platform.status'] = 1;
                        $map['c_platform.platform_type_id'] = 5;

                        $fields = ['c_platform_agency_mapping.agency_platform_id','c_platform.platform_name'];

                        $map['leftjoin'] = [
                            ['c_platform','c_platform.platform_id', 'c_platform_agency_mapping.agency_platform_id']
                        ];

                        $account_agency_list = PlatformLogic::getDistinctPlatformAgenceMapping($map,$fields)->get();
                        $account_agency_list = Service::data($account_agency_list);
                        if ($account_agency_list) {
                            foreach ($account_agency_list as $aalk => $aalv) {
                                $platform_accounts = [];
                                $platform_accounts['data_account'] = $palv['account'];
                                $platform_accounts['agency_id'] = $aalv['agency_platform_id'];
                                $platform_accounts['agency_name'] = $aalv['platform_name'];
                                $return_platform_list[$platform_id]['account'][] = $platform_accounts;
                            }
                        }else {
                            $platform_accounts = [];
                            $platform_accounts['data_account'] = $palv['account'];
                            $platform_accounts['agency_name'] = '';
                            $platform_accounts['agency_id'] = 0;
                            $return_platform_list[$platform_id]['account'][] = $platform_accounts;
                        }
                    }
                }
            }
        }

        $return_platform_list = array_values($return_platform_list);
        ApiResponseFactory::apiResponse(['table_list' => $return_platform_list],[]);

    }


    /**
     * 广告信息配置列表 [废弃]
     * @param $params array 请求数据
     */
    public static function appAdList($params)
    {
        // 获取平台下信息 账号
        $map = [];
        $map['platform_type_id'] = 2;
        $map['status'] = 1;
        $fields = ['id','platform_id', 'platform_name as name','customer_id','company_id'];
        $platform_list = PlatformLogic::getPlatformList($map, $fields)->get();
        $platform_list = Service::data($platform_list);
        if (!$platform_list) ApiResponseFactory::apiResponse([],[],1000);

        $platform_account_list = [];

        foreach ($platform_list as $key => $platform){
            $platform_account_list[$platform['platform_id']]['id'] = $platform['platform_id'];
            $platform_account_list[$platform['platform_id']]['name'] = $platform['name'];

            $map = [];
            $map['platform_id'] = $platform['platform_id'];
            $map['customer_id'] = $platform['customer_id'];
            $map['company_id'] = $platform['company_id'];
            $platform_account_infos = PlatformLogic::getPlatformAccountMapping($map)->get();
            $platform_account_infos = Service::data($platform_account_infos);
            if ($platform_account_infos){
                foreach ($platform_account_infos as $palk => $palv){
                    $platform_account_list[$platform['platform_id']]['account'][] = $palv['account'];
                }
            }else{
                $platform_account_list[$platform['platform_id']]['account'][] = [];
            }
        }


        // 平台动态字段字段
        $map = [];
        $map['conf_type_id'] = 2;
        $dictionary_keys_list = PlatformLogic::getPlatformKeys($map)->get();
        $dictionary_keys_list = Service::data($dictionary_keys_list);
        $return_platform_list = [];
        if ($dictionary_keys_list){
            foreach ($platform_account_list as $p_k => $platform){
                $platform_keys = [];
                foreach ($dictionary_keys_list as $key => $dictionary_keys){
                    if ($dictionary_keys['platform_id'] == $platform['id']){
                        $platform_keys[$key]['key'] = $dictionary_keys['key'];
                        $platform_keys[$key]['relative_key'] = $dictionary_keys['relative_key'];
                        $platform_keys[$key]['type'] = $dictionary_keys['key_type'];
                    }
                }
                $platform['parameter'] = array_values($platform_keys);
                unset($platform['platform_ids']);
                $platform['account'] = array_values(array_unique($platform['account']));
                $return_platform_list[] = $platform;
            }
        }

        ApiResponseFactory::apiResponse(['table_list' => $return_platform_list],[]);

    }

    /**
     * 广告信息配置账号数据
     * @param $params array 请求数据
     */
    public static function appAdAccountList($params)
    {
        // 获取当前选择平台下信息 账号
        $app_id = isset($params['app_id']) ? $params['app_id'] : ''; // 应用ID
        $platform_id = isset($params['platform_id']) ? $params['platform_id'] : ''; // 平台ID
        if (!$app_id) ApiResponseFactory::apiResponse([],[],617);

        // 获取数据
        $map = [];
        $map['app_id'] = $app_id;
        if ($platform_id) $map['platform_id'] = $platform_id;
        $map['status'] = 1;
        $ad_platform_list = PlatformLogic::getAppAdPlatform($map)->get();
        $ad_platform_list = Service::data($ad_platform_list);
        if (!$ad_platform_list) ApiResponseFactory::apiResponse([],[],1000);

        $map = []; // 查询条件
        $map['status'] = 1;
        $map['platform_type_id'] = 2;
        $fields = ['platform_id','platform_name'];
        // 获取分页数据
        $platform_list = PlatformLogic::getPlatformList($map, $fields)->distinct()->get();
        $platform_list = Service::data($platform_list);

        // 获取平台keys
        $map = [];
        $map['conf_type_id'] = 2;
        $dictionary_keys_list = PlatformLogic::getPlatformKeys($map)->get();
        $dictionary_keys_list = Service::data($dictionary_keys_list);
        $platform_keys = [];
        // 获取每个平台的必填字段
        if ($dictionary_keys_list){
            foreach ($dictionary_keys_list as $key => $dictionary_keys){
                $platform_id = $dictionary_keys['platform_id'];
                $platform_keys[$platform_id][$dictionary_keys['key_type']][] = $dictionary_keys['relative_key'];
            }
        }

        $ad_platform_data = [];
        foreach ($ad_platform_list as $key => $ad_platform){

            $ad_platform_data[$ad_platform['id']]['id'] =  $ad_platform['id'];
            $ad_platform_data[$ad_platform['id']]['app_id'] =  $ad_platform['app_id'];
            $ad_platform_data[$ad_platform['id']]['platform_id'] =  $ad_platform['platform_id'];
            $ad_platform_data[$ad_platform['id']]['params'] =  [];
            if ($platform_list){
                foreach ($platform_list as $platform_list_k => $platform_list_v){
                    if ($platform_list_v['platform_id'] == $ad_platform['platform_id']){
                        $ad_platform_data[$ad_platform['id']]['platform_name'] =  $platform_list_v['platform_name'];
                    }
                }
            }

            if (isset($platform_keys[$ad_platform['platform_id']])){
                $platform_key_info = $platform_keys[$ad_platform['platform_id']];
                $ad_platform_data[$ad_platform['id']]['params']['dynamic_id'] = $ad_platform['id'];
                $ad_platform_data[$ad_platform['id']]['params']['redundancy_status'] = $ad_platform['redundancy_status'];
                if (isset($platform_key_info[1]) && $platform_key_info[1]){
                    foreach ($platform_key_info[1] as $kkk => $vvv){
                        $ad_platform_data[$ad_platform['id']]['params'][$vvv] = $ad_platform[$vvv];
                    }
                }
            }

        }


        if ($ad_platform_data) {
            foreach ($ad_platform_data as $kk => $ad_platform_info) {
                $app_ad_slot_map['app_ad_platform_id'] = $kk;
                $app_ad_slot_map['status'] = 1;
                $fields = ['id as sub_id','app_ad_platform_id', 'ad_slot_name', 'ad_slot_id', 'video_placement_id', 'interstitial_placement_id','ad_type'];
                $app_ad_slot_list = PlatformLogic::getAppAdSlot($app_ad_slot_map, $fields)->get();
                $app_ad_slot_list = Service::data($app_ad_slot_list);
                if ($app_ad_slot_list){
                    foreach ($app_ad_slot_list as $key => $app_ad_slot) {
                        if ($app_ad_slot['app_ad_platform_id'] == $kk) {
                            if (isset($platform_keys[$ad_platform_info['platform_id']])){
                                $platform_key_info = $platform_keys[$ad_platform_info['platform_id']];
                                if (isset($platform_key_info[2]) && $platform_key_info[2]){
                                    $ad_platform_info['params']['sub_params'][$key]['sub_id'] = $app_ad_slot['sub_id'];
                                    foreach ($platform_key_info[2] as $kkk => $vvv){
                                        $ad_platform_info['params']['sub_params'][$key][$vvv] = $app_ad_slot[$vvv];
                                    }
                                }
                            }
                        }
                    }
                }else{
                    if (isset($platform_keys[$ad_platform_info['platform_id']])){
                        $platform_key_info = $platform_keys[$ad_platform_info['platform_id']];
                        if (isset($platform_key_info[2]) && $platform_key_info[2]){
                            $key_fields_list = [];
                            foreach ($platform_key_info[2] as $kkk => $vvv){
                                $key_fields_list[$key][$vvv] = '';
                            }
                            $ad_platform_info['params']['sub_params'] = array_values($key_fields_list);
                        }
                    }
                }

                $ad_platform_data[$kk] = $ad_platform_info;
            }
        }

        $new_ad_platform_data = [];
        if($ad_platform_data){
            foreach ($ad_platform_data as $ad_platform_data_k => $ad_platform_data_v){
                if (isset($new_ad_platform_data[$ad_platform_data_v['platform_id']])){
                    if (isset($ad_platform_data_v['params']) && $ad_platform_data_v['params']){
                        $new_ad_platform_data[$ad_platform_data_v['platform_id']]['params'][] = $ad_platform_data_v['params'];
                    }
                }else{
                    $new_ad_platform_data_one = [];
                    $new_ad_platform_data_one['id'] = $ad_platform_data_v['id'];
                    $new_ad_platform_data_one['app_id'] = $ad_platform_data_v['app_id'];
                    $new_ad_platform_data_one['platform_id'] = $ad_platform_data_v['platform_id'];
                    if (isset($ad_platform_data_v['platform_name'])){
                        $new_ad_platform_data_one['platform_name'] = $ad_platform_data_v['platform_name'];
                    }else{
                        $new_ad_platform_data_one['platform_name'] = '';
                    }
                    if (isset($ad_platform_data_v['params']) && $ad_platform_data_v['params']) $new_ad_platform_data_one['params'][] = $ad_platform_data_v['params'];
                    $new_ad_platform_data[$ad_platform_data_v['platform_id']] = $new_ad_platform_data_one;
                }
            }
        }

        $new_ad_platform_data = array_values($new_ad_platform_data);
        ApiResponseFactory::apiResponse(['table_list' => $new_ad_platform_data],[]);
    }

    /**
     * 广告信息配置增加 修改
     * @param $params array 请求数据
     */
    public static function createAppAd($params)
    {
        // 必填参数判断
        if (!$params) ApiResponseFactory::apiResponse([],[],300);

        $map['conf_type_id'] = 2;
        $dictionary_keys_list = PlatformLogic::getPlatformKeys($map)->get();
        $dictionary_keys_list = Service::data($dictionary_keys_list);
        $platform_keys = [];
        // 获取每个平台的必填字段
        if ($dictionary_keys_list){
            foreach ($dictionary_keys_list as $key => $dictionary_keys){
                $platform_id = $dictionary_keys['platform_id'];
                $platform_keys[$platform_id][$dictionary_keys['key_type']][] = $dictionary_keys['relative_key'];
            }
        }

        // 验证必填字段
        unset($params['token']);
        unset($params['sessionid']);
        $app_info_list = [];

        $app_id = isset($params['app_id']) ? $params['app_id'] : '';
        if (!$app_id) ApiResponseFactory::apiResponse([],[],617);

        $platform_id = isset($params['platform_id']) ? $params['platform_id'] : '';
        if (!$platform_id) ApiResponseFactory::apiResponse([],[],505);

        $ad_config = isset($params['ad_config']) ? $params['ad_config'] : '';
        if (!$ad_config) ApiResponseFactory::apiResponse([],[],669);

        foreach ($ad_config as $p_key => $app_ad_list){
            $id = isset($app_ad_list['dynamic_id']) ? $app_ad_list['dynamic_id'] : '';
            $redundancy_status = isset($app_ad_list['redundancy_status']) ? $app_ad_list['redundancy_status'] : 1;
            // 获取平台的主键ID
            foreach ($platform_keys as $plat_id => $platform_key_info){
                if ($plat_id == $platform_id) {
                    if ($id) $app_info_list[$p_key]['id'] = $id;
                    if ($app_id) $app_info_list[$p_key]['app_id'] = $app_id;
                    if ($platform_id) $app_info_list[$p_key]['platform_id'] = $platform_id;
                    if ($redundancy_status) $app_info_list[$p_key]['redundancy_status'] = $redundancy_status;

                    if (isset($platform_key_info[1]) && $platform_key_info[1]) {
                        $app_info_list_info = Service::checkField($app_ad_list, $platform_key_info[1], ApplicationLogic::$appAdFieldName);
                        $app_info_list[$p_key] = array_merge($app_info_list[$p_key],$app_info_list_info);
                    }

                    if (isset($platform_key_info[2]) && $platform_key_info[2]) {
                        if (isset($app_ad_list['sub_params']) && $app_ad_list['sub_params']) {
                            $ad_slot_params = $app_ad_list['sub_params'];
                            foreach ($ad_slot_params as $key => $ad_slot) {
                                $app_info_list[$p_key]['sub_params'][$key] = Service::checkField($ad_slot, $platform_key_info[2], ApplicationLogic::$appAdFieldName);
                            }
                        }
                    }
                }
            }
            if(!isset($app_info_list[$p_key]))  ApiResponseFactory::apiResponse([],[],825);
        }

        $update_time = date('Y-m-d H:i:s');
        $create_time = date('Y-m-d H:i:s');
        // 开启事物 保存数据
        DB::beginTransaction();

        foreach ($app_info_list as $key => $app_info) {
            $id = isset($app_info['id']) ? $app_info['id'] : '';
            $app_id = isset($app_info['app_id']) ? $app_info['app_id'] : '';

            if ($id){ // 先删除 再添加
                // 删除原数据
                $map = [];
                $map['id'] = $id;
                $map['status'] = 1;
                $app_ad_info_all = ApplicationLogic::getAppAdList($map)->first();
                $app_ad_info_all = Service::data($app_ad_info_all);

                if ($app_ad_info_all){
                    $app_ad_platform_id = $id;
                    $update_data['status'] = 2;
                    $update_data['update_time'] = $update_time;
                    $bool = ApplicationLogic::updateAppAdPlatform($app_ad_platform_id, $update_data);
                    if (!$bool){
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([],[],636);
                    }
                    OperationLogImp::saveOperationLog(3,10,$id);

                    // 删除原关联数据
                    $map = [];
                    $map['app_ad_platform_id'] = $app_ad_platform_id;
                    $map['status'] = 1;
                    $fields = ['id'];
                    $app_ad_slot_list = ApplicationLogic::getAppAdSlot($map, $fields)->get();
                    $app_ad_slot_list = Service::data($app_ad_slot_list);
                    if ($app_ad_slot_list){
                        $map = [];
                        $map['app_ad_platform_id'] = $app_ad_platform_id;
                        $update_data['status'] = 2;
                        $update_data['update_time'] = $update_time;
                        $bool = ApplicationLogic::updateAppAdSlot($map, $update_data);
                        if (!$bool){
                            DB::rollBack();
                            ApiResponseFactory::apiResponse([],[],636);
                        }
                        foreach ($app_ad_slot_list as $key1 => $value1) {
                            OperationLogImp::saveOperationLog(3,14,$value1['id']);
                        }

                    }
                }
            }
        }
        DB::commit();

        DB::beginTransaction();
        foreach ($app_info_list as $key => $app_info) {
            $dynamic_params = isset($app_info['sub_params']) ? $app_info['sub_params'] : [];
            unset($app_info['sub_params']);
            unset($app_info['id']);

            // 验证重复性
            $app_info['status'] = 1;
            $result = ApplicationLogic::getAppAdList($app_info)->first();
            $result = Service::data($result);
            if ($result){
                DB::rollBack();
                ApiResponseFactory::apiResponse([], [], 665);
            }
            // 插入新数据
            $app_info['create_time'] = $create_time;
            $app_info['update_time'] = $update_time;
            $app_ad_platform_id = ApplicationLogic::addAppAdPlatform($app_info);
            if (!$app_ad_platform_id){
                DB::rollBack();
                ApiResponseFactory::apiResponse([],[],666);
            }
            // 保存日志
            OperationLogImp::saveOperationLog(1,10,$app_ad_platform_id);

            // 原关联数据
            if ($dynamic_params){
                foreach ($dynamic_params as $kkk => $value){
                    // 验证重复性
                    $map = [];
                    $map['status'] = 1;
                    $map[] = ['app_ad_platform_id', '=', $app_ad_platform_id];
                    $map = array_merge($map, $value);
                    $result = ApplicationLogic::getAppAdSlot($map)->first();
                    $result = Service::data($result);
                    if ($result){
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([], [], 667);
                    }

                    // 插入新数据
                    $value['status'] = 1;
                    $value['app_ad_platform_id'] = $app_ad_platform_id;
                    $value['create_time'] = $create_time;
                    $value['update_time'] = $update_time;
                    $id = ApplicationLogic::addAppAdSlot($value);
                    if (!$id){
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([],[],668);
                    }
                    OperationLogImp::saveOperationLog(1,14,$id);
                }
            }
        }

        DB::commit();
        ApiResponseFactory::apiResponse([],[]);
    }

    /**
     * 广告信息配置删除
     * @param $params array 请求数据
     */
    public static function changeAppAdStatus($params)
    {
        // 必填参数判断
        $type = isset($params['type']) ? $params['type'] : ''; // 删除级别
        $ad_status = isset($params['status']) ? $params['status'] : 2; // 状态ID
        if (!$type) ApiResponseFactory::apiResponse([],[],828);

        $update_time = date('Y-m-d H:i:s');
        // 1:平台 2:第一级参数 3:第二级参数
        if ($type == 1){
            $app_id = isset($params['app_id']) ? $params['app_id'] : ''; // 应用ID
            $platform_id = isset($params['platform_id']) ? $params['platform_id'] : ''; // 平台ID

            // 删除原数据
            $map = [];
            $map['app_id'] = $app_id;
            $map['platform_id'] = $platform_id;
            $map['status'] = 1;
            $app_ad_info_all = ApplicationLogic::getAppAdList($map)->get();
            $app_ad_info_all = Service::data($app_ad_info_all);

            if ($app_ad_info_all){
                DB::beginTransaction();
                foreach ($app_ad_info_all as $app_ad_info_one){
                    $app_ad_platform_id = $app_ad_info_one['id'];
                    $update_data['status'] = 2;
                    $update_data['update_time'] = $update_time;
                    $bool = ApplicationLogic::updateAppAdPlatform($app_ad_platform_id, $update_data);
                    if (!$bool){
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([],[],636);
                    }
                    OperationLogImp::saveOperationLog(3,10,$app_ad_platform_id);

                    // 删除原关联数据
                    $map = [];
                    $map['app_ad_platform_id'] = $app_ad_platform_id;
                    $map['status'] = 1;
                    $fields = ['id'];
                    $app_ad_slot_list = ApplicationLogic::getAppAdSlot($map, $fields)->get();
                    $app_ad_slot_list = Service::data($app_ad_slot_list);
                    if ($app_ad_slot_list){
                        $map = [];
                        $map['app_ad_platform_id'] = $app_ad_platform_id;
                        $update_data['status'] = 2;
                        $update_data['update_time'] = $update_time;
                        $bool = ApplicationLogic::updateAppAdSlot($map, $update_data);
                        if (!$bool){
                            DB::rollBack();
                            ApiResponseFactory::apiResponse([],[],636);
                        }
                        foreach ($app_ad_slot_list as $key1 => $value1) {
                            OperationLogImp::saveOperationLog(3,14,$value1['id']);
                        }

                    }
                }
                DB::commit();
            }

        }elseif ($type == 2){
            $id = isset($params['id']) ? $params['id'] : ''; // ID
            if ($id){ // 先删除 再添加
                DB::beginTransaction();
                // 删除原数据
                $map = [];
                $map['id'] = $id;
                $map['status'] = 1;
                $app_ad_info_all = ApplicationLogic::getAppAdList($map)->first();
                $app_ad_info_all = Service::data($app_ad_info_all);

                if ($app_ad_info_all){
                    $app_ad_platform_id = $id;
                    $update_data['status'] = 2;
                    $update_data['update_time'] = $update_time;
                    $bool = ApplicationLogic::updateAppAdPlatform($app_ad_platform_id, $update_data);
                    if (!$bool){
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([],[],636);
                    }
                    OperationLogImp::saveOperationLog(3,10,$id);

                    // 删除原关联数据
                    $map = [];
                    $map['app_ad_platform_id'] = $app_ad_platform_id;
                    $map['status'] = 1;
                    $fields = ['id'];
                    $app_ad_slot_list = ApplicationLogic::getAppAdSlot($map, $fields)->get();
                    $app_ad_slot_list = Service::data($app_ad_slot_list);
                    if ($app_ad_slot_list){
                        $map = [];
                        $map['app_ad_platform_id'] = $app_ad_platform_id;
                        $update_data['status'] = 2;
                        $update_data['update_time'] = $update_time;
                        $bool = ApplicationLogic::updateAppAdSlot($map, $update_data);
                        if (!$bool){
                            DB::rollBack();
                            ApiResponseFactory::apiResponse([],[],636);
                        }
                        foreach ($app_ad_slot_list as $key1 => $value1) {
                            OperationLogImp::saveOperationLog(3,14,$value1['id']);
                        }

                    }
                }
                DB::commit();
            }

        }elseif($type == 3){
            $id = isset($params['id']) ? $params['id'] : ''; // 版本信息ID
            $data['update_time'] = $update_time;
            $data['status'] = $ad_status;
            // 老数据
            $map['id'] = $id;

            DB::beginTransaction();
            $bool = ApplicationLogic::updateAppAdSlot($map, $data);
            if (!$bool){
                DB::rollBack();
                ApiResponseFactory::apiResponse([],[],636);
            }
            DB::commit();
            // 保存日志
            OperationLogImp::saveOperationLog(3,14, $id);
        }

        ApiResponseFactory::apiResponse([],[]);

    }

    /**
     * 计费信息配置列表
     * @param $params array 请求数据
     */
    public static function appBillingList($params)
    {
        $page = isset($params['page']) ? $params['page'] : 1 ;
        $page_size = isset($params['pageSize']) ? $params['pageSize'] : 9000 ;
        $appid = isset($params['appid']) ? $params['appid'] :'';//应用的自增ID
        if(empty($appid)) ApiResponseFactory::apiResponse([],[],701);
        $app_map['id'] = $appid;
        $app_map['status'] = 1;
        $app_list = ApplicationLogic::getApplicationList($app_map)->get();
        $app_list = Service::data($app_list);
        $map =[];
        $map['app_id'] = $appid;
        $map['status'] = 1;
        // 获取Statistic数据
        $fields = ['c_billing.*','c_channel.channel_name'];
        $map['leftjoin'] = [
            ['c_channel','c_channel.id', 'c_billing.channel_id']
        ];
        $app_billing_statistic_list = ApplicationLogic::changeStatisticBillingList($map,$fields)->forPage($page,$page_size)->get();
        $app_billing_statistic_list = Service::data($app_billing_statistic_list);
        if (!$app_billing_statistic_list) ApiResponseFactory::apiResponse([],[],1000);

        $map =[];
        $map['app_id'] = $appid;
        $map['status'] = 0;
        // 获取Statistic Point数据
        $app_billing_point_statistic_list = ApplicationLogic::changeStatisticBillingPoinList($map)->forPage($page,$page_size)->get();
        $app_billing_point_statistic_list = Service::data($app_billing_point_statistic_list);
        if (!$app_billing_point_statistic_list) ApiResponseFactory::apiResponse([],[],1000);
        $data =[];
        $index =0;
        //计费的基础信息
        foreach ($app_billing_statistic_list as $key => $app_billing){
            //os_id 1 ios计费不分国内国外    2 安卓 区分国内国外计费
            if($app_list[0]['os_id'] ==  1 || ($app_list[0]['os_id'] ==  2 && $app_list[0]['release_region_id'] ==1)){
                $data['overseas']['app_package_name'] = $app_billing['app_package_name'];
            }elseif (($app_list[0]['os_id'] ==  2 && $app_list[0]['release_region_id'] ==3)){
                $data['domestic']['billing_list'][$index]['billing_id']= $app_billing['id'];
                $data['domestic']['billing_list'][$index]['channel_id']= $app_billing['channel_id'];
                $data['domestic']['billing_list'][$index]['channel_name']= $app_billing['channel_name'];
                $data['domestic']['billing_list'][$index]['billing_app_name'] = $app_billing['billing_app_name'];
                $data['domestic']['billing_list'][$index]['billing_app_id'] = $app_billing['billing_app_id'];
                $data['domestic']['billing_list'][$index]['pay_platform_id'] = $app_billing['pay_platform_id'];
                $data['domestic']['billing_list'][$index]['app_package_name'] = $app_billing['app_package_name'];
                $index ++;
            }
        }
        $num =0;
        $num1 =0;
        //计费点信息
        foreach ($app_billing_point_statistic_list as $k => $app_billing_point){
            //1、海外计费信息2、国内计费信息
            if ($app_list[0]['os_id'] ==  1 || ($app_list[0]['os_id'] ==  2 && $app_list[0]['release_region_id'] ==1)){
                $data['overseas']['bill_point_list'][$num]['id'] = $app_billing_point['id'];
                $data['overseas']['bill_point_list'][$num]['billing_point_name'] = $app_billing_point['billing_point_name'];
                $data['overseas']['bill_point_list'][$num]['billing_point_id'] = $app_billing_point['billing_point_id'];
                $data['overseas']['bill_point_list'][$num]['billing_point_price_usd'] = $app_billing_point['billing_point_price_usd'];
                $data['overseas']['bill_point_list'][$num]['billing_point_price_cny'] = $app_billing_point['billing_point_price_cny'];
                $data['overseas']['bill_point_list'][$num]['rebate_billing_point_price_usd'] = $app_billing_point['rebate_billing_point_price_usd'];
                $data['overseas']['bill_point_list'][$num]['rebate_begin_time'] = $app_billing_point['rebate_begin_time'];
                $data['overseas']['bill_point_list'][$num]['rebate_end_time'] = $app_billing_point['rebate_end_time'];
                $data['overseas']['bill_point_list'][$num]['currency_type'] = $app_billing_point['currency_type'];
                $num ++;
            }elseif(($app_list[0]['os_id'] ==  2 && $app_list[0]['release_region_id'] ==3)){
                $data['domestic']['bill_point_list'][$num1]['id'] = $app_billing_point['id'];
                $data['domestic']['bill_point_list'][$num1]['billing_point_name'] = $app_billing_point['billing_point_name'];
                $data['domestic']['bill_point_list'][$num1]['billing_point_id'] = $app_billing_point['billing_point_id'];
                $data['domestic']['bill_point_list'][$num1]['billing_point_price_cny'] = $app_billing_point['billing_point_price_cny'];
                $data['domestic']['bill_point_list'][$num1]['rebate_billing_point_price_cny'] = $app_billing_point['rebate_billing_point_price_cny'];
                $data['domestic']['bill_point_list'][$num1]['rebate_begin_time'] = $app_billing_point['rebate_begin_time'];
                $data['domestic']['bill_point_list'][$num1]['rebate_end_time'] = $app_billing_point['rebate_end_time'];
                $num1 ++;
            }
        }
        ApiResponseFactory::apiResponse($data,[]);
    }

    /**
     * 计费信息配置增加
     * @param $params array 请求数据
     */
    public static function createAppBilling($params)
    {
        //var_dump($params);die;
        // 必填参数判断
        if (!$params) ApiResponseFactory::apiResponse([],[],300);
        $statistic_arr = [];
        $update_time = date('Y-m-d H:i:s');
        $create_time = date('Y-m-d H:i:s');

        if((!isset($params['overseas']) && !isset($params['domestic'])) ||  $params==null) ApiResponseFactory::apiResponse([],[],700);

        $bill_index =0;
        $bill_point_index =0;
        $data =[];
        //海外
        $map=[];
        if(!empty($params['overseas'])){
            if(!isset($params['overseas']['appid'])){
                ApiResponseFactory::apiResponse([],[],701);
            }
            if(!isset($params['overseas']['app_package_name'])){

                ApiResponseFactory::apiResponse([],[],702);
            }

            $app_map['id'] = $params['overseas']['appid'];
            $app_map['status'] = 1;
            $app_list = ApplicationLogic::getApplicationList($app_map)->first();
            $app_list = Service::data($app_list);
            if($app_list['os_id'] ==1){
                $channel_id ='89';
                $platform_id ='pff03';
            }elseif($app_list['os_id'] ==2){
                $channel_id ='88';
                $platform_id ='pff02';
            }

            $map['app_id'] =$params['overseas']['appid'];
            //整理数据
            $data['bill_list'][$bill_index]['pay_platform_id']=$platform_id;
            $data['bill_list'][$bill_index]['channel_id']=$channel_id;
            $data['bill_list'][$bill_index]['app_package_name']=$params['overseas']['app_package_name'];
            $data['bill_list'][$bill_index]['status']=1;
            $data['bill_list'][$bill_index]['create_time']=$create_time;
            $data['bill_list'][$bill_index]['update_time']=$update_time;
            $data['bill_list'][$bill_index]['app_id']=$params['overseas']['appid'];
            $bill_index++;
            foreach ($params['overseas']['bill_point_list'] as $key => $value) {
                //判断这些字段不能为空
//                $condition = ['billing_point_name', 'billing_point_id', 'billing_point_price_usd','billing_point_price_cny'];
                $condition = ['billing_point_name', 'billing_point_id', 'billing_point_price_usd'];
                $statistic_data = Service::checkField($value, $condition, ApplicationLogic::$tableFieldName);

                if(!is_numeric($value['billing_point_price_usd']))  ApiResponseFactory::apiResponse([],[],707);
                //if(!is_numeric($value['billing_point_price_cny']))  ApiResponseFactory::apiResponse([],[],708);
                // if(isset($value['rebate_billing_point_price_usd'])){
                //     if(!$value['rebate_billing_point_price_usd']) ApiResponseFactory::apiResponse([],[],756);
                //     if(!$value['rebate_begin_time']) ApiResponseFactory::apiResponse([],[],757);
                //     if(!$value['rebate_end_time']) ApiResponseFactory::apiResponse([],[],758);
                //     if($value['rebate_begin_time']>$value['rebate_end_time']) ApiResponseFactory::apiResponse([],[],759);
                //     if(!is_numeric($value['rebate_billing_point_price_usd']))  ApiResponseFactory::apiResponse([],[],760);

                // }

                //判断 计费点名称 or 计费点id 不能重复
                $map = [];
                $map['status'] = 0;
                $map['app_id'] = $params['overseas']['appid'];
//                $map['orWhere'][] = ['billing_point_name',$value['billing_point_name']];
                $map['orWhere'][] = ['billing_point_id',$value['billing_point_id']];
//                $map['billing_point_price_usd'] =$value['billing_point_price_usd'];
//                $map['billing_point_price_cny'] =$value['billing_point_price_cny'];

                $checkBillPoint = ApplicationLogic::changeStatisticBillingPoinList($map)->first();
                $checkBillPoint =Service::data($checkBillPoint);
                if(!empty($checkBillPoint))  ApiResponseFactory::apiResponse([],[],709);
                //整理数据
                $data['billing_point_list'][$bill_point_index]['billing_point_name']=$value['billing_point_name'];
                $data['billing_point_list'][$bill_point_index]['status']=0;
                $data['billing_point_list'][$bill_point_index]['create_time']=$create_time;
                $data['billing_point_list'][$bill_point_index]['update_time']=$update_time;
                $data['billing_point_list'][$bill_point_index]['billing_point_id']=$value['billing_point_id'];
                $data['billing_point_list'][$bill_point_index]['currency_type']= $value['currency_type'];
                $data['billing_point_list'][$bill_point_index]['billing_point_price_usd']=$value['billing_point_price_usd'];
                //$data['billing_point_list'][$bill_point_index]['billing_point_price_cny']=$value['billing_point_price_cny'];
                if(isset($value['rebate_billing_point_price_usd']) && isset($value['rebate_begin_time']) && isset($value['rebate_end_time'])){
                    $data['billing_point_list'][$bill_point_index]['rebate_billing_point_price_usd']=$value['rebate_billing_point_price_usd'];
                    $data['billing_point_list'][$bill_point_index]['rebate_begin_time']=$value['rebate_begin_time'];
                    $data['billing_point_list'][$bill_point_index]['rebate_end_time']=$value['rebate_end_time'];
                }

                $data['billing_point_list'][$bill_point_index]['app_id']=$params['overseas']['appid'];
                $bill_point_index++;
            }
        }
        unset($map);
        //国内
        if(!empty($params['domestic'])){
            if(!isset($params['domestic']['appid'])){
                ApiResponseFactory::apiResponse([],[],701);
            }
            $map['app_id'] =$params['domestic']['appid'];
            foreach ($params['domestic']['billing_list'] as $billing_key => $billing_value) {
                //判断这些字段不能为空
//                    $condition = ['channel_id', 'billing_app_name', 'billing_app_id','pay_platform_id','app_package_name'];
                $condition = ['channel_id', 'pay_platform_id','app_package_name'];
                $statistic_data = Service::checkField($billing_value, $condition, ApplicationLogic::$tableFieldName);
                //判断 同一个应用下计费点名称 计费点id 计费点价格(USD) 计费点价格(CNY)
                $map['channel_id'] =$billing_value['channel_id'];
                $map['status'] = 1;
                $map['billing_app_name'] =$billing_value['billing_app_name'];
                $map['billing_app_id'] =$billing_value['billing_app_id'];
                $map['pay_platform_id'] =$billing_value['pay_platform_id'];
                $map['app_package_name'] =$billing_value['app_package_name'];

                $checkBill= ApplicationLogic::changeStatisticBillingList($map)->first();
                $checkBill =Service::data($checkBill);
                if(!empty($checkBill))  ApiResponseFactory::apiResponse([],[],714);
                $data['bill_list'][$bill_index]['channel_id']=$billing_value['channel_id'];
                $data['bill_list'][$bill_index]['billing_app_name']=$billing_value['billing_app_name'];
                $data['bill_list'][$bill_index]['billing_app_id']=$billing_value['billing_app_id'];
                $data['bill_list'][$bill_index]['pay_platform_id']=$billing_value['pay_platform_id'];
                $data['bill_list'][$bill_index]['app_package_name']=$billing_value['app_package_name'];
                $data['bill_list'][$bill_index]['status']=1;
                $data['bill_list'][$bill_index]['create_time']=$create_time;
                $data['bill_list'][$bill_index]['update_time']=$update_time;
                $data['bill_list'][$bill_index]['app_id']=$params['domestic']['appid'];
                $bill_index++;
            }
            unset($map);
            $map['app_id'] =$params['domestic']['appid'];
            foreach ($params['domestic']['bill_point_list'] as $billing_point_key => $billing_point_value) {
                //判断这些字段不能为空
//                $condition = ['billing_point_name', 'billing_point_id','billing_point_price_cny'];
                $condition = ['billing_point_name'];
                $statistic_data = Service::checkField($billing_point_value, $condition, ApplicationLogic::$tableFieldName);
                if(!is_numeric($billing_point_value['billing_point_price_cny']))  ApiResponseFactory::apiResponse([],[],710);
                // if(isset($value['rebate_billing_point_price_cny'])){
                //     if(!$billing_point_value['rebate_billing_point_price_cny']) ApiResponseFactory::apiResponse([],[],756);
                //     if(!$billing_point_value['rebate_begin_time']) ApiResponseFactory::apiResponse([],[],757);
                //     if(!$billing_point_value['rebate_end_time']) ApiResponseFactory::apiResponse([],[],758);
                //     if($billing_point_value['rebate_begin_time']>$billing_point_value['rebate_end_time']) ApiResponseFactory::apiResponse([],[],759);
                //     if(!is_numeric($billing_point_value['rebate_billing_point_price_cny']))  ApiResponseFactory::apiResponse([],[],760);
                //     if(!is_numeric($billing_point_value['billing_point_price_cny']))  ApiResponseFactory::apiResponse([],[],708);
                // }

                //判断 计费点名称 or 计费点id 不能存在
                $map = [];
                $map['status'] = 0;
                $map['orWhere'][] = ['billing_point_name',$billing_point_value['billing_point_name']];
                $map['orWhere'][] = ['billing_point_id',$billing_point_value['billing_point_id']];

                $checkBillPoint = ApplicationLogic::changeStatisticBillingPoinList($map)->first();
                $checkBillPoint =Service::data($checkBillPoint);

                if(!empty($checkBillPoint))  ApiResponseFactory::apiResponse([],[],715);
                $data['billing_point_list'][$bill_point_index]['billing_point_name']=$billing_point_value['billing_point_name'];
                $data['billing_point_list'][$bill_point_index]['status']=0;
                $data['billing_point_list'][$bill_point_index]['create_time']=$create_time;
                $data['billing_point_list'][$bill_point_index]['update_time']=$update_time;
                $data['billing_point_list'][$bill_point_index]['billing_point_id']=$billing_point_value['billing_point_id'];
                $data['billing_point_list'][$bill_point_index]['billing_point_price_cny']=$billing_point_value['billing_point_price_cny'];
                if(isset($value['rebate_billing_point_price_cny']) && isset($value['rebate_begin_time']) && isset($value['rebate_end_time'])){
                    $data['billing_point_list'][$bill_point_index]['rebate_billing_point_price_cny']=$billing_point_value['rebate_billing_point_price_cny'];
                    $data['billing_point_list'][$bill_point_index]['rebate_begin_time']=$billing_point_value['rebate_begin_time'];
                    $data['billing_point_list'][$bill_point_index]['rebate_end_time']=$billing_point_value['rebate_end_time'];
                }
                $data['billing_point_list'][$bill_point_index]['app_id']=$params['domestic']['appid'];
                $bill_point_index++;
            }
        }




        // 开启事物 保存数据
        $new_id = '';
        DB::beginTransaction();
        foreach ($data as $k =>$v){
            if($k =='bill_list'){
                foreach ($v as $key1 => $value1) {
                    $new_id = ApplicationLogic::addBill($value1);
                    if (!$new_id) {
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([], [], 716);
                    }
                    OperationLogImp::saveOperationLog(1,12,$new_id);
                    # code...
                }

            }elseif($k =='billing_point_list'){
                foreach ($v as $key2 => $value2) {
                    $new_id = ApplicationLogic::addBillPoint($value2);
                    if (!$new_id) {
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([], [], 716);
                    }
                    OperationLogImp::saveOperationLog(1,16,$new_id);
                }

            }

        }

        DB::commit();
        $new_id = $new_id ? $new_id : '';
        ApiResponseFactory::apiResponse($new_id,[]);
    }

    /**
     * 计费信息配置删除
     * @param $params array 请求数据
     */
    public static function changeAppBillingStatus($params)
    {
        // 必填参数判断
        if (!$params) ApiResponseFactory::apiResponse([],[],300);

        $update_time = date('Y-m-d H:i:s');
        $data['update_time'] = $update_time;
        $data['status'] = 1;
        $id= $params['id'];
        //1、代表操作计费基础信息2、代表操作计费点信息
        if($params['type'] ==1){
            $data['status'] = 0;
            $bool = ApplicationLogic::changeBillingStatus($id, $data);
            OperationLogImp::saveOperationLog(3,16, $id);

        }elseif ($params['type'] ==2) {
            $bool = ApplicationLogic::changeBillingPointStatus($id, $data);
            OperationLogImp::saveOperationLog(3,16,$id);
        }
        if (!$bool){

            ApiResponseFactory::apiResponse([],[],717);
        }
        ApiResponseFactory::apiResponse($id,[]);
    }


    /**
     * 计费信息折扣价格配置修改
     * @param $params array 请求数据
     */
    public static function changeAppBillingRebate($params)
    {
        // 必填参数判断
        if (!$params) ApiResponseFactory::apiResponse([],[],300);

        $update_time = date('Y-m-d H:i:s');


        DB::beginTransaction();
        $id= $params['id'];
        $data_status['status'] = 1;
        $data_status['update_time'] = $update_time;
        $bool = ApplicationLogic::changeBillingPointStatus($id, $data_status);

        $data['app_id']= isset($params['appid'])?$params['appid']:'';
        $data['billing_point_name']= isset($params['billing_point_name'])?$params['billing_point_name']:'';
        $data['billing_point_id']= isset($params['billing_point_id'])?$params['billing_point_id']:'';
        $data['billing_point_price_cny']= isset($params['billing_point_price_cny'])?$params['billing_point_price_cny']:'';
        $data['billing_point_price_usd']= isset($params['billing_point_price_usd'])?$params['billing_point_price_usd']:'';
        $data['rebate_billing_point_price_cny']= isset($params['rebate_billing_point_price_cny'])?$params['rebate_billing_point_price_cny']:'';
        $data['rebate_billing_point_price_usd']= isset($params['rebate_billing_point_price_usd'])?$params['rebate_billing_point_price_usd']:'';
        $data['rebate_begin_time']= isset($params['rebate_begin_time']) && $params['rebate_begin_time'] ? $params['rebate_begin_time'] : NULL;
        $data['rebate_end_time']= isset($params['rebate_end_time']) && $params['rebate_end_time'] ? $params['rebate_end_time'] : NULL;
        $data['currency_type']= isset($params['currency_type'])  ? $params['currency_type'] : NULL;
        $new_id = ApplicationLogic::addBillPoint($data);

        if ($bool && $new_id){

            DB::commit();
            OperationLogImp::saveOperationLog(1,16,$new_id);
            OperationLogImp::saveOperationLog(3,16,$id);
        }else{
            DB::rollBack();
            ApiResponseFactory::apiResponse([],[],719);
        }
        //OperationLogImp::saveOperationLog(2,16, $params, $old_data);
        ApiResponseFactory::apiResponse($id,[]);
    }

    /**
     * 推广信息配置列表 [废弃]
     * @param $params array 请求数据
     */
    public static function appGeneralizeList($params)
    {

        // 第一步 获取推广平台信息
        $map = [];
        $map['platform_type_id'] = 4;
        $map['status'] = 1;
        $fields = ['platform_id as id','platform_name as name'];

        $platform_name_list = PlatformLogic::getPlatformList($map, $fields)->groupBy(['platform_id','platform_name'])->get();
        $platform_name_list = Service::data($platform_name_list);
        if (!$platform_name_list) ApiResponseFactory::apiResponse([],[],1000);

        // 第二步 获取平台账号
        foreach ($platform_name_list as $pnlk => $pnlv){
            $map = [];
            $map['platform_id'] = $pnlv['id'];
            $platform_account_list = PlatformLogic::getPlatformAccountMapping($map)->get();
            $platform_account_list = Service::data($platform_account_list);

            if ($platform_account_list){
                foreach ($platform_account_list as $palk => $palv){
                    $map = [];
                    $map['platform_account_id'] = $palv['id'];
                    $account_agency_list = PlatformLogic::getPlatformAgenceMapping($map)->get();
                    $account_agency_list = Service::data($account_agency_list);
                    if ($account_agency_list){
                        foreach ($account_agency_list as $aalk => $aalv){
                            $pnlv['account'][$palk]['data_account'] = $palv['account'];
                            $pnlv['account'][$palk]['agency_ids'][] = $aalv['agency_platform_id'];
                        }
                    }else{
                        $pnlv['account'][$palk]['data_account'] = $palv['account'];
                        $pnlv['account'][$palk]['agency_ids'][] = 0;
                    }
                }
            }
            $platform_name_list[$pnlk] = $pnlv;
        }


        $map = [];
        $fields = ['c_dictionary_keys.*'];
        // 平台动态字段字段
        $map['conf_type_id'] = 1;
        $dictionary_keys_list = PlatformLogic::getPlatformKeys($map, $fields)->get();
        $dictionary_keys_list = Service::data($dictionary_keys_list);

        $platform_keys = [];
        if ($dictionary_keys_list){
            // 处理平台信息
            foreach ($dictionary_keys_list as $key => $dictionary_keys){


                if ($dictionary_keys['agency_platform_id']){
                    $map = [];
                    $map['platform_id'] = $dictionary_keys['agency_platform_id'];
                    $map['platform_type_id'] = 5;
                    $map['status'] = 1;
                    $fields = ['platform_id','platform_name'];
                    $platform_agency_info = PlatformLogic::getPlatformList($map, $fields)->first();
                    $platform_agency_info = Service::data($platform_agency_info);
                    $platform_agency_name = $platform_agency_info['platform_name'];
                    $dictionary_keys_list[$key]['agency_name'] = $platform_agency_name;
                }else{
                    $dictionary_keys_list[$key]['agency_name'] = '';
                }


                foreach ($platform_name_list as $kkkk => $platform_name_info){
                    if ($dictionary_keys['platform_id'] == $platform_name_info['id']){
                        $platform_keys[$dictionary_keys['platform_id']]['id'] = $dictionary_keys['platform_id'];
                        $platform_keys[$dictionary_keys['platform_id']]['name'] = $dictionary_keys['platform_name'];
                        $platform_keys[$dictionary_keys['platform_id']]['account'] = isset($platform_name_info['account']) ? $platform_name_info['account'] : '';
                    }
                }
            }

            $platform_keys = array_values($platform_keys);

            // 处理代理商信息
            foreach ($platform_keys as $k => $v){
                foreach ($dictionary_keys_list as $key => $value){
                    if ($v['id'] == $value['platform_id']){
                        $v['agency'][$key]['agency_id'] = $value['agency_platform_id'];
                        $v['agency'][$key]['agency_name'] = $value['agency_name'];
                    }
                }
                $v['agency'] = array_values(array_unique($v['agency'],SORT_REGULAR));
                $platform_keys[$k] = $v;
            }

            // 处理字段信息
            foreach ($platform_keys as $k => $v){
                foreach ($dictionary_keys_list as $key => $value){
                    if ($v['id'] == $value['platform_id']){
                        foreach ($v['agency'] as $a_k => $a_v){
                            if ($value['agency_platform_id'] == $a_v['agency_id']){
                                $a_v['parameter'][$key]['key'] = $value['key'];
                                $a_v['parameter'][$key]['relative_key'] = $value['relative_key'];
                                $a_v['parameter'][$key]['type'] = $value['key_type'];
                            }
                            $v['agency'][$a_k] = $a_v;
                        }
                    }
                }
                $platform_keys[$k] = $v;
            }



            // 处理数据
            foreach ($platform_keys as $p_k => $platform_list){
                foreach ($platform_list['agency'] as $k => $agency_list ){
                    $platform_list['agency'][$k]['parameter'] = array_values($agency_list['parameter']);
                }
                $platform_keys[$p_k] = $platform_list;
            }
        }

        ApiResponseFactory::apiResponse(['table_list' => $platform_keys],[]);
    }

    /**
     * 推广信息配置代理平台数据
     * @param $params array 请求数据
     */
    public static function appGeneralizeAgencyList($params)
    {
        // 获取当前选择平台下信息 账号
        $app_id = isset($params['app_id']) ? $params['app_id'] : ''; // 平台ID
        $agency_id = isset($params['agency_id']) ? $params['agency_id'] : ''; // 平台ID
        if (!$app_id) ApiResponseFactory::apiResponse([],[],617);

        // 获取数据
        $map = [];
        $map['app_id'] = $app_id;
        if ($agency_id) $map['agency_platform_id'] = $agency_id;
        $map['generalize_status'] = 1;

        $app_generalize_list = ApplicationLogic::getAppGeneralizeList($map)->get();
        $app_generalize_list = Service::data($app_generalize_list);
        if (!$app_generalize_list) ApiResponseFactory::apiResponse([],[],1000);

        // 获取平台名称信息
        foreach ($app_generalize_list as $key => $app_generalize){
            $platform_id = $app_generalize['platform_id'];
            $agency_platform_id = $app_generalize['agency_platform_id'];

            if ($platform_id){
                $map = [];
                $map['platform_id'] = $platform_id;
                $fields = ['id','platform_name'];
                $platform_info = PlatformLogic::getPlatformList($map, $fields)->first();
                $platform_info = Service::data($platform_info);
                $app_generalize['platform_name'] = $platform_info['platform_name'];
            }

            if ($agency_platform_id){
                $map = [];
                $map['platform_id'] = $agency_platform_id;
                $fields = ['id','platform_name as agency_platform_name'];
                $agency_platform_info = PlatformLogic::getPlatformList($map, $fields)->first();
                $agency_platform_info = Service::data($agency_platform_info);
                $app_generalize['agency_platform_name'] = $agency_platform_info['agency_platform_name'];
            }else{
                $app_generalize['agency_platform_name'] = '';
            }
            $app_generalize_list[$key] = $app_generalize;
        }

        // 拼接代理商信息
        $app_generalize_data = [];
        foreach ($app_generalize_list as $key => $app_generalize){
            $app_generalize_data[$app_generalize['id']]['id'] =  $app_generalize['id'];
            $app_generalize_data[$app_generalize['id']]['platform_id'] =  $app_generalize['platform_id'];
            $app_generalize_data[$app_generalize['id']]['platform_name'] =  $app_generalize['platform_name'];
            $app_generalize_data[$app_generalize['id']]['data_account'] =  $app_generalize['data_account'];
            $app_generalize_data[$app_generalize['id']]['application_id'] =  $app_generalize['application_id'];
            $app_generalize_data[$app_generalize['id']]['agency_id'] =  $app_generalize['agency_platform_id'];
            $app_generalize_data[$app_generalize['id']]['agency_name'] =  $app_generalize['agency_platform_name'];
            $app_generalize_data[$app_generalize['id']]['agency']['agency_id'] =  $app_generalize['agency_platform_id'];
            $app_generalize_data[$app_generalize['id']]['agency']['params'] = [];

        }

        // 获取平台keys
        $map = [];
        $map['conf_type_id'] = 1;
        $dictionary_keys_list = PlatformLogic::getPlatformKeys($map)->get();
        $dictionary_keys_list = Service::data($dictionary_keys_list);

        $platform_keys = [];
        // 获取每个平台的必填字段
        if ($dictionary_keys_list){
            foreach ($dictionary_keys_list as $key => $dictionary_keys){
                $platform_id = $dictionary_keys['platform_id'];
                $platform_keys[$platform_id][$dictionary_keys['key_type']][] = $dictionary_keys['relative_key'];
            }
        }



        // 拼接不可变动态参数
        $app_generalize_data = array_values($app_generalize_data);
        foreach ($app_generalize_data as $key => $app_generalize){
            $agency_list = $app_generalize['agency'];
            foreach ($app_generalize_list as $a_l_k => $a_l_generalize) {
                if ($a_l_generalize['platform_id'] == $app_generalize['platform_id'] && $a_l_generalize['data_account'] == $app_generalize['data_account'] && $a_l_generalize['application_id'] == $app_generalize['application_id']) {
                    if ($a_l_generalize['agency_platform_id'] == $agency_list['agency_id']) {
                        $n_platform_id = $a_l_generalize['platform_id'];
                        if (isset($platform_keys[$n_platform_id])){
                            $platform_key_info = $platform_keys[$n_platform_id];
                            $agency_list['params']['dynamic_id'] = $a_l_generalize['id'];
                            $agency_list['params']['redundancy_generalize_status'] = $a_l_generalize['redundancy_generalize_status'];
                            if (isset($platform_key_info[1]) && $platform_key_info[1]){
                                foreach ($platform_key_info[1] as $kkk => $vvv){
                                    $agency_list['params'][$vvv] = $a_l_generalize[$vvv];
                                    $agency_list['params']['sub_params'] = [];
                                }
                            }

                        }else{
                            $agency_list['params']= [];
                            $agency_list['params']['sub_params'] = [];
                        }
                    }
                }
            }
            $app_generalize['agency'] = $agency_list;
            $app_generalize_data[$key] = $app_generalize;
        }


        // 拼接可变动态参数
        foreach ($app_generalize_data as $key => $app_generalize){

            $agency_list = $app_generalize['agency'];

            // 获取可变动态参数值
            $map = [];
            $map['generalize_id'] = $app_generalize['id'];
            $map['status'] = 1;
            $fields = ['id','generalize_id', 'campaign_id', 'campaign_name','ad_group_id', 'internal_suffix','channel_id','channel_name','generalize_time', 'generalize_price'];
            $app_generalize_ad_List = ApplicationLogic::getAppGeneralizeAdList($map,$fields)->get();
            $app_generalize_ad_List = Service::data($app_generalize_ad_List);
            if ($app_generalize_ad_List){
                foreach ($app_generalize_ad_List as $a_l_k => $a_l_l_generalize){
                    if ($a_l_l_generalize['generalize_id'] == $app_generalize['id']){
                        if (isset($app_generalize['platform_id'])) {
                            $n_platform_id = $app_generalize['platform_id'];
                            if (isset($app_generalize['agency'])){
                                if (isset($platform_keys[$n_platform_id])) {
                                    $platform_key_info = $platform_keys[$n_platform_id];
                                    if (isset($platform_key_info[2]) && $platform_key_info[2]) {
                                        foreach ($platform_key_info[2] as $kkk => $vvv) {
                                            $agency_list['params']['sub_params'][$a_l_k]['sub_id'] = $a_l_l_generalize['id'];
                                            $agency_list['params']['sub_params'][$a_l_k][$vvv] = $a_l_l_generalize[$vvv];
                                        }
                                    }
                                } else {
                                    $agency_list['params']['sub_params'] = [];
                                }
                            }

                        }

                    }
                }
            }else{
                if (isset($app_generalize['platform_id'])) {
                    $n_platform_id = $app_generalize['platform_id'];
                    if (isset($app_generalize['agency'])){
                        if (isset($platform_keys[$n_platform_id])) {
                            $platform_key_info = $platform_keys[$n_platform_id];
                            if (isset($platform_key_info[2]) && $platform_key_info[2]) {
                                $a_l_k_list = [];
                                foreach ($platform_key_info[2] as $kkk => $vvv) {
                                    $a_l_k_list[$a_l_k][$vvv] = '';

                                }
                                $agency_list['params']['sub_params'] = array_values($a_l_k_list);
                            }
                        } else {
                            $agency_list['params']['sub_params'] = [];
                        }
                    }

                }
            }

            $app_generalize['agency'] = $agency_list;
            $app_generalize_data[$key] = $app_generalize;
        }

        foreach ($app_generalize_data as $kkk => $app_generalize_value){
            // Facebook 平台ID写死 需要确定正式的ID值
            $value_agency = $app_generalize_value['agency'];
            unset($app_generalize_value['agency']);
            $app_generalize_value = array_merge($app_generalize_value,$value_agency);
//            if (isset($app_generalize_value['params']['dynamic_params'])){
//                $value_agency_params = $app_generalize_value['params']['dynamic_params'];
//                unset($app_generalize_value['params']['dynamic_params']);
//                $app_generalize_value['dynamic_params'] = $value_agency_params;
//            }
            $app_generalize_data[$kkk] = $app_generalize_value;
        }

        $app_generalize_data_arr_list = [];
        $app_generalize_data_arr = [];
        if ($app_generalize_data){
            foreach ($app_generalize_data as $app_generalize_data_k => $app_generalize_data_v) {
                $app_generalize_data_arr_list[$app_generalize_data_k]['platform_id'] = $app_generalize_data_v['platform_id'];
                $app_generalize_data_arr_list[$app_generalize_data_k]['agency_id'] = $app_generalize_data_v['agency_id'];
                $app_generalize_data_arr_list[$app_generalize_data_k]['data_account'] = $app_generalize_data_v['data_account'];
            }

            foreach ($app_generalize_data_arr_list as $app_generalize_data_arr_list_k => $app_generalize_data_arr_list_v) {
                $app_generalize_data_arr_list[$app_generalize_data_arr_list_k] = implode(',',$app_generalize_data_arr_list_v);
            }

            $app_generalize_data_arr_list = array_unique($app_generalize_data_arr_list);

            foreach ($app_generalize_data_arr_list as $app_generalize_data_arr_list_k => $app_generalize_data_arr_list_v) {
                $app_generalize_data_arr_list[$app_generalize_data_arr_list_k] = explode(',',$app_generalize_data_arr_list_v);
            }

            foreach ($app_generalize_data_arr_list as $app_generalize_data_arr_list_k => $app_generalize_data_arr_list_v) {
                $app_generalize_data_arr_list[$app_generalize_data_arr_list_k]['platform_id'] = $app_generalize_data_arr_list_v[0];
                unset($app_generalize_data_arr_list[$app_generalize_data_arr_list_k][0]);
                $app_generalize_data_arr_list[$app_generalize_data_arr_list_k]['agency_id'] = $app_generalize_data_arr_list_v[1];
                unset($app_generalize_data_arr_list[$app_generalize_data_arr_list_k][1]);
                $app_generalize_data_arr_list[$app_generalize_data_arr_list_k]['data_account'] = $app_generalize_data_arr_list_v[2];
                unset($app_generalize_data_arr_list[$app_generalize_data_arr_list_k][2]);

                foreach ($app_generalize_data as $app_generalize_data_k => $app_generalize_data_v) {
                   if ($app_generalize_data_arr_list[$app_generalize_data_arr_list_k]['platform_id'] == $app_generalize_data_v['platform_id']){
                       $app_generalize_data_arr_list[$app_generalize_data_arr_list_k]['platform_name'] = $app_generalize_data_v['platform_name'];
                   }

                   if ($app_generalize_data_arr_list[$app_generalize_data_arr_list_k]['agency_id'] == $app_generalize_data_v['agency_id']){
                       $app_generalize_data_arr_list[$app_generalize_data_arr_list_k]['agency_name'] = $app_generalize_data_v['agency_name'];
                   }
                }
            }

            foreach ($app_generalize_data as $app_generalize_data_k => $app_generalize_data_v){
                foreach ($app_generalize_data_arr_list as $app_generalize_data_arr_k => $app_generalize_data_arr_v){
                    if (($app_generalize_data_arr_v['platform_id'] == $app_generalize_data_v['platform_id']) && ($app_generalize_data_arr_v['data_account'] == $app_generalize_data_v['data_account']) && ($app_generalize_data_arr_v['agency_id'] == $app_generalize_data_v['agency_id'])){
                        $app_generalize_data_arr_list[$app_generalize_data_arr_k]['params'][] = $app_generalize_data_v['params'];
                    }
                }

            }
        }
        $app_generalize_data_arr_list = array_values($app_generalize_data_arr_list);
        ApiResponseFactory::apiResponse(['table_list' => $app_generalize_data_arr_list],[]);

    }

    /**
     * 推广信息配置增加 修改
     * @param $params array 请求数据
     */
    public static function createAppGeneralize($params)
    {
        // 必填参数判断
        if (!$params) ApiResponseFactory::apiResponse([],[],300);

        $map['conf_type_id'] = 1;
        $dictionary_keys_list = PlatformLogic::getPlatformKeys($map)->get();
        $dictionary_keys_list = Service::data($dictionary_keys_list);

        // 获取每个平台的必填字段
        $platform_keys = [];
        if ($dictionary_keys_list){
            foreach ($dictionary_keys_list as $key => $dictionary_keys){
                $platform_id = $dictionary_keys['platform_id'];
                $platform_keys[$platform_id][$dictionary_keys['key_type']][] = $dictionary_keys['relative_key'];
            }
        }

        // 验证必填字段
        unset($params['token']);
        unset($params['sessionid']);
        $generalize_info_list = [];

        $app_id = isset($params['app_id']) ? $params['app_id'] : '';
        if (!$app_id) ApiResponseFactory::apiResponse([],[],617);

        $platform_id = isset($params['platform_id']) ? $params['platform_id'] : '';
        if (!$platform_id) ApiResponseFactory::apiResponse([],[],505);

        $data_account = isset($params['data_account']) ? $params['data_account'] : '';
        if (!$data_account) ApiResponseFactory::apiResponse([],[],805);

        $agency_id = isset($params['agency_platform_id']) ? $params['agency_platform_id'] : '';

        $ad_config = isset($params['ad_config']) ? $params['ad_config'] : '';
//        if (!$ad_config) ApiResponseFactory::apiResponse([],[],669);

        if ($ad_config){
            foreach ($ad_config as $p_key => $alize_ad_list){
                $id = isset($alize_ad_list['dynamic_id']) ? $alize_ad_list['dynamic_id'] : '';
                $redundancy_generalize_status = isset($alize_ad_list['redundancy_generalize_status']) ? $alize_ad_list['redundancy_generalize_status'] : 1;
                foreach ($platform_keys as $plat_id => $platform_key_info){
                    if ($platform_id == $plat_id) {
                        if ($id) $generalize_info_list[$p_key]['id'] = $id;
                        if ($app_id) $generalize_info_list[$p_key]['app_id'] = $app_id;
                        if ($platform_id) $generalize_info_list[$p_key]['platform_id'] = $platform_id;
                        if ($data_account) $generalize_info_list[$p_key]['data_account'] = $data_account;
                        if ($redundancy_generalize_status) $generalize_info_list[$p_key]['redundancy_generalize_status'] = $redundancy_generalize_status;
                        $generalize_info_list[$p_key]['agency_platform_id'] = $agency_id;
                        if (isset($platform_key_info[1]) && $platform_key_info[1]) {
                            $check_data = Service::checkField($alize_ad_list, $platform_key_info[1], ApplicationLogic::$appGeneralizeFieldName);
                            $generalize_info_list[$p_key] = array_merge($generalize_info_list[$p_key], $check_data);
                        }
                        if (isset($platform_key_info[2]) && $platform_key_info[2]) {
                            if (isset($alize_ad_list['sub_params']) && $alize_ad_list['sub_params']) {
                                $ad_slot_params = $alize_ad_list['sub_params'];
                                foreach ($ad_slot_params as $key => $ad_slot) {
                                    $generalize_info_list[$p_key]['sub_params'][$key] = Service::checkField($ad_slot, $platform_key_info[2], ApplicationLogic::$appGeneralizeFieldName);
                                }
                            }
                        }
                    }
                }
                if(!isset($generalize_info_list[$p_key]))  ApiResponseFactory::apiResponse([],[],825);
            }
        }else{
            $generalize_info_list[0]['app_id'] = $app_id;
            $generalize_info_list[0]['platform_id'] = $platform_id;
            $generalize_info_list[0]['data_account'] = $data_account;
            $generalize_info_list[0]['agency_platform_id'] = $agency_id;
        }


        $update_time = date('Y-m-d H:i:s');
        $create_time = date('Y-m-d H:i:s');
        // 开启事物 保存数据
        DB::beginTransaction();

        foreach ($generalize_info_list as $key => $generalize_info) {
            $id = isset($generalize_info['id']) ? $generalize_info['id'] : '';
            $update_data = [];
            if ($id){ // 先删除 再添加
                // 删除原数据
                $map = [];
                $map['id'] = $id;
                $map['generalize_status'] = 1;
                $app_generalize_info = ApplicationLogic::getAppGeneralizeList($map)->first();
                $app_generalize_info = Service::data($app_generalize_info);
                if ($app_generalize_info){
                    $generalize_id = $app_generalize_info['id'];
                    $update_data['generalize_status'] = 2;
                    $update_data['update_time'] = $update_time;
                    $bool = ApplicationLogic::updateAppGeneralize($id, $update_data);
                    if (!$bool){
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([],[],636);
                    }
                    OperationLogImp::saveOperationLog(3,11,$generalize_id);

                    // 删除原关联数据
                    $map = [];
                    $map['generalize_id'] = $generalize_id;
                    $map['status'] = 1;
                    $fields = ['id'];
                    $app_generalize_ad_list = ApplicationLogic::getAppGeneralizeAdList($map, $fields)->get();
                    $app_generalize_ad_list = Service::data($app_generalize_ad_list);
                    if ($app_generalize_ad_list){
                        $map = [];
                        $map['generalize_id'] = $generalize_id;
                        unset($update_data['generalize_status']);
                        $update_data['status'] = 2;
                        $update_data['update_time'] = $update_time;
                        $bool = ApplicationLogic::updateAppGeneralizeApp($map, $update_data);
                        if (!$bool){
                            DB::rollBack();
                            ApiResponseFactory::apiResponse([],[],636);
                        }
                        foreach ($app_generalize_ad_list as $key1 => $value1) {
                            OperationLogImp::saveOperationLog(3,15,$value1['id']);
                        }
                    }
                }
            }
        }
        DB::commit();
        // 开启事物 保存数据
        DB::beginTransaction();

        foreach ($generalize_info_list as $key => $generalize_info) {
            $dynamic_params = isset($generalize_info['sub_params']) ? $generalize_info['sub_params'] : [];
            unset($generalize_info['sub_params']);
            unset($generalize_info['id']);

            // 验证重复性
            $generalize_info['generalize_status'] = 1;
            $result = ApplicationLogic::getAppGeneralizeList($generalize_info)->first();
            $result = Service::data($result);
            if ($result){
                DB::rollBack();
                ApiResponseFactory::apiResponse([], [], 680);
            }
            // 插入新数据
            $generalize_info['create_time'] = $create_time;
            $generalize_info['update_time'] = $update_time;
            if (!$generalize_info['agency_platform_id']) $generalize_info['agency_platform_id'] = 0;
            $generalize_id = ApplicationLogic::addAppGeneralize($generalize_info);
            if (!$generalize_id){
                DB::rollBack();
                ApiResponseFactory::apiResponse([],[],681);
            }
            OperationLogImp::saveOperationLog(1,11,$generalize_id);

            // 原关联数据
            if ($dynamic_params){
                foreach ($dynamic_params as $kkk => $value){
                    // 验证重复性
                    $map = [];
                    $map['status'] = 1;
                    $map[] = ['generalize_id', '=', $generalize_id];
                    $map = array_merge($map, $value);
                    $result = ApplicationLogic::getAppGeneralizeAdList($map)->first();
                    $result = Service::data($result);
                    if ($result){
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([], [], 682);
                    }

                    // 插入新数据
                    $value['status'] = 1;
                    $value['generalize_id'] = $generalize_id;
                    $value['create_time'] = $create_time;
                    $value['update_time'] = $update_time;
                    $id = ApplicationLogic::addAppGeneralizeAd($value);
                    if (!$id){
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([],[],683);
                    }
                    OperationLogImp::saveOperationLog(1,15,$id);
                }
            }
        }

        DB::commit();
        ApiResponseFactory::apiResponse([],[]);
    }

    /**
     * 推广信息配置删除
     * @param $params array 请求数据
     */
    public static function changeAppGeneralizeStatus($params)
    {
        // 必填参数判断
        if (!$params) ApiResponseFactory::apiResponse([],[],300);

        // 必填参数判断
        $type = isset($params['type']) ? $params['type'] : ''; // 删除级别
        $ad_status = isset($params['status']) ? $params['status'] : 2; // 状态ID
        if (!$type) ApiResponseFactory::apiResponse([],[],828);

        $update_time = date('Y-m-d H:i:s');
        // 1:平台 2:第一级参数 3:第二级参数
        if ($type == 1){
            $app_id = isset($params['app_id']) ? $params['app_id'] : ''; // 应用ID
            $platform_id = isset($params['platform_id']) ? $params['platform_id'] : ''; // 平台ID
            $data_account = isset($params['data_account']) ? $params['data_account'] : ''; // 平台ID
            $agency_platform_id = isset($params['agency_platform_id']) ? $params['agency_platform_id'] : ''; // 平台ID

            // 删除原数据
            $map = [];
            $map['app_id'] = $app_id;
            $map['platform_id'] = $platform_id;
            $map['data_account'] = $data_account;
            $map['agency_platform_id'] = $agency_platform_id;
            $map['generalize_status'] = 1;
            $app_generalize_list = ApplicationLogic::getAppGeneralizeList($map)->get();
            $app_generalize_list = Service::data($app_generalize_list);

            if ($app_generalize_list){
                DB::beginTransaction();
                foreach ($app_generalize_list as $app_generalize_info) {
                    $generalize_id = $app_generalize_info['id'];
                    $update_data['generalize_status'] = 2;
                    $update_data['update_time'] = $update_time;
                    $bool = ApplicationLogic::updateAppGeneralize($generalize_id, $update_data);
                    if (!$bool) {
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([], [], 636);
                    }
                    OperationLogImp::saveOperationLog(3, 11, $generalize_id);

                    // 删除原关联数据
                    $map = [];
                    $map['generalize_id'] = $generalize_id;
                    $map['status'] = 1;
                    $fields = ['id'];
                    $app_generalize_ad_list = ApplicationLogic::getAppGeneralizeAdList($map, $fields)->get();
                    $app_generalize_ad_list = Service::data($app_generalize_ad_list);
                    if ($app_generalize_ad_list) {
                        $map = [];
                        $map['generalize_id'] = $generalize_id;
                        unset($update_data['generalize_status']);
                        $update_data['status'] = 2;
                        $update_data['update_time'] = $update_time;
                        $bool = ApplicationLogic::updateAppGeneralizeApp($map, $update_data);
                        if (!$bool) {
                            DB::rollBack();
                            ApiResponseFactory::apiResponse([], [], 636);
                        }
                        foreach ($app_generalize_ad_list as $key1 => $value1) {
                            OperationLogImp::saveOperationLog(3, 15, $value1['id']);
                        }
                    }
                }
                DB::commit();
            }

        }elseif($type == 2){
            $id = isset($params['id']) ? $params['id'] : ''; // ID
            if ($id) { // 先删除 再添加
                DB::beginTransaction();
                // 删除原数据
                $map = [];
                $map['id'] = $id;
                $map['generalize_status'] = 1;
                $app_generalize_info = ApplicationLogic::getAppGeneralizeList($map)->first();
                $app_generalize_info = Service::data($app_generalize_info);
                if ($app_generalize_info) {
                    $generalize_id = $app_generalize_info['id'];
                    $update_data['generalize_status'] = 2;
                    $update_data['update_time'] = $update_time;
                    $bool = ApplicationLogic::updateAppGeneralize($id, $update_data);
                    if (!$bool) {
                        DB::rollBack();
                        ApiResponseFactory::apiResponse([], [], 636);
                    }
                    OperationLogImp::saveOperationLog(3, 11, $generalize_id);

                    // 删除原关联数据
                    $map = [];
                    $map['generalize_id'] = $generalize_id;
                    $map['status'] = 1;
                    $fields = ['id'];
                    $app_generalize_ad_list = ApplicationLogic::getAppGeneralizeAdList($map, $fields)->get();
                    $app_generalize_ad_list = Service::data($app_generalize_ad_list);
                    if ($app_generalize_ad_list) {
                        $map = [];
                        $map['generalize_id'] = $generalize_id;
                        unset($update_data['generalize_status']);
                        $update_data['status'] = 2;
                        $update_data['update_time'] = $update_time;
                        $bool = ApplicationLogic::updateAppGeneralizeApp($map, $update_data);
                        if (!$bool) {
                            DB::rollBack();
                            ApiResponseFactory::apiResponse([], [], 636);
                        }
                        foreach ($app_generalize_ad_list as $key1 => $value1) {
                            OperationLogImp::saveOperationLog(3, 15, $value1['id']);
                        }
                    }
                }
                DB::commit();
            }
        }elseif ($type == 3){
            $id = isset($params['id']) ? $params['id'] : ''; // 版本信息ID
            $data['update_time'] = $update_time;
            $data['status'] = $ad_status;
            $bool = ApplicationLogic::changeAppGeneralizeStatus($id, $data);
            if (!$bool){
                ApiResponseFactory::apiResponse([],[],717);
            }
            // 保存日志
            OperationLogImp::saveOperationLog(3,15, $id);
        }


        ApiResponseFactory::apiResponse([],[]);
    }

}
