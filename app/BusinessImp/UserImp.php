<?php

namespace App\BusinessImp;

use App\BusinessLogic\CommonLogic;
use App\BusinessLogic\OperationLogLogic;
use App\Common\ApiResponseFactory;
use App\Common\Service;
use App\Common\CurlRequest;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\UserLogic;
use Illuminate\Support\Facades\DB;
use App\BusinessLogic\RoleLogic;

class UserImp extends ApiBaseImp
{

    /**
     * 用户登录
     * @param $params array 请求数据
     */
    public static function login($params){
        // 必填参数判断
//        var_dump(111,$params);
        if (!$params) ApiResponseFactory::apiResponse([],[],300);
//        var_dump(222,$params);
        $token = isset($params['t']) ? $params['t'] : ''; //获取token
        if($token == '') ApiResponseFactory::apiResponse([],[],301);
        $url = env('ZPASSPORT_CHECK_URL');
        $info['token'] = $token;
        //内网通行证的信息
        $possportUserinfo = CurlRequest::curl_header_Post($url,$info,array());
        $possportUserinfo =json_decode($possportUserinfo,true);
        if($possportUserinfo['errno'] !=0) {
            ApiResponseFactory::apiResponse([],[],1002);
            exit;
        }
        $map['user_account'] = $possportUserinfo['data']['email'];
        $map['status'] = 1;
        $fields = ['user.id','user.name','user.user_account','user.role_id','role.department_id','user.type'];
        $map['leftjoin'] = [
            ['role','user.role_id', 'role.id'],
        ];
        //验证用户是否有权限登录
        $userInfo = UserLogic::Userlist($map,$fields)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) {
            ApiResponseFactory::apiResponse([],[],1002);
        }
        $_SESSION['erm_data']['expireTime'] = strtotime($possportUserinfo['data']['expireTime']);
        $_SESSION['erm_data']['name'] = $userInfo[0]['name'];
        $_SESSION['erm_data']['guid'] = $userInfo[0]['id'];
        $_SESSION['erm_data']['email'] = $userInfo[0]['user_account'];
        // 获取树形菜单
        $back_data=[
            'guid'=> $userInfo[0]['id'],
            'email'=> $userInfo[0]['user_account'],
            'name'=> $userInfo[0]['name'],
        ];
        OperationLogImp::saveOperationLog(4,0);
        ApiResponseFactory::apiResponse($back_data,[]);
    }

    /**
     * 用户登录
     * @param $params array 请求数据
     */
    public static function devlogin($params){
        // 必填参数判断
        if (!$params) ApiResponseFactory::apiResponse([],[],300);
        $email = isset($params['email']) ? $params['email'] : ''; //获取token
        if(!$email) ApiResponseFactory::apiResponse([],[],1034);
        // 查询数据平台用户表有无此用户
        $map['user_account'] = $email;
        $map['account_type'] = 2;
        $fields = ['id','user_account','developer_id','account_type'];

        //验证用户是否有权限登录
        $userInfo = UserLogic::Userlist($map,$fields)->get();
        $userInfo =Service::data($userInfo);
        if(!$userInfo) {
            ApiResponseFactory::apiResponse([],[],1002);
        }
        $_SESSION['erm_dev_data']['guid'] = $userInfo[0]['id'];
        $_SESSION['erm_dev_data']['email'] = $userInfo[0]['user_account'];
        $_SESSION['erm_dev_data']['user_account'] = $userInfo[0]['user_account'];
        $_SESSION['erm_dev_data']['developer_id'] = $userInfo[0]['developer_id'];
        // 获取树形菜单
        $back_data=[
            'guid'=> $userInfo[0]['id'],
            'email'=> $userInfo[0]['user_account'],
            'developer_id'=> $userInfo[0]['developer_id'],
        ];
        ApiResponseFactory::apiResponse($back_data,[]);
    }
        /**
     * 用户列表
     * @param $params array 请求数据
     */
    public static function list($params){
        $user_account = isset($params['user_account']) ? $params['user_account'] : ''; //用户账号
        $name = isset($params['name']) ? $params['name'] : ''; // 用户名称
        $role_id = isset($params['role_id']) ? $params['role_id'] : ''; // 用户名称
        $page = isset($params['page']) ? $params['page'] : 1 ;
        $page_size = isset($params['size']) ? $params['size'] : 1000 ;
        $uid =$_SESSION['erm_data']['guid'];

                // 获取数据
        $map = [];
        if ($user_account) $map['like'][] = ['user.user_account','like', $user_account];
        if ($name) $map['like'][] = ['user.name','like', $name];
        if ($role_id) $map['role_id'] = $role_id;
        $map['parent_uid'] =$uid;
        $fields = ['user.company_id','user.type','user.id','user.name','user.user_account','user.role_id','user.app_permission','user.function_permission','user.status','role.role_name'];

        $map['leftjoin'] = [
            ['role','user.role_id', 'role.id'],
        ];
        $Info = UserLogic::Userlist($map,$fields)->forPage($page,$page_size)->orderby("role.id","desc")->get();
        $Info =Service::data($Info);
        // 获取数据总数
        $total = UserLogic::Userlist($map)->count();
        //获取权限
        $appList = UserLogic::appList()->get();
        $appList =Service::data($appList);
        $appListInfo =[];
        foreach ($appList as $key => $value) {
            $appListInfo[$value['id']] = $value['app_name'];
        }
        
        //拼接数据
        foreach ($Info as $k => $v) {
            if($v['app_permission'] ==-2){
                $Info[$k]['app_permission_name']='全部应用';

            }else{
                $app_permission = explode(',',$v['app_permission']); 
                $str = '';

                foreach ($app_permission as  $v1) {
                    $str.= $appListInfo[$v1].'、';
                }
                $Info[$k]['app_permission_name']=rtrim($str,'、');
            }
                     
        }




        $back_data=[
            'table_list'=>$Info,
            'total'=> $total,
            'page_total'=> ceil($total / $page_size),
        ];

        ApiResponseFactory::apiResponse($back_data,[]);

       
    }
    /**
     * 用户创建和修改
     * @param $params array 请求数据
     */
    public static function create($params){
        $parent_uid = $_SESSION['erm_data']['guid'];

                // 必填参数判断
        if (!$params) ApiResponseFactory::apiResponse([],[],300);
        $id = isset($params['id']) ? $params['id'] : ''; // 应用自增ID

        // 判断 编辑? 还是 创建?
        if ($id){ // 编辑
            // 必填参数判断
            $condition = ['name','type'];
            Service::checkField($params, $condition, UserLogic::$tableFieldName);
            $map['id']=$id;
            //获取用户信息
            $userInfo = UserLogic::checkUserAccount($map)->first();
            $userInfo =Service::data($userInfo);
             $old_data['id']=$id;
             $old_data['company_id']='';
            if($userInfo){
                $old_data['name']=$userInfo['name'];
                $old_data['role_id']=$userInfo['role_id'];
                if($params['login_type']!=3){
                    $condition = ['company_id'];
                    Service::checkField($params, $condition, UserLogic::$tableFieldName);
                    $old_data['company_id']=$params['company_id'];
                    $old_data['type']=$params['type'];
                }
                
            }else{
                $old_data['name']='';
                $old_data['role_id']='';
                if($params['login_type']!=3){
                    $old_data['company_id']='';
                    $old_data['type']='';
                }
                
            }
            //查出这个角色有什么权限
            $role_map['id'] = $userInfo['role_id'];
            $role_info = RoleLogic::Rolelist($role_map)->first();
            $role_info =Service::data($role_info);
           
            
            $data = [];
            $data['name']=$params['name'];
            if ($params['role_id'] != $userInfo['role_id']){
                $data['role_id']=$params['role_id'];
                $data['function_permission']= $role_info['permission'];
            }

            if($params['login_type']!=3){
                $data['company_id']=$params['company_id'];
                $data['type']=$params['type'];
            }

            $data['update_time']=date('Y-m-d H:i:s');
            // 保存应用数据
            $new_id = UserLogic::userEdit($id,$data);
            if (!$new_id){
                ApiResponseFactory::apiResponse([],[],725);
            }

            // 保存日志
            OperationLogImp::saveOperationLog(2,27,$params, $old_data);

            ApiResponseFactory::apiResponse($new_id,[]);

        }else{ // 创建
            // 必填参数判断
            $condition = ['name', 'user_account', 'type', 'company_id'];
            $data = Service::checkField($params, $condition, UserLogic::$tableFieldName);

            //验证邮箱格式
            $checkemail = Service::checkemail($data['user_account']);
            if(!$checkemail)  ApiResponseFactory::apiResponse([],[],721);
            $map['user_account']=$data['user_account'];
            //验证账号是否存在
            $checkUserAccount = UserLogic::checkUserAccount($map)->first();
            $checkUserAccount =Service::data($checkUserAccount);
            if($checkUserAccount) ApiResponseFactory::apiResponse([],[],723);
            //查出这个角色有什么权限
            $role_map['id'] = $params['role_id'];
            $role_info = RoleLogic::Rolelist($role_map)->first();
            $role_info =Service::data($role_info);

            $data['role_id']=$params['role_id'];
            $data['company_id']=$params['company_id'];
            $data['create_time']=date('Y-m-d H:i:s');
            $data['update_time']=date('Y-m-d H:i:s');
            $data['type']=$params['type'];
            $data['parent_uid']=$parent_uid;
            $data['function_permission']=$role_info['permission'];
            
            // 保存应用数据
            $new_id = UserLogic::userAdd($data);
            if (!$new_id){
                ApiResponseFactory::apiResponse([],[],724);
            }
            // 保存日志
            OperationLogImp::saveOperationLog(1,30,$new_id);

            ApiResponseFactory::apiResponse($new_id,[]);

        }
    }
    /**
     * 用户状态修改
     * @param $params array 请求数据
     */
    public static function editStatus($params){
        // 必填参数判断
        if (!$params) ApiResponseFactory::apiResponse([],[],300);
        $id= $params['id'];
                //获取用户信息
        $map['id']= $id;
        $userInfo = UserLogic::checkUserAccount($map)->first();
        $userInfo =Service::data($userInfo);
        $old_data['id']=$params['id'];
        if($userInfo){
            $old_data['status']=$userInfo['status'];
        }else{
            $old_data['status']='';
        }

        $update_time = date('Y-m-d H:i:s');
        $data['update_time'] = $update_time;
        $data['status'] = $params['status'];
        //0禁用1开启
        $bool = UserLogic::userEdit($id, $data);
        OperationLogImp::saveOperationLog(2,28,$params,$old_data);
        if (!$bool){
            ApiResponseFactory::apiResponse([],[],726);
        }
        ApiResponseFactory::apiResponse($id,[]);
       
    }
    /**
     * 用户权限修改
     * @param $params array 请求数据
     */
    public static function editRole($params){
         // 必填参数判断
        if (!$params) ApiResponseFactory::apiResponse([],[],300);
        $id= $params['id'];
                //获取用户信息
        $map['id']= $id;
        $userInfo = UserLogic::checkUserAccount($map)->first();
        $userInfo =Service::data($userInfo);
        $old_data['id']=$params['id'];
        if($userInfo){
            $old_data['app_permission']=$userInfo['app_permission'];
            if($params['type'] ==1){
                $old_data['function_permission']=$userInfo['function_permission'];
            }
        }else{
            $old_data['app_permission']='';
            if($params['type'] ==1){
                $old_data['function_permission']='';
            }
        }
        $update_time = date('Y-m-d H:i:s');
        $data['update_time'] = $update_time;
        $data['app_permission'] = $params['app_permission'];
        if($params['type'] ==1){
            $data['function_permission'] = $params['function_permission'];
        }
        //0禁用1开启
        
        $bool = UserLogic::userEdit($id, $data);
        if (!$bool){
            ApiResponseFactory::apiResponse([],[],727);
        }
        OperationLogImp::saveOperationLog(2,29,$params,$old_data);
        ApiResponseFactory::apiResponse($id,[]);
       
    }
    public static function wholeUserList($params){
        $fields = ['user.id','user.name','user.user_account'];
        $map =[];
        $Info = UserLogic::Userlist($map,$fields)->get();
        $Info =Service::data($Info);
        $back_data=[
            'table_list'=>$Info
        ];
        ApiResponseFactory::apiResponse($back_data,[]);

    }


}
