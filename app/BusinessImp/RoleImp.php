<?php

namespace App\BusinessImp;

use App\BusinessLogic\CommonLogic;
use App\BusinessLogic\OperationLogLogic;
use App\Common\ApiResponseFactory;
use App\Common\Service;
use Illuminate\Support\Facades\Log;
use App\BusinessLogic\RoleLogic;
use Illuminate\Support\Facades\DB;

class RoleImp extends ApiBaseImp
{


        /**
     * 用户列表
     * @param $params array 请求数据
     */
    public static function RoleList($params){
        $department_id = isset($params['department_id']) ? $params['department_id'] : ''; // 部门id
        $role_name = isset($params['role_name']) ? $params['role_name'] : ''; // 角色名称
        $page = isset($params['page']) ? $params['page'] : 1 ;
        $page_size = isset($params['size']) ? $params['size'] : 1000 ;

                // 获取数据
        $map = [];
        if ($role_name) $map['like'][] = ['role.role_name','like', $role_name];
        if ($department_id) $map['department_id'] = $department_id;
        $fields = ['role.*','department.department_name'];

        $map['leftjoin'] = [
            ['department','department.id', 'role.department_id']
        ];
        $Info = RoleLogic::Rolelist($map,$fields)->forPage($page,$page_size)->orderby("role.id","desc")->get();
        $Info =Service::data($Info);
        // 获取数据总数
        $total = RoleLogic::Rolelist($map)->count();
        //获取权限
        $permissionList = RoleLogic::permissionList()->get();
        $permissionList =Service::data($permissionList);
        $permissionListInfo =[];
        foreach ($permissionList as $key => $value) {
            $permissionListInfo[$value['id']] = $value['menu_name'];
        }
        
        //拼接数据
        foreach ($Info as $k => $v) {
            if($v['permission'] == -2){
                 $Info[$k]['permission_name'] ='全部权限';
            }else{
                $permission = explode(',',$v['permission']);  
                $str = ''; 
                foreach ($permission as  $v1) {
                    $str.= $permissionListInfo[$v1].'、';           
                }
                $Info[$k]['permission_name']=rtrim($str,'、'); 
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
    public static function RoleCreate($params){


        $id = isset($params['id']) ? $params['id'] : ''; // 应用自增ID


        // 判断 编辑? 还是 创建?
        if ($id){ // 编辑
            $map['id']=$id;
            //获取用户信息
            $Info = RoleLogic::checkRoleAccount($map)->first();
            $Info =Service::data($Info);
             $old_data['id']=$id;
            if($Info){
                $old_data['permission']=$Info['permission'];
            }else{
                $old_data['permission']='';
            }
           
            // 必填参数判断
            $condition = ['permission'];
            $data = Service::checkField($params, $condition, RoleLogic::$tableFieldName);
            $data['permission']=$params['permission'];
            $data['update_time']=date('Y-m-d H:i:s');

            // 保存应用数据
            $new_id = RoleLogic::RoleEdit($id,$data);
            if (!$new_id){
                ApiResponseFactory::apiResponse([],[],725);
            }

            // 保存日志
            OperationLogImp::saveOperationLog(2,32,$params, $old_data);

            ApiResponseFactory::apiResponse($new_id,[]);

        }else{ // 创建
            // 必填参数判断
            $condition = ['role_name', 'department_id','permission'];

            $data = Service::checkField($params, $condition, RoleLogic::$tableFieldName);
            $data['create_time']=date('Y-m-d H:i:s');
            $data['update_time']=date('Y-m-d H:i:s');
            // 保存应用数据
            $new_id = RoleLogic::RoleAdd($data);
            if (!$new_id){
                ApiResponseFactory::apiResponse([],[],733);
            }
            // 保存日志
            OperationLogImp::saveOperationLog(1,31,$new_id);

            ApiResponseFactory::apiResponse($new_id,[]);

        }
    }
    


}
