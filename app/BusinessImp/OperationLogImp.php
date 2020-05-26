<?php

namespace App\BusinessImp;

use App\BusinessLogic\CommonLogic;
use App\BusinessLogic\OperationLogLogic;
use App\BusinessLogic\PlatformLogic;
use App\Common\ApiResponseFactory;
use App\Common\Service;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;


class OperationLogImp extends ApiBaseImp
{
    /**
     * 操作日志列表
     * @param $params array 请求数据
     */
    public static function getOperationLogList($params)
    {
        // 查询搜索参数
        $search = isset($params['search']) ? $params['search'] : ''; // 用户ID
        $operate_type = isset($params['operate_type']) ? $params['operate_type'] : ''; // 操作类型名称
        $model_id = isset($params['model_id']) ? $params['model_id'] : ''; // 模块ID
        $begin_time = isset($params['begin_time']) ? $params['begin_time'].' 00:00:00' :date('Y-m-d 00:00:00',strtotime("-7 day")); // 时间
        $end_time = isset($params['end_time']) ? $params['end_time'].' 23:59:59' : date('Y-m-d 23:59:59'); // 时间

        $page = isset($params['page']) ? $params['page'] : 1 ;
        $page_size = isset($params['size']) ? $params['size'] : 1000 ;

        $map = []; // 查询条件
        if ($search) $map['like'][] = ['user.user_account','like', $search];
        if ($operate_type) $map['operate_type'] = $operate_type;
        if ($model_id) $map['model_id'] = $model_id;

        $fields = ['c_operation_log.operate_content','c_operation_log.create_time','c_operation_log.operate_type','c_operation_log.operate_position','user.user_account'];
        $map['between'] = ['c_operation_log.create_time',[ $begin_time, $end_time]];
        $map['leftjoin'] = [
            ['user','user.id', 'c_operation_log.user_id']
        ];
        // 获取分页数据
        $operation_log_list = OperationLogLogic::getOperationLogList($map, $fields)->forPage($page,$page_size)->orderby("c_operation_log.id","desc")->get();
        $operation_log_list = Service::data($operation_log_list);
        if (!$operation_log_list) ApiResponseFactory::apiResponse([],[]);

        // 获取数据总数
        $total = OperationLogLogic::getOperationLogList($map)->count();

        $back_data=[
            'table_list'=>$operation_log_list,
            'total'=> $total,
            'page_total'=> ceil($total / $page_size),
        ];

        ApiResponseFactory::apiResponse($back_data,[]);
    }

    /*
     * 保存日志
     */
    public static function saveOperationLog($operation_type, $model_id, $new_data = [], $old_data = []){

        $create_time = date('Y-m-d H:i:s');
        $user_id = $_SESSION['erm_data']['guid']; // 从session中获取
        $map['id'] = $model_id;
        if($operation_type != 0){
            $menu_list = OperationLogLogic::navMenuInfo()->get();
            $menu_list = Service::data($menu_list);
            $menu_info = Service::getTopParentName($menu_list, $model_id);
            if ($menu_info){
                $operate_position = implode('->', $menu_info);
            }else{
                $operate_position = '未知';
            }

        }

        if($operation_type == 4){
             // 添加
            $operation_log_data['user_id'] = $user_id;
            $operation_log_data['operate_position'] = '登录';
            $operation_log_data['model_id'] = 0;
            $operation_log_data['operate_type'] = 4;
            $operation_log_data['operate_content'] = '登录用户为:'. $_SESSION['erm_data']['name'];
            $operation_log_data['create_time'] = $create_time;

            OperationLogLogic::addOperationLog($operation_log_data);

        }elseif ($operation_type == 1){
            // 添加
            $operation_log_data['user_id'] = $user_id;
            $operation_log_data['operate_position'] = $operate_position;
            $operation_log_data['model_id'] = $model_id;
            $operation_log_data['operate_type'] = $operation_type;
            $operation_log_data['operate_content'] = '新增数据,ID为:'. $new_data;
            $operation_log_data['create_time'] = $create_time;

            OperationLogLogic::addOperationLog($operation_log_data);
        }else if($operation_type == 2 ){
            // 编辑
//            var_dump($new_data,$old_data);
            $change_data = [];
            unset($old_data["create_time"]);
            unset($old_data["update_time"]);
            $content_str = '';
            $content_str .="修改数据，自增id为".$old_data['id'].";";
            foreach ($new_data as $k => $v) {
                if (isset($old_data[$k]) && $v!=$old_data[$k]) {
                    $change_data["origin"][$k] = $old_data[$k];
                    $change_data["modified"][$k] = $new_data[$k];
                    if (isset(OperationLogLogic::$tableFieldName[$k])){
                        $content_str .= OperationLogLogic::$tableFieldName[$k] . '由<' . $old_data[$k] . '>改为<' . $new_data[$k] .'>;';
                    }else{
                        $content_str .= $k . '由<' . $old_data[$k] . '>改为<' . $new_data[$k] .'>;';
                    }
                }
            }
            if ($content_str){
                $operation_log_data['user_id'] = $user_id;
                $operation_log_data['operate_position'] = $operate_position;
                $operation_log_data['model_id'] = $model_id;
                $operation_log_data['operate_type'] = $operation_type;
                $operation_log_data['operate_content'] = $content_str;
                $operation_log_data['create_time'] = $create_time;
                OperationLogLogic::addOperationLog($operation_log_data);
            }

        }else{
            // 删除
            $operation_log_data['user_id'] = $user_id;
            $operation_log_data['operate_position'] = $operate_position;
            $operation_log_data['model_id'] = $model_id;
            $operation_log_data['operate_type'] = $operation_type;
            $operation_log_data['operate_content'] = '删除数据,ID为:'. $new_data;
            $operation_log_data['create_time'] = $create_time;
            OperationLogLogic::addOperationLog($operation_log_data);

        }

    }
}
