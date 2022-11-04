<?php

namespace App\BusinessPlatForm;

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
use Illuminate\Support\Facades\Redis;
use App\Models\Zhibo_user;

class ApplicationImp
{
    public static function zhibo_user_add($params)
    {
        // 必填参数判断
        if (!$params) ApiResponseFactory::apiResponse([], [], 300, '');
        if (!$params['name']) ApiResponseFactory::apiResponse([], [], 2000, '用户姓名不能为空');
        if (!$params['phone']) ApiResponseFactory::apiResponse([], [], 20001, '用户手机号不能为空');

        $preg_phone = '/^1[3456789]\d{9}$/ims';
        if (!preg_match($preg_phone, $params['phone'])) {
            ApiResponseFactory::apiResponse([], [], 20002, '用户手机号合法');
        }
        $arr = [];
        $arr['name'] = $params['name'];
        $arr['phone'] = $params['phone'];
        $arr['age'] = $params['age'];
        $arr['sex'] = $params['sex'];
        $arr['experience'] = $params['experience'];
        $arr['province'] = $params['area'][0]['name'] ?? '';
        $arr['city'] = $params['area'][1]['name'] ?? '';
        $arr['area'] = $params['area'][2]['name'] ?? '';
        $pre = 'zhibo_';
        $key = $pre . $arr['phone'];

        Redis::select(0);

        $get_num = Redis::get($key);
        if ($get_num >= 10) {
            ApiResponseFactory::apiResponse([], [], 20003, '一分钟只能填10次');
        }

        $com_obj = Zhibo_user::create($arr);
        $com_obj = Service::data($com_obj);
        Redis::incr($key);
        Redis::expire($key, '60');
        if (!$com_obj) {
            ApiResponseFactory::apiResponse([], [], 20004, '添加失败');
        }
        ApiResponseFactory::apiResponse([], []);
    }
    public static function zhibo_get_num()
    {
        Redis::select(0);
        $key = 'zhibo_count';
        $get_num = Redis::get($key);
        if (!$get_num) {
            Redis::set($key, 384);
        }
        $arr['count'] = $get_num ? $get_num : 384;
        $rand = rand(1, 20);
        Redis::set($key, $get_num + $rand);
        $name_arr = ['张伟','王伟','王芳','李伟','李娜','张敏','李静','王静','刘伟','王秀英','张丽','李秀英','王丽','张静','张秀英','李强','王敏','李敏','王磊','刘洋','王艳','王勇','李军','张勇','李杰','张杰','张磊','王强','李娟','王军','张艳','张涛','王涛','李艳','王超','李明','李勇','王娟','刘杰','刘敏','李霞','李丽','张军','王杰','张强','王秀兰','王刚','陈勇','王鑫','李芳','张桂兰','李波','杨勇','王霞','李桂兰','王斌','李鹏','张平','张莉','张辉','张宇','刘娟','李斌','王浩','陈杰','王凯','陈丽','陈敏','王秀珍','李玉兰','刘秀英','王萍','王萍','张波','刘桂英','杨秀英','张英','杨丽','张健','李俊','李莉','王波','张红','刘丹','李鑫','王莉','杨静','刘超','张娟','杨帆','刘燕','刘英','李雪','李秀珍','张鑫','王健','刘玉兰','刘辉','刘波','张浩','张明','陈燕','张霞','陈艳','杨杰','王帅','李慧','王雪','杨军','张旭','刘刚','王华','杨敏','王宁','李宁','王俊','刘桂兰','刘斌','张萍','王婷','陈涛','王玉梅','王娜','张斌','陈龙','李林','王玉珍','张凤英','王红','李凤英','杨洋','李婷','张俊','王林','陈英','陈军','刘霞','陈浩','张凯','王晶','陈芳','张婷','杨涛','杨波','陈红','刘欢','王玉英','陈娟','陈刚','王慧','张颖','张林','张娜','张玉梅','王凤英','张玉英','李红梅','刘佳','刘磊','张倩','刘鹏','王旭','张雪','李阳','张秀珍','王梅','王建华','李玉梅','王颖','刘平','杨梅','李飞','王亮','李磊','李建华','王宇','陈玲','张建华','刘鑫','王倩','张帅','李健','陈林','李洋','陈强','赵静','王成','张玉珍','陈超','陈亮','刘娜','王琴','张兰英','张慧','刘畅','李倩','杨艳','张亮','张建','李云','张琴','王兰英','李玉珍','刘萍','陈桂英','刘颖','杨超','张梅','陈平','王建','刘红','赵伟','张云','张宁','杨林','张洁','高峰','王建国','杨阳','陈华','杨华','王建军','杨柳','刘阳','王淑珍','杨芳','李春梅','刘俊','王海燕','刘玲','陈晨','王欢','李冬梅','张龙','陈波','陈磊','王云','王峰','王秀荣','王瑞','李琴','李桂珍','陈鹏','王莹','刘飞','王秀云','陈明','王桂荣','李浩','王志强','张丹','李峰','张红梅','刘凤英','李玉英','王秀梅','李佳','王丽娟','陈辉','张婷婷','张芳','王婷婷','王玉华','张建国','李兰英','王桂珍','李秀梅','陈玉兰','陈霞','刘凯','张玉华','刘玉梅','刘华','李兵','张雷','王东','李建军','刘玉珍','王琳','李建国','李颖','杨伟'];
        $n = array_rand($name_arr,20);
        $ar_name = [];
        foreach($n as $key =>$v){
            $str = $name_arr[$v];
            if(mb_strlen($name_arr[$v])==2){
                $str = mb_substr($str, 0, 1, 'UTF-8') . '*' . mb_substr($str, -1, 1, 'UTF-8');
            }else{
                $str = mb_substr($str, 0, 1, 'UTF-8') . '*';
            }
            $ar_name['name'] = $str;
            $ar_name['date'] =date('Y-m-d');
            $arr['name'][]= $ar_name;
        }
        ApiResponseFactory::apiResponse($arr, []);
    }
}
