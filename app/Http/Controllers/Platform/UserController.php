<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: ����4:18
 */
namespace App\Http\Controllers\Platform;

use App\BusinessPlatForm\ApplicationImp;
use App\Http\Controllers\Controller as Controller;
use Illuminate\Http\Request;

class UserController extends Controller
{
    /**
     * Ӧ���б�
     */
    public function zhibo_add(){
        ApplicationImp::zhibo_user_add($this->params);
    }
    public function zhibo_get_num(){
        ApplicationImp::zhibo_get_num();
    }

}