<?php

namespace App\Http\Controllers;

use Illuminate\Foundation\Bus\DispatchesJobs;
use Illuminate\Routing\Controller as BaseController;
use Illuminate\Foundation\Validation\ValidatesRequests;
use Illuminate\Foundation\Auth\Access\AuthorizesRequests;
use Illuminate\Http\Request;
use App\Common\ApiResponseFactory;
use App\Common\Service;
use Illuminate\Support\Facades\Session;

class Controller extends BaseController
{
//    use AuthorizesRequests, DispatchesJobs, ValidatesRequests;

    public $params = [];
    public $header;
    public $token;
    public $sessionId='';

    public $postData=[];
    public $jwtUser=[];

    public function __construct()
    {

        // ��ʼ��
        $this->init();

    }

    private function init(){

        //��header��ȡ�� token
        $sessionid = Request::createFromGlobals()->header('sessionid');
        if ($sessionid){
            $this->sessionId = $sessionid;
        }
        $authorization = Request::createFromGlobals()->header('authorization');
        if($authorization) {
            if (0 === stripos($authorization, 'Bearer')) {
                $token = substr($authorization, 7);
                $this->token = $token;
                $token_param = explode(" ", $authorization);
                if ($token_param) {
                    $this->token = isset($token_param[1]) ? $token_param[1] : '';
                }
            }
        }
        $params = [];
        if (!$_GET){
//            if (!$_POST){
//                if (file_get_contents('php://input')){
//                    $params = file_get_contents('php://input');
//                }
//            }else{
//                $params = $_POST;
//            }
            if (file_get_contents('php://input')){
                $params = json_decode(file_get_contents('php://input'),true);
            }else{
                $params = [];
            }
        }else{
            $params = $_GET;
        }
        // $params['token'] =  $this->token;
        // $params['sessionid'] =  $this->sessionId;

        $this->params = $params;
    }
}
