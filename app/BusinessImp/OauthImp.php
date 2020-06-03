<?php

namespace App\BusinessImp;

use App\Common\CurlRequest;
use OAuth2\GrantType\RefreshToken;
use OAuth2\Server;
use OAuth2\Storage\Pdo;
use OAuth2\GrantType\AuthorizationCode;
use OAuth2\GrantType\ClientCredentials;
use OAuth2\GrantType\UserCredentials;
use OAuth2\Request;
use OAuth2\Response;
use App\Common\ApiResponseFactory;
use App\Common\ApiResponse;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Artisan;
use App\Common\Service;
use Illuminate\Support\Facades\Redis;

class OauthImp
{
    // 初始化oauth2服务
    private function _server () {
        $host = env('DB_HOST');
        $dbname = env('DB_DATABASE');
        $dbParams = array(
            'dsn'      => "mysql:host=$host;port=3306;dbname=$dbname;charset=utf8;",
            'username' => env('DB_USERNAME'),
            'password' => env('DB_PASSWORD'),
        );

        // $dsn is the Data Source Name for your database, for exmaple "mysql:dbname=my_oauth2_db;host=localhost"
        $storage = new Pdo($dbParams);

        // Pass a storage object or array of storage objects to the OAuth2 server class
        $server = new Server($storage, array('enforce_state'=>false,'access_lifetime'=>env('ACCESS_LIFETIME')));

        // Add the "Client Credentials" grant type (it is the simplest of the grant types)
        $server->addGrantType(new ClientCredentials($storage));

        // Add the "Authorization Code" grant type (this is where the oauth magic happens)
        $server->addGrantType(new AuthorizationCode($storage));

        $server->addGrantType(new UserCredentials($storage));

        $server->addGrantType(new RefreshToken($storage));

        return $server;
    }

    /**
     * 获取授权code
     */
    public function authorizecode () {

        $request = Request::createFromGlobals();
        $server = $this->_server();
        $response = $server->handleAuthorizeRequest($request, new Response(), true);
        $headers        =  $response->getHttpHeaders();
        if(!isset($headers['Location'])) ApiResponseFactory::apiResponse([] , [], 300);
        // 根据redirect_uri重定向
//        header("Location: http://{$headers['Location']}");
//        exit;
        $data           =  parse_url($headers['Location']);
        parse_str($data['query'],$data);
        ApiResponseFactory::apiResponse($data , []);
        return false;

    }

    /**
     * 获取access_token
     *
     * 返回JSON结构数据
     * {
            access_token: "977b1077556e9b23ff07ef7606a5eaf947f27d41",
            expires_in: 3600,
            token_type: "Bearer",
            scope: "basic",
            refresh_token: "d2367887bdd743121adfe5fda5083064439f1cb1"
        }
     */
    public function tokenInfo () {

        $request = Request::createFromGlobals();

        $client_id = $request->request['client_id'];
        $client_secret = $request->request['client_secret'];

        $oauthClient = DB::table('oauth_clients')->where(['client_id' =>  $client_id, 'client_secret' => $client_secret])->first();

        if(!$oauthClient){
            (new ApiResponse)->setCode(300);
            (new ApiResponse)->send([],[]);
        }
        $server = $this->_server();
        $request = Request::createFromGlobals();
        $response =  $server->handleTokenRequest($request,new Response(),true);
        $data     =  $response->getParameters();

        if($oauthClient->user_id){
            if($data['access_token']){
                DB::table('oauth_access_tokens')->where('access_token' ,$data['access_token'])->update(['user_id' => $oauthClient->user_id]);
            }
        }
        //有效期
        $time = time();
        $data['access_expires_in'] = $time+$data['expires_in'];
        ApiResponseFactory::apiResponse($data,[]);


    }

    /*
     * refresh_token
     */

    public function tokenRefresh () {
        $server = $this->_server();
        $request = Request::createFromGlobals();
        $response =  $server->handleTokenRequest($request);
        ApiResponseFactory::apiResponse($response->getParameters(),[]);
    }


    /*
     * get gdt code
     */
//    public function getGdtCode(){
//        if(isset($_GET['authorization_code']) && $_GET['authorization_code']){
//            $code = $_GET['authorization_code'];
//            $params = [
//                'grant_type' => 'authorization_code',
//                'client_id' => env('GDT_CLIENT_ID'),
//                'client_secret' => env('GDT_CLIENT_SECRET'),
//                'authorization_code' => $code,
//                'redirect_uri' => env('GDT_CALLBACL_URL'),
//            ];
//            $url = env('GDT_OAUTH_URL').'oauth/token'. '?' . http_build_query($params, null, '&');
//            $result = CurlRequest::curl_gdt_get($url);
//            $res = json_decode($result,true);
//            if($res['data']){
//                $data = $res['data'];
//                $file = '../gdtToken.json';
//                $res = fopen($file,'w+');
//                $time = time();
//                $data['expires_in'] = $time + $data['access_token_expires_in'];
//                fwrite($res,json_encode($data));
//                fclose($res);
//            }
//        }else{
//            echo 'authorization_code获取失败';
//        }
//    }

    public function getGdtCode(){
        if(isset($_GET['account_id']) && $_GET['account_id']){
            if(isset($_GET['authorization_code']) && $_GET['authorization_code']){

                $account_id = $_GET['account_id'];

                $gdt_app_list = [
                    [
                        'client_id' => env('GDT_CLIENT_ID1'),
                        'client_name' => env('GDT_CLIENT_NAME1'),
                        'client_secret' => env('GDT_CLIENT_SECRET1'),
                        'account_id' => env('GDT_ACCOUTN_ID1'),
                        'advertiser_id' => env('GDT_ADVERTISER_ID1'),
                        'advertiser_name' => env('GDT_ADVERTISER_NAME1'),
                    ],
                    [
                        'client_id' => env('GDT_CLIENT_ID2'),
                        'client_name' => env('GDT_CLIENT_NAME2'),
                        'client_secret' => env('GDT_CLIENT_SECRET2'),
                        'account_id' => env('GDT_ACCOUTN_ID2'),
                        'advertiser_id' => env('GDT_ADVERTISER_ID2'),
                        'advertiser_name' => env('GDT_ADVERTISER_NAME2'),
                    ],
                    [
                        'client_id' => env('GDT_CLIENT_ID3'),
                        'client_name' => env('GDT_CLIENT_NAME3'),
                        'client_secret' => env('GDT_CLIENT_SECRET3'),
                        'account_id' => env('GDT_ACCOUTN_ID3'),
                        'advertiser_id' => env('GDT_ADVERTISER_ID3'),
                        'advertiser_name' => env('GDT_ADVERTISER_NAME3'),
                    ],
                    [
                        'client_id' => env('GDT_CLIENT_ID4'),
                        'client_name' => env('GDT_CLIENT_NAME4'),
                        'client_secret' => env('GDT_CLIENT_SECRET4'),
                        'account_id' => env('GDT_ACCOUTN_ID4'),
                        'advertiser_id' => env('GDT_ADVERTISER_ID4'),
                        'advertiser_name' => env('GDT_ADVERTISER_NAME4'),
                    ],
                    [
                        'client_id' => env('GDT_CLIENT_ID5'),
                        'client_name' => env('GDT_CLIENT_NAME5'),
                        'client_secret' => env('GDT_CLIENT_SECRET5'),
                        'account_id' => env('GDT_ACCOUTN_ID5'),
                        'advertiser_id' => env('GDT_ADVERTISER_ID5'),
                        'advertiser_name' => env('GDT_ADVERTISER_NAME5'),
                    ],
                    [
                        'client_id' => env('GDT_CLIENT_ID6'),
                        'client_name' => env('GDT_CLIENT_NAME6'),
                        'client_secret' => env('GDT_CLIENT_SECRET6'),
                        'account_id' => env('GDT_ACCOUTN_ID6'),
                        'advertiser_id' => env('GDT_ADVERTISER_ID6'),
                        'advertiser_name' => env('GDT_ADVERTISER_NAME6'),
                    ]
                ];

                $code = $_GET['authorization_code'];

                var_dump($account_id,$code);
                foreach ($gdt_app_list as $key => $gdt_app_info){
                    if ($account_id == $gdt_app_info['account_id']){

                        $token_file = "../".$gdt_app_info['account_id'].".txt";
                        var_dump($token_file);
                        $params = [
                            'grant_type' => 'authorization_code',
                            'client_id' => $gdt_app_info['client_id'],
                            'client_secret' => $gdt_app_info['client_secret'],
                            'authorization_code' => $code,
                            'redirect_uri' => env('GDT_CALLBACL_URL'),
                        ];
                        $url = env('GDT_OAUTH_URL').'oauth/token'. '?' . http_build_query($params, null, '&');
                        $result = CurlRequest::curl_gdt_get($url);
                        $res = json_decode($result,true);
                        var_dump($res);
                        if($res['data']){
                            $data = $res['data'];
                            $time = time();
                            $data['expires_in'] = $time + $data['access_token_expires_in'];
                            file_put_contents($token_file, json_encode($data));
                        }
                    }
                }

            }else{
                echo 'authorization_code获取失败';
            }
        }else{
            echo 'ok';
        }

    }

    // getKuaishouCode

    public function getKuaishouCode(){
        if(isset($_GET['account_id']) && $_GET['account_id']){
            if(isset($_GET['auth_code']) && $_GET['auth_code']){
                $account_id = $_GET['account_id'];
                $ks_app_list = [
                    [
                        'client_id' => env('KS_CLIENT_ID1'),
                        'client_name' => env('KS_CLIENT_NAME1'),
                        'client_secret' => env('KS_CLIENT_SECRET1'),
                        'account_id' => env('KS_ACCOUTN_ID1'),
                        'advertiser_id' => env('KS_ADVERTISER_ID1'),
                        'advertiser_name' => env('KS_ADVERTISER_NAME1'),
                    ]
                ];

                $code = $_GET['auth_code'];
                foreach ($ks_app_list as $key => $ks_app_info){
                    if ($account_id == $ks_app_info['account_id']){
                        $token_file = "../ks_".$ks_app_info['account_id'].".txt";
                        $params = [
                            'app_id' => $ks_app_info['client_id'],
                            'secret' => $ks_app_info['client_secret'],
                            'auth_code' => $code
                        ];
                        $url = env('KS_OAUTH_URL').'oauth2/authorize/access_token';
                        $header = ["Content-Type:application/json"];
                        $result = CurlRequest::curl_header_json_Post($url,$params,$header);
                        $res = json_decode($result,true);
                        if($res['data']){
                            $data = $res['data'];
                            $time = time();
                            $data['expires_in'] = $time + $data['access_token_expires_in'];
                            file_put_contents($token_file, json_encode($data));
                        }
                    }
                }

            }else{
                echo 'auth_code获取失败';
            }
        }else{
            echo 'ok';
        }

    }



    // getTiktokCode

    public function getTiktokCode(){
        if(isset($_GET['auth_code']) && $_GET['auth_code']){
            $tiktok_conf_arr = [
                'username' => 'promtion@zplay.com', 'pass' => 'Zpl@y1119', 'app_id' => 1648343684797446, 'secret' => '6c566e4b401d6fb14a0418e07eb30abd1681e202'
            ];

            $code = $_GET['auth_code'];
            // todo 此处为生成access_token代码
            $account_name = $tiktok_conf_arr['username'];
            $token_file = "../tiktok_".$account_name.".txt";
            $get_access_token_data = [
                'app_id'=>$tiktok_conf_arr['app_id'],
                'secret'=>$tiktok_conf_arr['secret'],
                'grant_type'=>'auth_code',
                'auth_code'=>$code
            ];
            $get_access_token_url = "https://ads.tiktok.com/open_api/oauth2/access_token/";
            $get_access_token_result = CurlRequest::curl_header_json_Post($get_access_token_url, $get_access_token_data,[]);
            var_dump($get_access_token_result);
            file_put_contents($token_file, $get_access_token_result);
            echo 'ok';

        }else{
            echo 'auth_code获取失败';
        }

    }

    // getToutiaoCode
    public function getToutiaoCode(){
        Redis::select(0);
        if(isset($_GET['advertiser_id']) && $_GET['advertiser_id']){
            if(isset($_GET['auth_code']) && $_GET['auth_code']){

                $advertiser_id = $_GET['advertiser_id'];
                $code = $_GET['auth_code'];

                $toutiao_app_list = DB::select("select distinct platform_id,data_account as username,account_app_id as app_id,account_token as secret,account_user_id as advertiser_id from c_platform_account_mapping   where platform_id = 'ptg66' and  account_user_id is not null and account_token is not null and account_app_id is not null");
                $toutiao_app_list = Service::data($toutiao_app_list);

                foreach ($toutiao_app_list as $key => $toutiao_app_info){
                    if ($advertiser_id == $toutiao_app_info['advertiser_id']){

                        $params = [
                            'app_id'=>$toutiao_app_info['app_id'],
                            'secret'=>$toutiao_app_info['secret'],
                            'grant_type'=>'auth_code',
                            'auth_code'=>$code
                        ];

                        $url = 'https://ad.toutiao.com/open_api/oauth2/access_token/';
                        $result = CurlRequest::curl_post_https($url,$params);
                        $res = json_decode($result,true);
                        var_dump($res);
                        if($res){
                            Redis::set($advertiser_id,json_encode($res));
                        }
                    }
                }

            }else{
                echo 'authorization_code获取失败';
            }
        }else{
            echo 'ok';
        }

    }


    // getSnapchatCode

    public function getSnapchatCode(){
        Redis::select(0);
        if(isset($_GET['code']) && $_GET['code']){
            $code = $_GET['code'];
            // todo 此处为生成access_token代码
            $get_access_token_data = [
                'client_id'=>'69da582e-40f7-4284-a94f-b9eecd3d85b0',
                'client_secret'=>'3d5f45d44d3d89ca2b2e',
                'grant_type'=>'authorization_code',
                'code'=>$code,
                'redirect_uri'=>"https://erp-api.zplay.cn/api/snapchat/code"
            ];
            $get_access_token_url = "https://accounts.snapchat.com/login/oauth2/access_token";
            $get_access_token_result = CurlRequest::curl_header_Post($get_access_token_url, $get_access_token_data,[]);
            var_dump($get_access_token_result);
            $get_access_token_result = json_decode($get_access_token_result,true);
            if ($get_access_token_result && isset($get_access_token_result['access_token'])) {
                Redis::set('snapchat_sino_access_token', json_encode($get_access_token_result));
                echo 'ok';
            }

        }else{
            echo 'auth_code获取失败';
        }

    }


    // 数据接口
    public function dataCommond(){
        $start_date = $_GET['start_time'];
        if (!$start_date ){
            echo "开始时间不能为空";die;
        }
        $end_date = $_GET['end_time'];
        if (!$end_date ){
            echo "结束时间不能为空";die;
        }
        $type = $_GET['type'];
        if (!$type ){
            echo "类型不能为空(1抓数OR2数据处理)";die;
        }
        $platform_id = $_GET['platform_id'];
        if (!$platform_id ){
            echo "平台ID不能为空";die;
        }
        if ($start_date && $end_date && $type && $platform_id) {
            $commond_info = DB::table('c_data_commond')->select("*")->where(['platform_id'=>$platform_id, 'type'=>$type])->first();
            $commond_info = Service::data($commond_info);
            var_dump($commond_info);
            if ($commond_info) {
                $commond_name = $commond_info['commond_name'];
                $data_account_arr = $commond_info['data_account'];
                $data_account_arr = json_decode($data_account_arr);
                if ($start_date == $end_date) {
                    if ($data_account_arr){
                        foreach ($data_account_arr as $data_account){
                            var_dump(trim($commond_name),$start_date,$data_account);
                            Artisan::call($commond_name, ['dayid' => $start_date,'data_account' => $data_account]);
                        }
                    }else{
                        if ($platform_id == 'pad44'){
                            var_dump(trim($commond_name),$start_date);
                            Artisan::call($commond_name, ['start_date' => $start_date,'end_date' => $end_date]);
                        }else{
                            var_dump(trim($commond_name),$start_date);
                            Artisan::call($commond_name, ['dayid' => $start_date]);
                        }
                    }

                } elseif ($start_date < $end_date) {
                    if ($platform_id == 'pad44'){
                        var_dump(trim($commond_name),$start_date,$end_date);
                        Artisan::call($commond_name, ['start_date' => $start_date,'end_date' => $end_date]);
                    }else {
                        $diff_day = Service::diffBetweenTwoDays($start_date, $end_date);
                        $date_arr = [];
                        for ($i = 0; $i <= $diff_day; $i++) {
                            $date_arr[] = date('Y-m-d', strtotime("+$i days", strtotime($start_date)));
                        }
                        if ($date_arr) {
                            foreach ($date_arr as $dayid) {
                                if ($data_account_arr) {
                                    foreach ($data_account_arr as $data_account) {
                                        var_dump(trim($commond_name), $dayid, $data_account);
                                        Artisan::call($commond_name, ['dayid' => $dayid, 'data_account' => $data_account]);
                                    }
                                } else {
                                    var_dump(trim($commond_name), $dayid);
                                    Artisan::call($commond_name, ['dayid' => $dayid]);
                                }
                            }
                        }
                    }
                } else {
                    echo "开始时间不能大于结束时间";
                }
            }else{
                echo "当前平台无法手动处理，请联系技术人员";
            }
        }

    }


    public function test(){
        // for ($i=0; $i <=30 ; $i++) { 
        //    $date = date('Y-m-d',strtotime('2019-10-01')+(86400*$i));
        //    Artisan::call('WechatHandworkFfHandleProcesses',['dayid'=>$date]);
        // }die;
Artisan::call('HandWorkDataProcesses'); die;
    Artisan::call('HandWorkDataProcesses',['begin_date'=>'2019-12-16','end_date'=>'2019-12-16']); die;

// $prize_arr = array(   
//     '0' => array('id'=>1,'prize'=>'MAC','rate'=>5),       
//     '1' => array('id'=>2,'prize'=>'抱歉!再接再厉','rate'=>95)
// );   


// foreach ($prize_arr as $key => $val) {   
//     $arr[$val['id']] = $val['rate'];   
// } 
// $num = $this->get_rand($arr);
// var_dump($arr);
// var_dump($num);die;




        // for ($i=37; $i >=7 ; $i--) { 
        //     $date_arr[] = date('Y-m-d',strtotime("-$i days"));
        // }
        
        //Artisan::call('FlurryTjHandleProcesses',['dayid'=>'2019-07-16','data_account'=>'jane.wang@zplay.com']);
    }
    public   function get_rand($proArr) { 

            $result = ''; 
            //概率数组的总概率精度
            $proSum = array_sum($proArr); 
            //概率数组循环  
            foreach ($proArr as $key => $proCur) { 
                $randNum = mt_rand(1, $proSum);             
                if ($randNum <= $proCur) { 
                    $result = $key;                       
                    break; 
                } else { 
                    $proSum -= $proCur;                     
                } 
            } 
            unset ($proArr); 
            return $result; 
    }

}
