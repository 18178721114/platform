<?php

namespace App\Http\Middleware;

use Illuminate\Foundation\Http\Middleware\VerifyCsrfToken as Middleware;
use Closure;
use App\Common\ApiResponseFactory;
use OAuth2\GrantType\RefreshToken;
use OAuth2\Server;
use OAuth2\Storage\Pdo;
use OAuth2\GrantType\AuthorizationCode;
use OAuth2\GrantType\ClientCredentials;
use OAuth2\GrantType\UserCredentials;
use OAuth2\Request;
use App\BusinessLogic\UserLogic;
use App\Common\Service;

class CheckUserMiddleware extends Middleware
{
    /**
     * Handle an incoming request.
     *
     * @param  \Illuminate\Http\Request  $request
     * @param  \Closure  $next
     * @param  string|null  $guard
     * @return mixed
     */
    public function handle($request, Closure $next, $guard = null)
    {

        $dir = './storage/country';
        if (!is_dir($dir)) {
            mkdir($dir,0777,true);
        }
        $logFilename = $dir.'/'.'country.log';
        //生成日志
        file_put_contents( $logFilename,'测试' . "\n\n",FILE_APPEND);
        file_put_contents( $logFilename,json_encode($_SESSION) . "\n\n",FILE_APPEND);
        if(!isset($_SESSION['erm_data']['expireTime'])|| (time() - $_SESSION['erm_data']['expireTime']) > 0) {
            file_put_contents( $logFilename,json_encode($_SESSION) . "\n\n",FILE_APPEND);
            unset($_SESSION['erm_data']);
            ApiResponseFactory::apiResponse([],[],1002);
        }

        $map['user.user_account'] = $_SESSION['erm_data']['email'];
        $map['user.id'] = $_SESSION['erm_data']['guid'];
        $map['user.status'] = 1;
        $fields = ['user.function_permission','user.id','user.name','user.user_account','user.role_id'];
        //验证用户是否有权限登录
        $userInfo = UserLogic::Userlist($map,$fields)->get();
        $userInfo =Service::data($userInfo);
        file_put_contents( $logFilename,json_encode($userInfo) . "\n\n",FILE_APPEND);
        if(!$userInfo) ApiResponseFactory::apiResponse([],[],1002);
        return $next($request);

    }
}
