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

class OauthMiddleware extends Middleware
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
        // access_token 验证

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
        $server = new Server($storage,array('enforce_state'=>false,'access_lifetime'=>env('ACCESS_LIFETIME')));

        // Add the "Client Credentials" grant type (it is the simplest of the grant types)
        $server->addGrantType(new ClientCredentials($storage));

        // Add the "Authorization Code" grant type (this is where the oauth magic happens)
        $server->addGrantType(new AuthorizationCode($storage));

        $server->addGrantType(new UserCredentials($storage));

        $server->addGrantType(new RefreshToken($storage));

        $request = Request::createFromGlobals();


        $this->tokenData = $server->getAccessTokenData($request);

        if (!$this->tokenData) {
            ApiResponseFactory::apiResponse([], [], 301);
        } else {
            return $next($request);
        }
    }
}
