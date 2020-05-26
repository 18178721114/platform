<?php

namespace App\Http\Middleware;

use Illuminate\Foundation\Http\Middleware\VerifyCsrfToken as Middleware;
use Closure;
use App\Common\ApiResponseFactory;
class AuthMiddleware extends Middleware
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

        $result = $request->header('ProbeHeader');
        if($result != env('PROBESECERTKEY')){
            ApiResponseFactory::apiResponse([], [], 501);
        }
        return $next($request);
    }
}
