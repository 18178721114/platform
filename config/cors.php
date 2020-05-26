<?php

return [

    /*
    |--------------------------------------------------------------------------
    | Laravel CORS
    |--------------------------------------------------------------------------
    |
    | allowedOrigins, allowedHeaders and allowedMethods can be set to array('*')
    | to accept any value.
    |
    */
   
    'supportsCredentials' => true,
//    'allowedOrigins' => ['http://erm-test.zplay.cn','http://erm-older-test.zplay.cn','https://erm.zplay.cn'],
    'allowedOrigins' => ['*'],
    'allowedOriginsPatterns' => [],
    'allowedHeaders' => ['DNT','X-CustomHeader','Keep-Alive','User- Agent','X-Requested-With','If-Modified-Since','Cache-Control','Content-Type','token'],
    'allowedMethods' => ['GET', 'POST', 'OPTIONS', 'PUT', 'DELETE'],
    'exposedHeaders' => [],
    'maxAge' => 0,

];
