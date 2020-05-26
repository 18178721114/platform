<?php

namespace App\Http\Controllers;

use App\BusinessImp\OauthImp;

class OauthController
{
    /**
     * 获取授权code
     */
    public function authorizecode () {
        (new OauthImp())->authorizecode();
    }

    /**
     * 获取access_token
     */
    public function token() {
        (new OauthImp())->tokenInfo();
    }

    /**
     * refresh_token
     */
    public function tokenRefresh () {
        (new OauthImp())->tokenRefresh();
    }

    /**
     * refresh_token
     */
    public function getGdtCode () {
        (new OauthImp())->getGdtCode();
    }

    /**
     * refresh_token
     */
    public function getKuaishouCode () {
        (new OauthImp())->getKuaishouCode();
    }

    /**
     * refresh_token
     */
    public function getTiktokCode () {
        (new OauthImp())->getTiktokCode();
    }

    /**
     * refresh_token
     */
    public function getToutiaoCode () {
        (new OauthImp())->getToutiaoCode();
    }

    /**
     * snapchat
     */
    public function getSnapchatCode () {
        (new OauthImp())->getSnapchatCode();
    }

        /**
     * 测试文件
     */
    public function test () {
        (new OauthImp())->test();
    }

    /**
     * 数据接口
     */
    public function dataCommond(){
        (new OauthImp())->dataCommond();
    }


}
