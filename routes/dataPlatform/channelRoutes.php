<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: ����4:11
 */

// ƽ̨�б�·��
Route::match(['get'], 'api/channel/list', 'ChannelController@getChannelList');

// ��� �޸�ƽ̨��Ϣ
Route::match(['post'], 'api/create/channel', 'ChannelController@createChannel');
