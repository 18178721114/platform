<?php
/**
 * Created by PhpStorm.
 * User: zhenliye
 * Date: 2019/5/7
 * Time: ����4:11
 */

// �������б�·��
Route::match(['get'], 'api/developer/list', 'DeveloperController@getDeveloperList');

// ��� �޸Ŀ�������Ϣ
Route::match(['post'], 'api/create/developer', 'DeveloperController@createDeveloper');
