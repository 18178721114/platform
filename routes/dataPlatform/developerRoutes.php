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

// ������ƽ̨Ӧ�ÿ����б�ӿ�
Route::match(['post'], 'api/develop/app/list', 'DeveloperController@developAppList');

// ������ƽ̨Ӧ�ÿ���״̬�޸Ľӿ�
Route::match(['post'], 'api/develop/app/status', 'DeveloperController@developAppStatus');

// ������ƽ̨�û�ע��ӿ�
Route::match(['post'], 'api/develop/user/register', 'DeveloperController@developUserRegister');
