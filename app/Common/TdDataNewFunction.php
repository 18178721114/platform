<?php
/*
 * talkingdata 第二个账数据新接口 分国家 
 * platformid ，1代表android，2代表ios 4代表WinPhone  老账号1为android 2为ios
 * @accesskey 534176a4b882fd88f822a0af3ec3f84a每个应用都有不同的accesskey
 * 文档地址https://www.talkingdata.com/app/document_web/index.jsp?statistics
 * https请求 获取数据
 * @day 20151027
 * @author daijingmei
 * @mail daijingmei@zplay.cn
 * 
 * 
*/
namespace App\Common;

use App\BusinessImp\DataImportImp;
use App\BusinessImp\DataSearchImp;
use App\BusinessLogic\DataSearchLogic;
use Illuminate\Support\Facades\DB;

class TdDataNewFunction{
	private static $data_url = "https://api.talkingdata.com/metrics/app/v1";//获取数据的url地址
	private static $version_url = "https://api.talkingdata.com/metrics/app/v1/versionlist"; //关于版本
	private static $channel_url = "https://api.talkingdata.com/metrics/app/v1/channellist"; //关于渠道
	private static $accesskey;
	private static $platformid; //1代表ios 2代表android

	private static $table_name='talkingdata_all';
    private static $gntjtable_name='talkingdata_china_session'; // 国内新账号的新表
    private static $tjtable_name='talkingdata_user'; //old 新账号的新表
    private static $sessiontable_name='talkingdata_sessionlength'; //old 新账号的新表 时长和启动次数

    private static $foreign_tjtable_name='talkingdata_foreign_user'; //国外 新账号的新表
    private static $foreign_sessiontable_name='talkingdata_foreign_sessionlength'; // 国外 新账号的新表 时长和启动次数
    private static $foreign_channel='talkingdata_foreign_channel'; // 国外 新账号的新表 按渠道分组  取所有数据

	//发送curl请求
	public static function curlPost($url, $data = array(), $timeout = 90, $CA = false){
		#print_r($url);
		$cacert = getcwd() . '/cacert.pem'; //CA根证书
		$SSL = substr($url, 0, 8) == "https://" ? true : false;

		$ch = curl_init();
		curl_setopt($ch, CURLOPT_URL, $url);
		curl_setopt($ch, CURLOPT_TIMEOUT, $timeout);
		curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, $timeout-2);
		if ($SSL && $CA) {
			curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, true);   // 只信任CA颁布的证书
			curl_setopt($ch, CURLOPT_CAINFO, $cacert); // CA根证书（用来验证的网站证书是否是CA颁布）
			curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 2); // 检查证书中是否设置域名，并且是否与提供的主机名匹配
		} else if ($SSL && !$CA) {
			curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false); // 信任任何证书
			curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0); // 检查证书中是否设置域名
		}
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
		//    curl_setopt($ch, CURLOPT_HTTPHEADER, array('Expect:')); //避免data数据过长问题
		curl_setopt($ch, CURLOPT_HTTPHEADER, array('Content-Type: application/json','Content-Length: ' . strlen($data)));
		curl_setopt($ch, CURLOPT_POST, true);
		curl_setopt($ch, CURLOPT_POSTFIELDS, $data);
		#print_r($data);
		//curl_setopt($ch, CURLOPT_POSTFIELDS, http_build_query($data)); //data with URLEncode

		$ret = curl_exec($ch);
		#var_dump(curl_error($ch));  //查看报错信息

		curl_close($ch);
		return $ret;
	}
	//获取新增和活跃用户
	public  static function getData($versionInfo=array(),$channelInfo=array(),$gameinfo=array(),$day){
		/**
		 * {
			 "accesskey": "fb46c6980e294be483fa********be35"// *填写应用的接入码
			    "metrics":["newuser"],              // *填写要查询的指标项
			    "groupby":"daily",                  // *填写数据维度，即数据分组方式
			    "filter":{                              // *数据筛选条件
			       "start":"2015-04-14",            // *查询时期的起始日
			       "end":"2015-04-15",             // *查询时期的截止日
			       "platformids":[1,2],                 //限定要查询的系统平台
			       "versions":["1.0","2.0"],         //限定应用版本号
			       "channelids":[1605,1607],           //限定查询渠道
			       "eventids":["clear","addCar"],   //限定查询的事件id
			       "pagenames":["index","pay"]   //限定查询的页面名
			 },
			    "order":"desc",                // 数据结果排序方式，除按时间分组外默认倒序
			    "limit":10,                       // 限定返回数据的条数
			    "sum":true,                      // 返回结果中给出数据总和
			    "avg":false                      // 返回结果中给出数据的平均值
			 }
		 */

		$groupby = 'geocountry';//分国家
		foreach($versionInfo as $vkey=>$version){
			foreach ($channelInfo as $ckey=>$channel){
				$search['filter'] = array('start'=>$day,'end'=>$day,'platformids'=>array(self::$platformid),'channelids'=>array($ckey),'versions'=>array($vkey));
				$search['groupby']='geocountry';//分国家
				$search['metrics'] =array('activeuser');
				$search['accesskey'] = self::$accesskey;
				$search_data = json_encode($search);
				$active_data = self::curlPost(self::$data_url,$search_data); //新增
				$active_data = json_decode($active_data,true);
				$active_result = $active_data['result'];
				foreach ($active_result as $active_info){
					if($active_info['activeuser']!=0){
						 $active_sql="insert into ". self::$table_name ." (day,appid,game_name,channel_name,active_user,version,platformid,country) values('{$day}','{$gameinfo['appid']}','{$gameinfo['gamename']}','{$channel}','{$active_info['activeuser']}','{$version}', ".self::$platformid." ,'{$active_info['Name']}') ";
						 DB::insert($active_sql);
					}
				}
				$search['metrics'] =array('newuser');
               	$search_data = json_encode($search);
             	$new_data = self::curlPost(self::$data_url,$search_data); //活跃
             	$newuser_result = json_decode($new_data,true);
             	foreach($newuser_result['result'] as $newuser_info){
             		if($newuser_info['newuser']!=0){
                        $newuser_sql="update ". self::$table_name ." set new_user = '{$newuser_info['newuser']}' where day='{$day}' and appid ='{$gameinfo['appid']}' and channel_name = '{$channel}' and version = '{$version}' and platformid = ".self::$platformid." and  country = '{$newuser_info['Name']}' ";
                        DB::update($newuser_sql);
             		}

             	}


			}
		}
	}

	/**
	 * @desc 获取新增和活跃
	 * @param string $accesskey
	 * @param array $gameinfo
	 * @param int $platform
	 * @param string $day
	 * @param array $channel
	 * @param array $version
	 * @param string $groupby
	 */
	public  static function CreateData($accesskey,$gameinfo,$platform,$day,$channel=array(),$version=array(),$groupby){
		$groupby = $groupby ? $groupby : 'daily';
		if(!empty($channel)){
			foreach ($channel as $ckey=>$cv){
				$search['filter']['channelids'] = array($ckey);
			}

		}
		if(!empty($version)){
			foreach ($version as $vkey=>$vv){
				$search['filter']['versions'] = array($vkey);
			}
		}
		$search['filter']['start'] = $day;
		$search['filter']['end'] = $day;
		$search['filter']['platformids'] = array($platform);
		$search['accesskey'] = $accesskey;
		$search['groupby']=$groupby;
		$search['metrics'] = array('activeuser');
		self::saveLog('TalkingData', date('Y-m-d H:i:s')."：".json_encode($search));
		//获取活跃
        sleep(3);
		$active_result = self::curlPost(self::$data_url,json_encode($search));
        self::saveLog('TalkingData', date('Y-m-d H:i:s')."结果：".$active_result);
		$active_result = json_decode($active_result,true);
//        var_dump($active_result);
		//获取新增
		unset($search['metrics']);
		$search['metrics'] = array('newuser');
        sleep(3);
		$new_result = self::curlPost(self::$data_url,json_encode($search));
		$new_result = json_decode($new_result,true);
//		var_dump($new_result);
        self::saveLog('TalkingData', date('Y-m-d H:i:s')."：".json_encode($search));
        self::saveLog('TalkingData', date('Y-m-d H:i:s')."结果：".json_encode($new_result));
		//根据分组不同获取不同维度对应的数据
        $activeData = [];
        $newData = [];
		if($groupby=='appversion'){
			$versionInfo = self::getVersionList($accesskey,$platform);
            self::saveLog('TalkingData', '分版本处理'.$accesskey,'error_groupby');
//			var_dump($versionInfo);exit;

		}elseif($groupby=='channelid'){
                $channelInfo = self::getChannelList($accesskey,$platform);
                foreach($channelInfo['result'] as $d){
                    $c[$d['channelid']] = $d['channelname'];
                }
                foreach ($active_result['result'] as $data){
                    if($data['activeuser'])
                        $activeData[$c[$data['channelid']]] = $data;
                    }
                    foreach($new_result['result'] as $new){
                        if($new['newuser'])
                            $newData[$c[$new['channelid']]] = $new;
                    }
                    self::Active_New($gameinfo, $platform, $day, $groupby, $activeData, $newData, $vv,'');
                }else{
		            //geocountry
                	self::Active_New($gameinfo, $platform, $day, $groupby, $active_result['result'], $new_result['result'], $vv,$cv);
                	//exit;
		}

		//exit;
	}

	/**
	 * @desc 获取新增和活跃
	 * @param string $accesskey
	 * @param array $gameinfo
	 * @param int $platform
	 * @param string $day
	 * @param array $channel
	 * @param array $version
	 * @param string $groupby
	 */
	public  static function foreignCreateData($accesskey,$gameinfo,$platform,$day,$channel=array(),$version=array(),$groupby){
		$groupby = $groupby ? $groupby : 'daily';

		if(!empty($channel)){
			foreach ($channel as $ckey=>$channel_name){
				$search['filter']['channelids'] = [$ckey];

                if(!empty($version)){
                    foreach ($version as $vkey=>$version_name){
                        $search['filter']['versions'] = [$vkey];

                        $search['filter']['start'] = $day;
                        $search['filter']['end'] = $day;
                        $search['filter']['platformids'] = array($platform);
                        $search['accesskey'] = $accesskey;
                        $search['groupby']=$groupby;
                        $search['metrics'] = array('activeuser');
                        self::saveLog('TalkingData', date('Y-m-d H:i:s')."：".json_encode($search));
                        //获取活跃
                        sleep(3);
                        $active_result = self::curlPost(self::$data_url,json_encode($search));
                        self::saveLog('TalkingData', date('Y-m-d H:i:s')."结果：".$active_result);
                        $active_result = json_decode($active_result,true);
                        if ($active_result['status'] != 200) {
                            self::saveErrorLog($active_result['message']);
                            return;
                        }

                        //获取新增
                        unset($search['metrics']);
                        $search['metrics'] = array('newuser');
                        sleep(3);
                        $new_result = self::curlPost(self::$data_url,json_encode($search));
                        $new_result = json_decode($new_result,true);
                        if ($new_result['status'] != 200) {
                            self::saveErrorLog($new_result['message']);
                            return;
                        }

                        self::saveLog('TalkingData', date('Y-m-d H:i:s')."：".json_encode($search));
                        self::saveLog('TalkingData', date('Y-m-d H:i:s')."结果：".json_encode($new_result));
                        //根据分组不同获取不同维度对应的数据
                        $activeData = [];
                        $newData = [];
                        if($groupby=='appversion'){
                            $versionInfo = self::getVersionList($accesskey,$platform);
                            self::saveLog('TalkingData', '分版本处理'.$accesskey,'error_groupby');
//			var_dump($versionInfo);exit;

                        }elseif($groupby=='channelid'){
                            $channelInfo = self::getChannelList($accesskey,$platform);
                            if ($channelInfo['status'] != 200) {
                                self::saveErrorLog($channelInfo['message']);
                                return;
                            }
                            foreach($channelInfo['result'] as $d){
                                $c[$d['channelid']] = $d['channelname'];
                            }
                            foreach ($active_result['result'] as $data){
                                if($data['activeuser'])
                                    $activeData[$c[$data['channelid']]] = $data;
                            }
                            foreach($new_result['result'] as $new){
                                if($new['newuser'])
                                    $newData[$c[$new['channelid']]] = $new;
                            }
                            self::foreign_active_new($gameinfo, $platform, $day, $groupby, $activeData, $newData, $vkey,$version_name,'');
                        }else{
                            //geocountry
                            self::foreign_active_new($gameinfo, $platform, $day, $groupby, $active_result['result'], $new_result['result'], $vkey,$version_name,$channel_name);
                            //exit;
                        }
                    }
                }
			}
		}
	}

	/**
	 * @desc 获取新增和活跃
	 * @param string $accesskey
	 * @param array $gameinfo
	 * @param int $platform
	 * @param string $day
	 * @param array $channel
	 * @param array $version
	 * @param string $groupby
	 */
	public  static function foreignTotalCreateData($accesskey,$gameinfo,$platform,$day,$groupby){
		$groupby = $groupby ? $groupby : 'daily';

        $search['filter']['start'] = $day;
        $search['filter']['end'] = $day;
        $search['filter']['platformids'] = array($platform);
        $search['accesskey'] = $accesskey;
        $search['groupby']=$groupby;
        $search['metrics'] = array('activeuser');
        self::saveLog('TalkingData', date('Y-m-d H:i:s')."：".json_encode($search));
        //获取活跃
        sleep(3);
        $active_result = self::curlPost(self::$data_url,json_encode($search));
        self::saveLog('TalkingData', date('Y-m-d H:i:s')."结果：".$active_result);
        $active_result = json_decode($active_result,true);
        if ($active_result['status'] != 200) {
            self::saveErrorLog($active_result['message']);
            return;
        }

        //获取新增
        unset($search['metrics']);
        $search['metrics'] = array('newuser');
        sleep(3);
        $new_result = self::curlPost(self::$data_url,json_encode($search));
        $new_result = json_decode($new_result,true);
        if ($new_result['status'] != 200) {
            self::saveErrorLog($new_result['message']);
            return;
        }

        self::saveLog('TalkingData', date('Y-m-d H:i:s')."：".json_encode($search));
        self::saveLog('TalkingData', date('Y-m-d H:i:s')."结果：".json_encode($new_result));
        //根据分组不同获取不同维度对应的数据
        $activeData = [];
        $newData = [];

        $channelInfo = self::getChannelList($accesskey,$platform);
        if ($channelInfo['status'] != 200) {
            self::saveErrorLog($channelInfo['message']);
            return;
        }
        foreach($channelInfo['result'] as $d){
            $c[$d['channelid']] = $d['channelname'];
        }
        foreach ($active_result['result'] as $data){
            if($data['activeuser'])
                $activeData[$c[$data['channelid']]] = $data;
        }
        foreach($new_result['result'] as $new){
            if($new['newuser'])
                $newData[$c[$new['channelid']]] = $new;
        }
        self::foreign_total_new($gameinfo, $platform, $day, $groupby, $activeData, $newData);
	}

    /**
     * @desc 将新增和活跃数据存入数据库
     * @param array $gameinfo
     * @param int $platform
     * @param date $day
     * @param string $groupby
     * @param array $activeData
     * @param array $newData
     * @param string $version
     * @param string $channel
     */
    public  static function foreign_total_new($gameinfo,$platform,$day,$groupby,$activeData,$newData){
        foreach($gameinfo as $appid=>$appname){
            foreach ($activeData as $c=>$data){
                $active_sql = "insert into ".self::$foreign_channel." (day,appid,game_name,channel_name,active_user,platformid) values('{$day}','{$appid}','{$appname}','{$c}','{$data['activeuser']}',$platform)";
                var_dump($active_sql);
                DB::insert($active_sql);
            }
            foreach($newData as $cc=>$new){
                $new_sql = "update ". self::$foreign_channel ." set new_user='{$new['newuser']}' where day='{$day}' and channel_name='{$cc}' and platformid=$platform and appid='{$appid}'";
                var_dump($new_sql);
                DB::update($new_sql);
            }
        }
    }


	/**
	 * @desc 将新增和活跃数据存入数据库
	 * @param array $gameinfo
	 * @param int $platform
	 * @param date $day
	 * @param string $groupby
	 * @param array $activeData
	 * @param array $newData
	 * @param string $version
	 * @param string $channel
	 */
	public  static function Active_New($gameinfo,$platform,$day,$groupby,$activeData,$newData,$version='',$channel=''){
		if($groupby=='channelid'){//按渠道分组取数的
			foreach($gameinfo as $appid=>$appname){
				foreach ($activeData as $c=>$data){
					$active_sql = "insert into ".self::$tjtable_name." (day,appid,game_name,channel_name,active_user,version,platformid) values('{$day}','{$appid}','{$appname}','{$c}','{$data['activeuser']}','{$version}',$platform)";
					var_dump($active_sql);
                    DB::insert($active_sql);
				}
				foreach($newData as $cc=>$new){
					$new_sql = "update ". self::$tjtable_name ." set new_user='{$new['newuser']}' where day='{$day}' and channel_name='{$cc}' and platformid=$platform and appid='{$appid}' and version='{$version}'";
                    var_dump($new_sql);
                    DB::update($new_sql);
				}
			}
		}else{
			foreach($gameinfo as $appid=>$appname){
				foreach ($activeData as $active){
					if($active['activeuser']){ //当数据不为0时
						$active_sql = "insert into ". self::$tjtable_name ."(day,appid,game_name,channel_name,active_user,version,platformid,country) values('{$day}','{$appid}','{$appname}','{$channel}','{$active['activeuser']}','{$version}',$platform,'{$active['Name']}')";
                        var_dump($active_sql);
                        DB::insert($active_sql);
					}
				}
				foreach ($newData as $new){
					if($new['newuser']){
						$new_sql = "update ".self::$tjtable_name." set new_user='{$new['newuser']}' where day='{$day}' and channel_name='{$channel}' and platformid=$platform and appid='{$appid}' and version='{$version}' and country='{$new['Name']}'";
                        var_dump($new_sql);
                        DB::update($new_sql);
					}
				}
			}
		}
	}

	/**
	 * @desc 将新增和活跃数据存入数据库
	 * @param array $gameinfo
	 * @param int $platform
	 * @param date $day
	 * @param string $groupby
	 * @param array $activeData
	 * @param array $newData
	 * @param string $version
	 * @param string $channel
	 */
	public  static function foreign_active_new($gameinfo,$platform,$day,$groupby,$activeData,$newData,$vkey,$version='',$channel=''){
		if($groupby=='channelid'){//按渠道分组取数的
			foreach($gameinfo as $appid=>$appname){
				foreach ($activeData as $c=>$data){
					$active_sql = "insert into ".self::$foreign_tjtable_name." (day,appid,game_name,channel_name,active_user,version_id,version,platformid) values('{$day}','{$appid}','{$appname}','{$c}','{$data['activeuser']}','{$vkey}','{$version}',$platform)";
					var_dump($active_sql);
                    DB::insert($active_sql);
				}
				foreach($newData as $cc=>$new){
					$new_sql = "update ". self::$foreign_tjtable_name ." set new_user='{$new['newuser']}' where day='{$day}' and channel_name='{$cc}' and platformid=$platform and appid='{$appid}' and version='{$version}' and version_id='{$vkey}' ";
                    var_dump($new_sql);
                    DB::update($new_sql);
				}
			}
		}else{
			foreach($gameinfo as $appid=>$appname){
				foreach ($activeData as $active){
					if($active['activeuser']){ //当数据不为0时
						$active_sql = "insert into ". self::$foreign_tjtable_name ."(day,appid,game_name,channel_name,active_user,version_id,version,platformid,country) values('{$day}','{$appid}','{$appname}','{$channel}','{$active['activeuser']}','{$vkey}','{$version}',$platform,'{$active['Name']}')";
                        var_dump($active_sql);
                        DB::insert($active_sql);
					}
				}
				foreach ($newData as $new){
					if($new['newuser']){
						$new_sql = "update ".self::$foreign_tjtable_name." set new_user='{$new['newuser']}' where day='{$day}' and channel_name='{$channel}' and platformid=$platform and appid='{$appid}' and version='{$version}' and version_id='{$vkey}' and country='{$new['Name']}'";
                        var_dump($new_sql);
                        DB::update($new_sql);
					}
				}
			}
		}
	}



	//
	/**
	 * @desc 获取启动次数和时长  没有区分国家，
	 * @param string $accesskey
	 * @param array $gameinfo
	 * @param int $platform
	 * @param string $day
	 * @param array $channel
	 * @param array $version
	 * @param string $groupby
	 */
	public  static function CreateSessionData($accesskey,$gameinfo,$platform,$day,$channel=array(),$version=array(),$groupby='daily'){
		if(!empty($channel)){
			foreach ($channel as $ckey=>$cv){
				$search['filter']['channelids'] = array($ckey);
			}
				
		}
		if(!empty($version)){
			foreach ($version as $vkey=>$vv){
				$search['filter']['versions'] = array($vkey);
			}
		}
		$search['filter']['start'] = $day;
		$search['filter']['end'] = $day;
		$search['filter']['platformids'] = array($platform);
		$search['accesskey'] = $accesskey;
		$search['groupby']=$groupby?$groupby:'daily';
		$search['metrics'] = array('session');
        self::saveLog('TalkingData', date('Y-m-d H:i:s')."：".json_encode($search));
		//获取启动次数
        sleep(3);
        var_dump(self::$data_url,json_encode($search));
		$session_result = self::curlPost(self::$data_url,json_encode($search));
        self::saveLog('TalkingData', date('Y-m-d H:i:s')."结果：".$session_result);
        var_dump($session_result);
		$session_result = json_decode($session_result,true);
//		var_dump($session_result);
		//获取平均时长
	
		unset($search['metrics']);
		$search['metrics'] = array('sessionlength');
        sleep(3);
        var_dump(self::$data_url,json_encode($search));
		$sessionlength_result = self::curlPost(self::$data_url,json_encode($search));
		var_dump($sessionlength_result);
		$sessionlength_result = json_decode($sessionlength_result,true);
//		var_dump($sessionlength_result);
        self::saveLog('TalkingData', date('Y-m-d H:i:s')."：".json_encode($search));
        self::saveLog('TalkingData', date('Y-m-d H:i:s')."结果：".json_encode($sessionlength_result));
		//根据分组不同获取不同维度对应的数据
		if($groupby=='appversion'){
			$versionInfo = self::getVersionList($accesskey,$platform);
            self::saveLog('TalkingData', '分版本处理'.$accesskey,'error_groupby');
//			var_dump($versionInfo);exit;
				
		}elseif($groupby=='channelid'){
			$SessionData=array();
			$SessionlengthData = array();
			$channelInfo = self::getChannelList($accesskey,$platform);
			foreach($channelInfo['result'] as $d){
				$c[$d['channelid']] = $d['channelname'];
			}
			foreach ($session_result['result'] as $data){
				if($data['session'])
					$SessionData[$c[$data['channelid']]] = $data;
			}
			foreach($sessionlength_result['result'] as $new){
				if($new['sessionlength'])
					$SessionlengthData[$c[$new['channelid']]] = $new;
			}
			self::Session_length($gameinfo, $platform, $day, $groupby, $SessionData, $SessionlengthData, $vv,'');
		}else{ //分天
			self::Session_length($gameinfo, $platform, $day, $groupby, $session_result['result'], $sessionlength_result['result'], $vv,$cv);
		}
	
		//exit;
	}
	//将启动次数和时长存入数据库
	public  static function Session_length($gameinfo,$platform,$day,$groupby,$SessionData,$sessionlengthData,$version='',$channel=''){
		if($groupby=='channelid'){
			foreach($gameinfo as $appid=>$appname){
				foreach ($SessionData as $c=>$data){
					$active_sql = "insert into ".self::$sessiontable_name." (day,appid,game_name,channel_name,session,version,platformid) values('{$day}','{$appid}','{$appname}','{$c}','{$data['session']}','{$version}',$platform)";
					DB::insert($active_sql);
				}
				foreach($sessionlengthData as $cc=>$new){
					$new_sql = "update ".self::$sessiontable_name. " set sessionlength='{$new['sessionlength']}' where day='{$day}' and channel_name='{$cc}' and platformid=$platform and appid='{$appid}' and version='{$version}'";
                    DB::update($new_sql);
				}
			}
		}else{
			foreach($gameinfo as $appid=>$appname){
				foreach ($SessionData as $active){
					if($active['session']){ //当数据不为0时
						$active_sql = "insert into ". self::$sessiontable_name ." (day,appid,game_name,channel_name,session,version,platformid) values('{$day}','{$appid}','{$appname}','{$channel}','{$active['session']}','{$version}',$platform)";
                        DB::insert($active_sql);
					}
				}
				foreach ($sessionlengthData as $new){
					if($new['sessionlength']){
						$new_sql = "update ".self::$sessiontable_name." set sessionlength='{$new['sessionlength']}' where day='{$day}' and channel_name='{$channel}' and platformid=$platform and appid='{$appid}' and version='{$version}'";
                        DB::update($new_sql);
					}
				}
			}
		}
	}

    /**
     * @desc 获取启动次数和时长  没有区分国家，
     * @param string $accesskey
     * @param array $gameinfo
     * @param int $platform
     * @param string $day
     * @param array $channel
     * @param array $version
     * @param string $groupby
     */
    public  static function foreignCreateSessionData($accesskey,$gameinfo,$platform,$day,$groupby='daily'){

        $search['filter']['start'] = $day;
        $search['filter']['end'] = $day;
        $search['filter']['platformids'] = array($platform);
        $search['accesskey'] = $accesskey;
        $search['groupby']=$groupby?$groupby:'daily';
        $search['metrics'] = array('session');
        self::saveLog('TalkingData', date('Y-m-d H:i:s')."：".json_encode($search));
        //获取启动次数
        sleep(3);
//                var_dump(self::$data_url,json_encode($search));
        $session_result = self::curlPost(self::$data_url,json_encode($search));
        self::saveLog('TalkingData', date('Y-m-d H:i:s')."结果：".$session_result);
        var_dump($session_result);
        $session_result = json_decode($session_result,true);
        if ($session_result['status'] != 200) {
            TdDataFunction::saveErrorLog($session_result['message']);
            return;
        }

        //获取平均时长
        unset($search['metrics']);
        $search['metrics'] = array('avgsessionlength');
        sleep(3);
        var_dump(self::$data_url,json_encode($search));
        $sessionlength_result = self::curlPost(self::$data_url,json_encode($search));
        var_dump($sessionlength_result);
        $sessionlength_result = json_decode($sessionlength_result,true);
        if ($sessionlength_result['status'] != 200) {
            TdDataFunction::saveErrorLog($sessionlength_result['message']);
            return;
        }
        self::saveLog('TalkingData', date('Y-m-d H:i:s')."：".json_encode($search));
        self::saveLog('TalkingData', date('Y-m-d H:i:s')."结果：".json_encode($sessionlength_result));
        //根据分组不同获取不同维度对应的数据
        $SessionData=array();
        $SessionlengthData = array();
        $channelInfo = self::getChannelList($accesskey,$platform);
        if ($channelInfo['status'] != 200) {
            TdDataFunction::saveErrorLog($channelInfo['message']);
            return;
        }
        foreach($channelInfo['result'] as $d){
            $c[$d['channelid']] = $d['channelname'];
        }
        foreach ($session_result['result'] as $data){
            if($data['session'])
                $SessionData[$c[$data['channelid']]] = $data;
        }
        foreach($sessionlength_result['result'] as $new){
            if($new['avgsessionlength'])
                $SessionlengthData[$c[$new['channelid']]] = $new;
        }
        self::foreign_session_length($gameinfo, $platform, $day, $groupby, $SessionData, $SessionlengthData);

    }

	//将启动次数和时长存入数据库
	public  static function foreign_session_length($gameinfo,$platform,$day,$groupby,$SessionData,$sessionlengthData){
        foreach($gameinfo as $appid=>$appname){
            foreach ($SessionData as $c=>$data){
//					$active_sql = "insert into ".self::$foreign_sessiontable_name." (day,appid,game_name,channel_name,session,version,platformid) values('{$day}','{$appid}','{$appname}','{$c}','{$data['session']}','{$version}',$platform)";
//					DB::insert($active_sql);

                $new_sql = "update ".self::$foreign_channel. " set session='{$data['session']}' where day='{$day}' and channel_name='{$c}' and platformid=$platform and appid='{$appid}'";
                var_dump($new_sql);
                DB::update($new_sql);
            }
            foreach($sessionlengthData as $cc=>$new){
                $new_sql = "update ".self::$foreign_channel. " set sessionlength='{$new['avgsessionlength']}' where day='{$day}' and channel_name='{$cc}' and platformid=$platform and appid='{$appid}'";
                var_dump($new_sql);
                DB::update($new_sql);
            }
        }
	}


    /**
     * @desc 获取启动次数和时长  没有区分国家，
     * @param string $accesskey
     * @param array $gameinfo
     * @param int $platform
     * @param string $day
     * @param array $channel
     * @param array $version
     * @param string $groupby
     */
    public  static function foreignCountryCreateSessionData($accesskey,$gameinfo,$platform,$day,$channel=array(),$version=array(),$groupby='daily'){
        if(!empty($channel)){
            foreach ($channel as $ckey=>$cv){
                $search['filter']['channelids'] = array($ckey);

                if(!empty($version)){
                    foreach ($version as $vkey=>$vv){
                        $search['filter']['versions'] = array($vkey);
                        $search['filter']['start'] = $day;
                        $search['filter']['end'] = $day;
                        $search['filter']['platformids'] = array($platform);
                        $search['accesskey'] = $accesskey;
                        $search['groupby']=$groupby?$groupby:'daily';
                        $search['metrics'] = array('session');
                        self::saveLog('TalkingData', date('Y-m-d H:i:s')."：".json_encode($search));
                        //获取启动次数
                        sleep(3);
//                        var_dump(self::$data_url,json_encode($search));
                        $session_result = self::curlPost(self::$data_url,json_encode($search));
                        self::saveLog('TalkingData', date('Y-m-d H:i:s')."结果：".$session_result);
//                        var_dump($session_result);
                        $session_result = json_decode($session_result,true);
                        if ($session_result['status'] != 200) {
                            TdDataFunction::saveErrorLog($session_result['message']);
                            return;
                        }

                        self::foreign_country_session_length($gameinfo, $platform, $day, $groupby, $session_result['result'], $vkey,$vv,$cv);
                    }
                }
            }

        }
    }

    //将启动次数和时长存入数据库
    public  static function foreign_country_session_length($gameinfo,$platform,$day,$groupby,$SessionData,$vkey,$version='',$channel=''){

        foreach($gameinfo as $appid=>$appname){
            foreach ($SessionData as $active){
                if($active['session']){ //当数据不为0时
//						$active_sql = "insert into ". self::$foreign_sessiontable_name ." (day,appid,game_name,channel_name,session,version,platformid) values('{$day}','{$appid}','{$appname}','{$channel}','{$active['session']}','{$version}',$platform)";
//                        DB::insert($active_sql);
                    $new_sql = "update ".self::$foreign_tjtable_name." set session='{$active['session']}' where day='{$day}' and channel_name='{$channel}' and platformid=$platform and appid='{$appid}' and version='{$version}' and version_id='{$vkey}' and country = '{$active['Name']}' ";
                    var_dump($new_sql);
                    DB::update($new_sql);
                }
            }
        }
    }
	
	//获取游戏版本信息
	public static function getVersionList($accesskey,$platform = ''){
		$request['accesskey'] = $accesskey;
		$request['filter'] = array('platformids'=>array($platform));
		$request = json_encode($request);
        sleep(3);
		$data = self::curlPost(self::$version_url,$request);
//		var_dump($data);
		return  json_decode($data,true);	
	}

	//获取渠道信息
	public static function getChannelList($accesskey,$platform = ''){
		$request['accesskey'] = $accesskey;
		$request['filter'] = array('platformids'=>array($platform));
        if (!empty($platform)) {
            $request['filter'] = array('platformids' => array($platform));
        }
		$request = json_encode($request);
        sleep(3);
		$data = self::curlPost(self::$channel_url,$request);
		return json_decode($data,true);
	}
	
    //生成留存数据20160504
    public  static function createTetention($accesskey,$gameinfo,$platform,$day,$channel=array(),$version=array(),$groupby='daily'){
            $calDate = array(1,3,7,14,30); //留存日期
            $dateStartArr = array();
            foreach ($calDate as $dateVal) {
                $dateStartArr[] = date("Y-m-d",strtotime($day)-$dateVal*86400); //开始日期
            }
            $search['filter']['platformids'] = array($platform);
            $search['accesskey'] = $accesskey;
            $search['metrics'] = array('day1retention','day3retention','day7retention','day14retention','day30retention','dauday1retention','dauday3retention','dauday7retention','dauday14retention','dauday30retention');
            $search['metrics'] = array('newuser','day1retention','day3retention','day7retention','day14retention','day30retention');
            $search['groupby']='daily';
            $search['groupby']='daily';
            if($groupby=='channel'){//当是按渠道分组时，多渠道信息
                $vv = '';
                $cv = '';
                foreach ($channel as $ckey=>$cv){
                    $search['filter']['channelids'] = array($ckey);
                    if(!empty($version)){
                        foreach ($version as $vkey=>$vv){
                            $search['filter']['versions'] = array($vkey);
                        }
                    }
                    foreach($dateStartArr as $start){
                        $search['filter']['start'] = $start;
                        $search['filter']['end'] = $start;
                        sleep(10);
                        $result = self::curlPost(self::$data_url,json_encode($search));
//                        var_dump($result);
                        $result = json_decode($result,true);
                        foreach($result['result'] as $data){
                            if($data['day1retention']||$data['day1retention']||$data['day3retention']||$data['day7retention']||$data['day14retention']||$data['day30retention']||$data['dauday1retention']||$data['day1retention']||$data['dauday3retention']||$data['dauday7retention']||$data['dauday14retention']||$data['dauday30retention']||$data['newuser']){
                                $sql = "replace into talkingdata_retention (day,appkey,game_name,channel_name,version,platformid,day1retention,day3retention,day7retention,day14retention,day30retention,dauday1retention,dauday3retention,dauday7retention,dauday14retention,dauday30retention,intime,newuser) values('{$start}','{$accesskey}','{}','{$cv}','{$vv}',$platform,'{$data['day1retention']}','{$data['day3retention']}','{$data['day7retention']}','{$data['day14retention']}','{$data['day30retention']}','{$data['dauday1retention']}','{$data['dauday3retention']}','{$data['dauday7retention']}','{$data['dauday14retention']}','{$data['dauday30retention']}',now(),{$data['newuser']})";
                                echo $sql;
                                DB::insert($sql);
                            }
                        }
                    }
                }
            }else{
                $vv = '';
                $cv = '';
                if(!empty($channel)){
                        foreach ($channel as $ckey=>$cv){
                                $search['filter']['channelids'] = array($ckey);
                        }

                }
                if(!empty($version)){
                    foreach ($version as $vkey=>$vv){
                            $search['filter']['versions'] = array($vkey);
                    }
                }
                foreach($dateStartArr as $start){
                    $search['filter']['start'] = $start;
                    $search['filter']['end'] = $start;
//                    var_dump(date('Y-m-d H:i:s').":".json_encode($search));
                    sleep(10);
                    $result = self::curlPost(self::$data_url,json_encode($search));
//                    var_dump(date('Y-m-d H:i:s').":".$result);
                    $result = json_decode($result,true);
                    foreach($result['result'] as $data){
                        if($data['day1retention']||$data['day1retention']||$data['day3retention']||$data['day7retention']||$data['day14retention']||$data['day30retention']||$data['newuser']){
                        $day1newuser = round($data['day1retention']*$data['newuser'])?round($data['day1retention']*$data['newuser']):0;
                        $day3newuser = round($data['day3retention']*$data['newuser'])?round($data['day3retention']*$data['newuser']):0;
                        $day7newuser = round($data['day7retention']*$data['newuser'])?round($data['day7retention']*$data['newuser']):0;
                        $day14newuser = round($data['day14retention']*$data['newuser'])?round($data['day14retention']*$data['newuser']):0;
                        $day30newuser = round($data['day30retention']*$data['newuser'])?round($data['day30retention']*$data['newuser']):0;
                        $appid  = $gameinfo[$accesskey];
                        $sql = "replace into talkingdata_retention (day,appkey,appid,channel_name,version,platformid,day1retention,day3retention,day7retention,day14retention,day30retention,intime,newuser,day1newuser,day3newuser,day7newuser,day14newuser,day30newuser) values('{$start}','{$accesskey}','{$appid}','{$cv}','{$vv}',$platform,'{$data['day1retention']}','{$data['day3retention']}','{$data['day7retention']}','{$data['day14retention']}','{$data['day30retention']}',now(),{$data['newuser']},{$day1newuser},{$day3newuser},{$day7newuser},{$day14newuser},{$day30newuser})";
                        echo date('Y-m-d H:i:s').$sql.PHP_EOL;
                        if(!DB::insert($sql)){
                            echo  date('Y-m-d H:i:s')."_sql_error".PHP_EOL;
                        }
                    }
                }
            }
        }
    }

    //生成留存数据20160504
    public  static function foreignCreateTetention($accesskey,$gameinfo,$platform,$day,$channel=array(),$version=array(),$groupby='daily'){
        $calDate = array(1,3,7,14,30); //留存日期
        $dateStartArr = array();
        foreach ($calDate as $dateVal) {
            $dateStartArr[] = date("Y-m-d",strtotime($day)-$dateVal*86400); //开始日期
        }
        $search['filter']['platformids'] = array($platform);
        $search['accesskey'] = $accesskey;
        $search['metrics'] = array('day1retention','day3retention','day7retention','day14retention','day30retention','dauday1retention','dauday3retention','dauday7retention','dauday14retention','dauday30retention');
        $search['metrics'] = array('newuser','day1retention','day3retention','day7retention','day14retention','day30retention');
        $search['groupby']='daily';
        $search['groupby']='daily';
        if($groupby=='channel'){//当是按渠道分组时，多渠道信息
            $vv = '';
            $cv = '';
            foreach ($channel as $ckey=>$cv){
                $search['filter']['channelids'] = array($ckey);
                if(!empty($version)){
                    foreach ($version as $vkey=>$vv){
                        $search['filter']['versions'] = array($vkey);
                    }
                }
                foreach($dateStartArr as $start){
                    $search['filter']['start'] = $start;
                    $search['filter']['end'] = $start;
                    sleep(3);
                    $result = self::curlPost(self::$data_url,json_encode($search));
//                        var_dump($result);
                    $result = json_decode($result,true);
                    if ($result['status'] != 200) {
                        self::saveErrorLog($result['message']);
                        return;
                    }

                    if (isset($result['result']) && $result['result']){
                        foreach($result['result'] as $data){
                            if($data['day1retention']||$data['day1retention']||$data['day3retention']||$data['day7retention']||$data['day14retention']||$data['day30retention']||$data['dauday1retention']||$data['day1retention']||$data['dauday3retention']||$data['dauday7retention']||$data['dauday14retention']||$data['dauday30retention']||$data['newuser']){
                                $sql = "replace into talkingdata_foreign_retention (day,appkey,game_name,channel_name,version,platformid,day1retention,day3retention,day7retention,day14retention,day30retention,dauday1retention,dauday3retention,dauday7retention,dauday14retention,dauday30retention,intime,newuser) values('{$start}','{$accesskey}','{}','{$cv}','{$vv}',$platform,'{$data['day1retention']}','{$data['day3retention']}','{$data['day7retention']}','{$data['day14retention']}','{$data['day30retention']}','{$data['dauday1retention']}','{$data['dauday3retention']}','{$data['dauday7retention']}','{$data['dauday14retention']}','{$data['dauday30retention']}',now(),{$data['newuser']})";
                                echo $sql;
                                DB::insert($sql);
                            }
                        }
                    }
                }
            }
        }else{
            $vv = '';
            $cv = '';
            if(!empty($channel)){
                foreach ($channel as $ckey=>$cv){
                    $search['filter']['channelids'] = array($ckey);
                }

            }
            if(!empty($version)){
                foreach ($version as $vkey=>$vv){
                    $search['filter']['versions'] = array($vkey);
                }
            }
            foreach($dateStartArr as $start){
                $search['filter']['start'] = $start;
                $search['filter']['end'] = $start;
//                    var_dump(date('Y-m-d H:i:s').":".json_encode($search));
                sleep(3);
                $result = self::curlPost(self::$data_url,json_encode($search));
//                    var_dump(date('Y-m-d H:i:s').":".$result);
                $result = json_decode($result,true);
                if ($result['status'] != 200) {
                    self::saveErrorLog($result['message']);
                    return;
                }

                if (isset($result['result']) && $result['result']){
                    foreach($result['result'] as $data){
                        if($data['day1retention']||$data['day1retention']||$data['day3retention']||$data['day7retention']||$data['day14retention']||$data['day30retention']||$data['newuser']){
                            $day1newuser = round($data['day1retention']*$data['newuser'])?round($data['day1retention']*$data['newuser']):0;
                            $day3newuser = round($data['day3retention']*$data['newuser'])?round($data['day3retention']*$data['newuser']):0;
                            $day7newuser = round($data['day7retention']*$data['newuser'])?round($data['day7retention']*$data['newuser']):0;
                            $day14newuser = round($data['day14retention']*$data['newuser'])?round($data['day14retention']*$data['newuser']):0;
                            $day30newuser = round($data['day30retention']*$data['newuser'])?round($data['day30retention']*$data['newuser']):0;
                            $appid  = $gameinfo[$accesskey];
                            $sql = "replace into talkingdata_foreign_retention (day,appkey,appid,channel_name,version,platformid,day1retention,day3retention,day7retention,day14retention,day30retention,intime,newuser,day1newuser,day3newuser,day7newuser,day14newuser,day30newuser) values('{$start}','{$accesskey}','{$appid}','{$cv}','{$vv}',$platform,'{$data['day1retention']}','{$data['day3retention']}','{$data['day7retention']}','{$data['day14retention']}','{$data['day30retention']}',now(),{$data['newuser']},{$day1newuser},{$day3newuser},{$day7newuser},{$day14newuser},{$day30newuser})";
                            echo date('Y-m-d H:i:s').$sql.PHP_EOL;
                            if(!DB::insert($sql)){
                                echo  date('Y-m-d H:i:s')."_sql_error".PHP_EOL;
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * 获取用户留存
     * @param $accesskey
     * @param $gameinfo
     * @param $platform
     * @param $day
     * @param array $channel
     * @param array $version
     * @param $groupby
     */
    public static function getKeepUser($gameinfo, $day, $groupby = 'daily', $table = 'talkingdata_china_keepuser', $channel = array(), $version = array())
    {

        $platform_id = $gameinfo['platform_id'];

        if (empty($groupby)) {
            $groupby = 'daily';
        }
        // 获取渠道信息
        $channelInfo = self::getChannelList($gameinfo['app_key'], $gameinfo['platform_id']);

        if ($channelInfo['status'] != 200) {
            self::saveErrorLog($channelInfo['message']);
            return;
        }
        $channels = array();
        foreach ($channelInfo['result'] as $ver) {
            if (!empty($channel)) {
                if (!in_array($ver['channelname'], $channel)) {
                    continue;
                }
            }
            $channels[] = $ver;
        }

        // 先算出每日新增的数据
        foreach ($channels as $kk => $d) {
            $channelname = $d['channelname'];
            $channelid = $d['channelid'];
            $search['filter']['channelids'] = array($channelid);
            $search['accesskey'] = $gameinfo['app_key'];

            $search['groupby'] = 'daily';

            $search['metrics'] = array('newuser', 'activeuser');
            if ($table == "talkingdata_china_keepuser") {
                $search['metrics'] = array('newuser', 'activeuser', 'session', 'sessionlength');
            }

            $search['filter']['start'] = $day;
            $search['filter']['end'] = $day;
            if (!empty($platform_id)) {
                $search['filter']['platformids'] = array($platform_id);
            }

            $jstr = json_encode($search);
            sleep(3);
            $new_result = self::curlPost(self::$data_url, $jstr);
            $new_result = json_decode($new_result, true);
            if ($new_result['status'] != 200) {
                self::saveErrorLog($new_result['message']);
                return;
            }

            $res = $new_result['result'][0];
            if (empty($res['activeuser']) && empty($res['newuser'])) {
                continue;
            }

            $newuser = $res['newuser'];
            $activeuser = $res['activeuser'];

            if ($table == "talkingdata_china_keepuser") {
                $session = $res['session'];
                $sessionlength = $res['sessionlength'];
                $sql = "INSERT INTO $table (tjdate,app_id,app_key,app_name,channel_id,channel_name,platform_id,session,sessionlength,newuser,activeuser) VALUES('$day','{$gameinfo['app_id']}','{$gameinfo['app_key']}','{$gameinfo['app_name']}','$channelid','$channelname','$platform_id','$session','$sessionlength','$newuser','$activeuser') ON DUPLICATE KEY UPDATE newuser='$newuser',activeuser='$activeuser' ,session='$session',sessionlength='$sessionlength'";

            }
            var_dump($sql);
            DB::insert($sql);
        }

        // 在算留存率
        $calDate = array(1, 3, 7, 14, 30); //留存日期
        foreach ($calDate as $dateVal) {
            $tjdate = date("Y-m-d", strtotime($day) - $dateVal * 86400); //开始日期
            self::getTjdate($gameinfo, $tjdate, $dateVal, $groupby, $table);
        }

        var_dump("{$gameinfo['app_name']} is ok");
        self::saveLog('TalkingData', date('Y-m-d H:i:s') . "：$table($day) {$gameinfo['app_id']}-{$gameinfo['app_name']} is ok", 'keepuser');
    }

    //生成留存数据20160504
    public static function getTjdate($gameinfo, $tjdate, $dateVal, $groupby = 'daily', $table = 'talkingdata_china_keepuser')
    {
        $platform_id = $gameinfo['platform_id'];
        $accesskey = $gameinfo['app_key'];
        $app_id = $gameinfo['app_id'];


        $where = "tjdate='$tjdate' AND app_id='$app_id' AND platform_id='$platform_id' ";

        if ($table == 'talkingdata_china_keepuser') {
            $sql = "SELECT channel_id FROM $table WHERE $where ";
        } else {
            $sql = "SELECT channel_id,appversion FROM $table WHERE $where ";
        }

        $result = DB::select($sql);
        $result = Service::data($result);

        if (empty($result)) {
            return;
        }

        foreach ($result as $row){
            $sql_res[] = $row;
        }

        if (empty($sql_res)) {
            return;
        }

        $metrics_day = "day{$dateVal}retention";
        $metrics_dau = "dauday{$dateVal}retention";
        $search = array();
        $search['metrics'] = array('newuser', 'activeuser', $metrics_day, $metrics_dau);
        $search['accesskey'] = $accesskey;
        $search['groupby'] = 'daily';
        $search['filter']['start'] = $tjdate;
        $search['filter']['end'] = $tjdate;
        foreach ($sql_res as $v) {
            $search['filter']['channelids'] = array($v['channel_id']);
            $new_where = $where;
            if ($groupby == 'appversion') {
                $search['filter']['versions'] = array($v['appversion']);
                $new_where .= " AND appversion='{$v['appversion']}'";
            }

            if (!empty($platform_id)) {
                $search['filter']['platformids'] = array($platform_id);
            }
            $jstr = json_encode($search);
//            var_dump($jstr);
            sleep(3);
            $new_result = self::curlPost(self::$data_url, $jstr);
            $new_result = json_decode($new_result, true);
//            var_dump($new_result);
            if ($new_result['status'] != 200) {
                self::saveErrorLog($new_result['message']);
                return;
            }
            $res = $new_result['result'][0];
            if (empty($res['activeuser']) && empty($res['newuser'])) {
                continue;
            }

            $newuser = $res['newuser'] * $res[$metrics_day];
            $activeuser = $res['activeuser'] * $res[$metrics_dau];

            $sql = "UPDATE $table SET $metrics_day='{$res[$metrics_day]}',$metrics_dau='{$res[$metrics_dau]}',day{$dateVal}num='$newuser',dauday{$dateVal}num='$activeuser' WHERE channel_id='{$v['channel_id']}' AND $new_where";
            var_dump($sql);
            DB::update($sql);
        }
    }

    /**
     * @desc 获取新增和活跃
     * @param string $accesskey
     * @param array $gameinfo
     * @param int $platform
     * @param string $day
     * @param array $channel
     * @param array $version
     * @param string $groupby
     */
    public static function keep_createData($accesskey, $gameinfo, $platform, $day, $channel = array(), $version = array(), $groupby)
    {
        $groupby = $groupby ? $groupby : 'daily';

        $search['filter']['start'] = $day;
        $search['filter']['end'] = $day;
        $search['filter']['platformids'] = array($platform);
        $search['accesskey'] = $accesskey;
        $search['groupby'] = $groupby;
        $search['metrics'] = array('activeuser', 'session', 'avgsessionlength');
        self::saveLog('TalkingData', date('Y-m-d H:i:s') . "：" . json_encode($search) , 'search_active');
        //获取活跃
        sleep(3);
        $active_result = self::curlPost(self::$data_url, json_encode($search));
        $active_result = json_decode($active_result, true);
        if ($active_result['status'] != 200) {
            TdDataFunction::saveErrorLog($active_result['message']);
            return;
        }
        //获取新增
        unset($search['metrics']);
        $search['metrics'] = array('newuser');
        sleep(3);
        $new_result = self::curlPost(self::$data_url, json_encode($search));
        $new_result = json_decode($new_result, true);
        if ($new_result['status'] != 200) {
            TdDataFunction::saveErrorLog($new_result['message']);
            return;
        }
        self::saveLog('TalkingData', date('Y-m-d H:i:s') . "：" . json_encode($search), 'search_newuser');
        //根据分组不同获取不同维度对应的数据
        $channelInfo = self::getChannelList($accesskey, $platform);
        if ($channelInfo['status'] != 200) {
            TdDataFunction::saveErrorLog($channelInfo['message']);
            return;
        }
        foreach ($channelInfo['result'] as $d) {
            $c[$d['channelid']] = $d['channelname'];
        }
        foreach ($active_result['result'] as $data) {
            if ($data['activeuser'])
                $activeData[$c[$data['channelid']]] = $data;
        }
        foreach ($new_result['result'] as $new) {
            if ($new['newuser'])
                $newData[$c[$new['channelid']]] = $new;
        }
        self::keep_active_new($gameinfo, $platform, $day, $groupby, $activeData, $newData);

    }

    /**
     * @desc 将新增和活跃数据存入数据库
     * @param array $gameinfo
     * @param int $platform
     * @param date $day
     * @param string $groupby
     * @param array $activeData
     * @param array $newData
     * @param string $version
     * @param string $channel
     */
    public static function keep_active_new($gameinfo, $platform, $day, $groupby, $activeData, $newData)
    {
        foreach ($gameinfo as $appid => $appname) {
            foreach ($activeData as $c => $data) {
                $active_sql = "insert into ".self::$gntjtable_name." (day,appid,game_name,channel_name,active_user,`session`,sessionlength,platformid) values('{$day}','{$appid}','{$appname}','{$c}','{$data['activeuser']}','{$data['session']}','{$data['avgsessionlength']}',$platform)";
                var_dump($active_sql);
                DB::insert($active_sql);
            }
            foreach ($newData as $cc => $new) {
                $new_sql = "update ".self::$gntjtable_name." set new_user='{$new['newuser']}' where day='{$day}' and channel_name='{$cc}' and platformid=$platform and appid='{$appid}'";
                var_dump($new_sql);
                DB::update($new_sql);
            }
        }
    }

    // todo 原代码
    public static function getBaseDataGroup($gameinfo, $day)
    {
        $table = "talkingdata_base_data";


        $platform_id = $gameinfo['platform_id'];
        $channel_type = $gameinfo['channel_type'];
        $version_type = $gameinfo['version_type'];

        $country_type = $gameinfo['country_type'];


        if ($country_type == 0) {
            self::getBaseData($gameinfo, $day);
            return;
        }


        $geo_kev = array();
        if ($country_type == '1') {
            $tmp = self::getChinaList($gameinfo['app_key']);
            foreach ($tmp['result'] as $t) {
                $geo_kev[$t['geochina']] = $t['name'];
            }
        } elseif ($country_type == '2') {
            $tmp = self::getCountryList($gameinfo['app_key']);
            foreach ($tmp['result'] as $t) {
                $geo_kev[$t['geocountry']] = $t['name'];
            }
        }


        //判断 version_type
        $versions = array();
        if ($version_type == '1') {
            $versions[] = array(
                'versionname' => $gameinfo['version_name'],
                'appversion' => $gameinfo['version_id'],
            );
        } else {
            $versionInfo = self::getVersionList($gameinfo['app_key'], $platform_id);
            if (empty($versionInfo['result'])) {
                return;
            }
            foreach ($versionInfo['result'] as $ver) {
                $versions[] = $ver;
            }
        }
//        var_dump($versions);


        //判断channel_type
        $channels = array();
        if ($channel_type == '1') {
            $channels[] = array(
                'channelid' => $gameinfo['channel_id'],
                'channelname' => $gameinfo['channel_name'],
            );
        } else {
            $channelInfo = self::getChannelList($gameinfo['app_key'], $gameinfo['platform_id'], $platform_id);
            if (empty($channelInfo['result'])) {
                return;
            }
            foreach ($channelInfo['result'] as $ver) {
                $channels[] = $ver;
            }
        }

//        var_dump($channels);

        $c_count = count($channels);
        $channels_group = $tmp = $channels_kv = array();
        for ($i = 0; $i < $c_count; $i++) {
            $channel_id = $channels[$i]['channelid'];
            $channel_name = $channels[$i]['channelname'];

            $channels_kv[$channel_id] = $channel_name;
            $tmp[] = $channel_id;
            if (count($tmp) == 100) {
                $channels_group[] = $tmp;
                $tmp = array();
            }
        }

        if (!empty($tmp)) {
            $channels_group[] = $tmp;
        }

        foreach ($channels_group as $channel_ids) {
            $search['filter']['channelids'] = $channel_ids;
            $search['accesskey'] = $gameinfo['app_key'];


            if ($country_type == '1') {
                $search['groupby'] = 'channelid,geochina';
            } elseif ($country_type == '2') {
                $search['groupby'] = 'channelid,geocountry';
            }


            $search['metrics'] = array('activeuser');

            $search['filter']['start'] = $day;
            $search['filter']['end'] = $day;
            $search['filter']['platformids'] = array($platform_id);

//            var_dump($channel_ids);
            foreach ($versions as $ver) {
                $version_id = $ver['appversion'];
                $version_name = $ver['versionname'];
                $search['filter']['versions'] = array($version_id);


                $jstr = json_encode($search);
//                var_dump($jstr);
                sleep(3);
                $new_result_json = self::curlPost(self::$data_url, $jstr);

                $new_result = json_decode($new_result_json, true);
//                var_dump($new_result);
                if (empty($new_result['result'])) {
                    var_dump('empty_result', $jstr, $new_result_json);
                    continue;
                }

                $result = $new_result['result'];

                foreach ($result as $res) {

                    if (empty($res['activeuser'])) {
                        continue;
                    }

                    $country_id = '';

                    if ($country_type == '1') {
                        $country_id = $res['geochina'];
                    } elseif ($country_type == '2') {
                        $country_id = $res['geocountry'];
                    }

                    @$activeuser = $res['activeuser'];
                    @$channelid = $res['channelid'];
                    @$country_name = $geo_kev[$country_id];

                    $sql = "INSERT INTO $table (tjdate,app_id,app_name,channel_id,channel_name,version_id,version_name,platform_id,country_type,country_id,country_name,activeuser) VALUES('$day','{$gameinfo['app_id']}','{$gameinfo['app_name']}','$channelid','{$channels_kv[$channelid]}','$version_id','$version_name','$platform_id','$country_type','$country_id','$country_name','$activeuser') ON DUPLICATE KEY UPDATE activeuser='$activeuser' ";
                    var_dump($sql);
                    DB::insert($sql);
                }

            }
        }

    }

    // todo 新代码
    public static function getNewBaseDataGroup($gameinfo, $day,$channels,$versions,$groupby,$geo_kev,$channelname = '')
    {
        $table = "talkingdata_china_user";
        $platform_id = $gameinfo['platform_id'];

        $c_count = count($channels);
        $channels_group = $tmp = $channels_kv = array();
        for ($i = 0; $i < $c_count; $i++) {
            $channel_id = $channels[$i]['channelid'];
            $channel_name = $channels[$i]['channelname'];

            $channels_kv[$channel_id] = $channel_name;
            $tmp[] = $channel_id;
            if (count($tmp) == 100) {
                $channels_group[] = $tmp;
                $tmp = array();
            }
        }

        if (!empty($tmp)) {
            $channels_group[] = $tmp;
        }

        if ($channels_group){
            foreach ($channels_group as $channel_ids) {
                $search['filter']['channelids'] = $channel_ids;
                $search['accesskey'] = $gameinfo['app_key'];

                $search['groupby'] = $groupby;

                $search['metrics'] = array('activeuser');

                $search['filter']['start'] = $day;
                $search['filter']['end'] = $day;
                $search['filter']['platformids'] = array($platform_id);

                if ($versions) {

                    foreach ($versions as $ver) {
                        $version_id = $ver['appversion'];
                        $version_name = $ver['versionname'];
                        $search['filter']['versions'] = array($version_id);

                        $jstr = json_encode($search);
                        sleep(3);
                        var_dump(self::$data_url, $jstr);
                        $new_result_json = self::curlPost(self::$data_url, $jstr);
                        var_dump($new_result_json);
                        $new_result = json_decode($new_result_json, true);

                        if ($new_result['status'] != 200) {
                            self::saveErrorLog($new_result['message']);
                            return;
                        }

                        $result = $new_result['result'];

                        foreach ($result as $res) {

                            if (empty($res['activeuser'])) {
                                continue;
                            }

                            $country_id = isset($res['geochina']) ? $res['geochina'] : '';

                            @$activeuser = $res['activeuser'];
                            @$channelid = $res['channelid'];
                            @$country_name = $geo_kev[$country_id] ? $geo_kev[$country_id] : '未知省份';

                            $sql = "INSERT INTO $table (tjdate,app_id,app_name,channel_id,channel_name,version_id,version_name,platform_id,country_type,country_id,country_name,activeuser) VALUES('$day','{$gameinfo['app_id']}','{$gameinfo['app_name']}','$channelid','{$channels_kv[$channelid]}','$version_id','$version_name','$platform_id',1,'$country_id','$country_name','$activeuser') ON DUPLICATE KEY UPDATE activeuser = '$activeuser'";
                            var_dump($sql);
                            DB::insert($sql);
                        }

                    }
                }
            }
        }
    }

    public static function getBaseDataGroup_orther($gameinfo, $day,$channels,$versions,$groupby,$geo_kev,$channelname = '')
    {
        $table = "talkingdata_china_user";
        $platform_id = $gameinfo['platform_id'];

        if ($channels){
            foreach ($channels as $c) {
                $channel_name = $c['channelname'];
                $channel_id = $c['channelid'];
                $search['filter']['channelids'] = array($channel_id);
                $search['accesskey'] = $gameinfo['app_key'];

                $search['groupby'] = 'geochina';

                $search['metrics'] = array('newuser', 'session', 'sessionlength');

                $search['filter']['start'] = $day;
                $search['filter']['end'] = $day;
                $search['filter']['platformids'] = array($platform_id);

                if ($versions){
                    foreach ($versions as $ver) {
                        $version_id = $ver['appversion'];
                        $version_name = $ver['versionname'];
                        $search['filter']['versions'] = array($version_id);

                        $jstr = json_encode($search);
                        sleep(3);
                        $new_result_json = self::curlPost(self::$data_url, $jstr);
//                        var_dump($new_result_json);
                        $new_result = json_decode($new_result_json, true);
                        if ($new_result['status'] != 200) {
                            self::saveErrorLog($new_result['message']);
                            return;
                        }

                        $result = $new_result['result'];

                        foreach ($result as $res) {

                            if (empty($res['newuser']) && empty($res['session'])) {
                                continue;
                            }

                            $country_name = $res['Name'];

                            $country_id = isset($res['geochina']) ? $res['geochina'] : '';

                            @$newuser = $res['newuser'];
                            @$session = $res['session'];
                            @$sessionlength = $res['sessionlength'];

                            $sql = "INSERT INTO $table (tjdate,app_id,app_name,channel_id,channel_name,version_id,version_name,platform_id,
country_type,country_id,country_name,newuser,session,sessionlength)
VALUES('$day','{$gameinfo['app_id']}','{$gameinfo['app_name']}','$channel_id','$channel_name','$version_id','$version_name','$platform_id',
1,'$country_id','$country_name','$newuser','$session','$sessionlength')
                    ON DUPLICATE KEY UPDATE newuser='$newuser',session='$session',sessionlength='$sessionlength' ";
                            var_dump($sql);
                            DB::insert($sql);
                        }
                    }
                }
            }
        }

    }

    public static function getChinaList($accesskey)
    {
        $request['accesskey'] = $accesskey;
        sleep(3);
        $request = json_encode($request);
        $data_json = self::curlPost('https://api.talkingdata.com/metrics/app/v1/provincelist', $request);
        $data = json_decode($data_json, true);
        if ($data['status'] != 200) {
            self::saveErrorLog($data['message']);
            die;
        }
        return $data;
    }

    public static function getCountryList($accesskey)
    {
        $request['accesskey'] = $accesskey;
        sleep(3);
        $request = json_encode($request);
        $data_json = self::curlPost('https://api.talkingdata.com/metrics/app/v1/countrylist', $request);

        $data = json_decode($data_json, true);
        if ($data['status'] != 200) {
            self::saveErrorLog($data['message']);
            die;
        }
        return $data;
    }

    // 保存日志
    private static function saveLog($platform_name = '未知', $message = '', $log_name = ''){

        $fileName = date('Y-m-d',time());
        $dir = './storage/tjDataLogs';

        if (!is_dir($dir)) {
            mkdir($dir,0777,true);
        }
        $logFilename = $dir.'/'.$platform_name.'_search'.'.log';
        if ($log_name){
            $logFilename = $dir.'/'.$platform_name.'_'.$log_name.'.log';
        }
        //生成日志
        file_put_contents( $logFilename,$message . "\n\n",FILE_APPEND);
    }


    public static function saveErrorLog($error_msg){
        $error_msg =  'TalkingData统计平台获取数据失败取数失败,错误信息:'.$error_msg;
        DataImportImp::saveDataErrorLog(1,'ptj02','TalkingData',1,$error_msg);

        $error_msg_arr = [];
        $error_msg_arr[] = $error_msg;
        CommonFunction::sendMail($error_msg_arr,'TalkingData取数error');

    }
}


