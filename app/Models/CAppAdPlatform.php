<?php
namespace App\Models;

use Illuminate\Support\Facades\DB;
use Illuminate\Database\Eloquent\Model;

class CAppAdPlatform extends Model{

    /**
     * 与模型关联的数据表
     *
     * @var string
     */
    protected $table = 'c_app_ad_platform';


    /**
     * 模型的主键id
     *
     * @var int
     */
    protected $primaryKey = 'id';


    /**
     * The attributes that are mass assignable.
     *
     * @var array
     */
    protected $fillable = [
        'app_id', 'platform_id', 'platform_app_id','platform_app_name', 'platform_account', 'api_key', 'currency', 'publisher_id', 'sdk_key', 'app_key', 'bundle_id', 'access_key', 'privkey_pem', 'app_signature', 'user_id', 'user_signature', 'system_user_token', 'agid', 'gdt_app_id', 'instance_id', 'secret_key', 'skey', 'reward_id', 'account_id', 'create_time', 'update_time','token'
    ];

    /**
     * 关闭自动维护updated_at、created_at字段
     * @var boolean
     */
    public $timestamps = false;


}
