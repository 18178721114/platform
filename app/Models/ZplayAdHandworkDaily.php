<?php
namespace App\Models;

use Illuminate\Support\Facades\DB;
use Illuminate\Database\Eloquent\Model;

class ZplayAdHandworkDaily extends Model{

    /**
     * 与模型关联的数据表
     *
     * @var string
     */
    protected $table = 'zplay_ad_handwork_daily';


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
        'date', 'platform_id', 'app_id', 'platform_app_id', 'platform_app_name', 'ad_id', 'ad_name', 'create_time', 'update_time','income','remark','os_id','biding_income'
    ];

    /**
     * 关闭自动维护updated_at、created_at字段
     * @var boolean
     */
    public $timestamps = false;


}
