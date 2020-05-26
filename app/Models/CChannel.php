<?php
namespace App\Models;

use Illuminate\Support\Facades\DB;
use Illuminate\Database\Eloquent\Model;

class CChannel extends Model{

    /**
     * 与模型关联的数据表
     *
     * @var string
     */
    protected $table = 'c_channel';


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
        'channel_id','td_channel_id','channel_name', 'customer_id', 'company_id', 'platform_id', 'channel_type', 'channel_region', 'currency_type_id', 'business_manager_id', 'create_time', 'update_time','alise', 'is_jailbreak'
    ];

    /**
     * 关闭自动维护updated_at、created_at字段
     * @var boolean
     */
    public $timestamps = false;


}
