<?php
namespace App\Models;

use Illuminate\Support\Facades\DB;
use Illuminate\Database\Eloquent\Model;

class AD extends Model{

    /**
     * 与模型关联的数据表
     *
     * @var string
     */
    protected $table = 'zplay_ad_report_daily';


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
        'date', 'app_id','version','channel_id','country_id','data_platform_id','data_account','platform_id','ad_type','statistics','platform_app_id','platform_app_name','ad_unit_id','ad_unit_name','round','all_request','success_requests','fail_requests','impression_port','impression_begin','impression','click','download','activate','reward','earning','earning_exc','flow_type','remark','create_time','update_time'
    ];

    /**
     * 关闭自动维护updated_at、created_at字段
     * @var boolean
     */
    public $timestamps = false;


}
