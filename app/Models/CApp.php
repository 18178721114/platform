<?php
namespace App\Models;

use Illuminate\Support\Facades\DB;
use Illuminate\Database\Eloquent\Model;

class CApp extends Model{

    /**
     * 与模型关联的数据表
     *
     * @var string
     */
    protected $table = 'c_app';


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
        'app_id', 'app_name', 'app_full_name', 'developer_id', 'app_type_id', 'company_id', 'release_region_id', 'os_id', 'release_group', 'app_category_id', 'online_time', 'create_time', 'update_time','is_jailbreak','is_dev_show'
    ];

    /**
     * 关闭自动维护updated_at、created_at字段
     * @var boolean
     */
    public $timestamps = false;


}
