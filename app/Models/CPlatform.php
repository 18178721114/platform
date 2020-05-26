<?php
namespace App\Models;

use Illuminate\Support\Facades\DB;
use Illuminate\Database\Eloquent\Model;

class CPlatform extends Model{

    /**
     * 与模型关联的数据表
     *
     * @var string
     */
    protected $table = 'c_platform';


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
        'platform_id', 'platform_name', 'customer_id', 'company_id', 'platform_account','alise', 'platform_data_account', 'currency_type_id', 'business_manager_id', 'platform_type_id','bad_account_rate', 'create_time', 'update_time'
    ];

    /**
     * 关闭自动维护updated_at、created_at字段
     * @var boolean
     */
    public $timestamps = false;


}
