<?php
namespace App\Models;

use Illuminate\Support\Facades\DB;
use Illuminate\Database\Eloquent\Model;

class CAppBillingPoint extends Model{

    /**
     * 与模型关联的数据表
     *
     * @var string
     */
    protected $table = 'c_billing_point';


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
        'id', 'app_id', 'billing_point_name', 'billing_point_id', 'billing_point_price_usd', 'billing_point_price_cny',  'create_time', 'update_time',  'type', 'status', 'rebate_billing_point_price_usd',  'rebate_begin_time', 'rebate_end_time', 'rebate_billing_point_price_cny','currency_type'
    ];

    /**
     * 关闭自动维护updated_at、created_at字段
     * @var boolean
     */
    public $timestamps = false;


}
