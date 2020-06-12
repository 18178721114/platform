<?php
namespace App\Models;

use Illuminate\Support\Facades\DB;
use Illuminate\Database\Eloquent\Model;

class User extends Model{

    /**
     * 与模型关联的数据表
     *
     * @var string
     */
    protected $table = 'user';


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
        'name', 'user_account','role_id','create_time','update_time','type','parent_uid','function_permission','company_id','developer_id','account_type','dev_language'
    ];

    /**
     * 关闭自动维护updated_at、created_at字段
     * @var boolean
     */
    public $timestamps = false;


}
