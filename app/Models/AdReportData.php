<?php
namespace App\Models;

use Illuminate\Support\Facades\DB;
use Illuminate\Database\Eloquent\Model;

class AdReportData extends Model{

    /**
     * 与模型关联的数据表
     *
     * @var string
     */
    protected $table = 'ad_report_data';

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
        'id', 'date', 'app_id', 'ad_unit_id', 'play_start', 'play_finish', 'imp', 'click', 'request', 'income', 'country', 'currency'
    ];

    /**
     * 关闭自动维护updated_at、created_at字段
     * @var boolean
     */
    public $timestamps = false;


}
