<?php
$time = date('t',time());
var_dump(empty(0.00));
die;
echo strpos("You love php, I love php too!","f");die;
trait Drive
{
    public $carName = 'BMW';

    public function driving()
    {
        echo "driving {$this->carName}\n";
    }
}
class Person {
    public function age() {
        echo "i am 18 years old\n";
    }
}
class Student extends Person {
    use Drive;
    public function study() {
        echo "Learn to drive \n";
    }
}
$student = new Student();
$student->study();
$student->age();
$student->driving();


die;

ob_start(); // 开始php缓存，写在最前面
echo 1;
echo "<hr/>";
// 获取php缓存区的内容
$buffer = ob_get_contents();
var_dump($buffer);
//echo $buffer;
die;
// echo isset(null),isset(false), empty(null) ,empty(false);die;

class factory{
    public function make($a){
        $b = new $a();
        $b->make();
    }
}
class fly{
public function make(){
echo '11111111飞机';
}
}

addcslashes();
$a = new factory();
$a->make('fly');
die;
class single{
    static public  $instance;
    public function __construct()
    {
        $n =1;
    }
    static  public  function  single(){
        if(!(self::$instance instanceof self)){
            single::$n = new self();
        }
        return single::$n;
    }
}


$array = [1,5,8,9,7,4,3,4,6];
function qucik($arr){
    if (count($arr) <= 1) {
        return $arr;
    }
    $num  = $arr[0];
    $left_arr= [];
    $right_arr = [];
    for ($i=1; $i < count($arr); $i++) {

        if ($num < $arr[$i]) {

            // �����м�ֵ
            $right_arr[] = $arr[$i];
        } else {

            // С���м�ֵ
            $left_arr[] = $arr[$i];
        }
    }
	$left  = qucik($left_arr);
	$right  = qucik($right_arr);
	return  array_merge($left,array($arr[0]),$right);

}
$arr = qucik($array);
var_dump($arr);die;

$array = [1,5,8,9,7,4,3,4,6];
$count =count($array);
for($i= 0 ;$i <$count;$i++){
    for ($j = $i+1;$j<$count ;$j++){
        if($array[$i]> $array[$j]){
            $tem = $array[$j];
            $array[$j] =$array[$i];
            $array[$i] =$tem;
	  }
    }
}
var_dump($array);
die;
echo $_SERVER['REMOTE_ADDR'];die;
$person=['name'=>'�뻶','age'=>19];
var_dump(xdebug_debug_zval('person'));die;
$a =8 ;
$b =8;
if($a =5 || $b =7){
    $a++;
    $b++;
}
var_dump($a,$b);die;

$str = 'abc def ghi';//ghi def abc
$len = strlen($str);
$pos = strpos($str,' ');
//echo $sub = substr($str,-11,3); die;
$strRev = '';
for($i=$pos;$i<=$len;$i++){

    $substr = substr($str,-$i,$pos);
    if( strpos($substr, ' ') ||(strpos($substr, ' ') === 0)){

    }else{
        $strRev.= $substr.' ';
    }


}
echo $strRev;die;
/*
//$a=[0,1,2,3]; $b=[1,2,3,4,5]; $a+$b; var_dump(array_merge($a,$b));die;
//$array = [];
//
$array[0]['name']='����';
$array[0]['age']='18';

$array[1]['name']='����';
$array[1]['age']='20';
//
//$arr_sort = array_column($array,'age');
//$a = array_multisort($array,SORT_ASC,$arr_sort);
//var_dump($array);
//die;

function arr_sort($array,$sort_desc,$name){
    $arr_sort = [];
    $arr_info = [];
    foreach ($array as $k => $v){
        $arr_sort[$k] =$v[$name];
    }
    if($sort_desc == 'asc') {
        arsort($arr_sort);
    }else{
        asort($arr_sort);
    }
    var_dump($arr_sort);
    foreach ($arr_sort as $k => $v ){
        $arr_info[$k]=$array[$k];
    }

    return $arr_info;
}
$a = arr_sort($array,'asc','age');
var_dump($a);

*/
$array = [1,2,3,7];
$array2 = [3,5,9];
var_dump($array+$array2);
