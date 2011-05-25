<?php
/*
include 'ara_def.php';
echo ARA_TYPE_INT_SIZE."<br>";

$myvar = "mohab";
$length = strlen($myvar);
$var = pack("Ia*",$length, "mohab");
echo bin2hex($var)."<br>";
echo strlen($var)."<br>";
$uvar = unpack("Ival", $var);
var_dump($uvar);
echo $uvar['val']."<br>";
//echo $uvar['str']."<br>";

$var = pack("cccIa*", '5', '6', '7', 8, "mohab");
$offset = 3;
$varu = unpack("c{$offset}char/Iint",$var);
echo $varu['int']."<br>";

$x = array("string", $offset+ARA_CFG_TRY_CNT);
var_dump($x);
echo $x[0];
*/

/*
$binarydata = "\x04\x00\xa0\x00";
$array = unpack("cchars/nint", $binarydata);
var_dump($array);

echo "<br>";

$binarydata = "\x04\x00\xa0\x00";
$array = unpack("c2chars/nint", $binarydata);
var_dump($array);
*/

//$ns = array(
//    'sampleapp_0' => array('ip'=>'127.0.0.1', 'port' => 20100)
//);
//
//$k = array_keys($ns);
//echo $k[0];



include 'arakoon.php';

/*The nodes list*/
$nodes = array(
    'sampleapp_0' => array('ip'=>'127.0.0.1', 'port' => 20100)
);

$cfg = new ArakoonClientConfig ('sampleapp',$nodes);
$arakoon = new Arakoon($cfg);


/*
 * SET/GET
 */
$key = 'key1'; 
$value = 'value1';
$arakoon->set($key, $value);
$val = $arakoon->get($key);
if($value != $val){
    arakoonLog(__FUNCTION__, "SET/GET: Nah .... Wrong Value");
}else{
    arakoonLog(__FUNCTION__,"SET/GET: CLIENT TEST SUCCESS!!!") ;
}


/*
 * SEQUENCE/EXISTS
 */
$kseq1 = "KeySeq1"; $vseq1="ValueSeq1";
$kseq2 = $key;

$seq = new Sequence();
$seq->addSet($kseq1, $vseq1);
$seq->addDelete($kseq2);
$arakoon->sequence($seq);
$val = $arakoon->get($kseq1);
$bool = $arakoon->exists($kseq2);
if($vseq1 != $val && !$bool){
    arakoonLog(__FUNCTION__, "SEQUENCE: Nah .... Wrong Value");
}else{
    arakoonLog(__FUNCTION__,"SEQUENCE: CLIENT TEST SUCCESS!!!") ;
}


/*
 * RANGE
 */
$keys = array();
$values = array();
for ($i = 0; $i < 5; $i++)
{
    $keys[] = "k$i";
    $values[] = "value$i";
    $arakoon->set($keys[$i], $values[$i]);
}
$rkeys = $arakoon->range($keys[0], TRUE, $keys[4], TRUE);
if(count($rkeys)<=0){
    arakoonLog(__FUNCTION__, "RANGE: Nah .... Wrong Value");
}else{
    foreach ($rkeys as $rkey) {
        arakoonLog(__FUNCTION__, "RANGE KEY: $rkey");
    }
    arakoonLog(__FUNCTION__,"RANGE: CLIENT TEST SUCCESS!!!") ;
}

/*
 * RANGE ENTRIES
 */
$rkeys = array();
$rkeys = $arakoon->range_entries($keys[0], TRUE, $keys[4], TRUE);
var_dump($rkeys);
if(count($rkeys)<=0){
    arakoonLog(__FUNCTION__, "RANGE: Nah .... Wrong Value");
}else{
    foreach ($rkeys as $rkey => $rvalue) {
        arakoonLog(__FUNCTION__, "RANGE ENTRIES KEY, VALUE: $rvalue[0], $rvalue[1]");
    }
    arakoonLog(__FUNCTION__,"RANGE ENTRIES: CLIENT TEST SUCCESS!!!") ;
}


/*
 * TEST AND SET 
 */
$tskey = 'testandsetkey';
$oldvalue = 'oldvalue';
$newvalue = 'newvalue';
$arakoon->set($tskey, $oldvalue);
$rvalue = $arakoon->testAndSet($tskey, $oldvalue, $newvalue);
if($rvalue != $oldvalue){
    arakoonLog(__FUNCTION__, "TESTANDSET: Nah .... Wrong Value");
}else{
    arakoonLog(__FUNCTION__,"TESTANDSET: CLIENT TEST SUCCESS!!!") ;
}


?>
