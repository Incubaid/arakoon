<?php
include 'arakoon.php';

$Functions = array();

/*The nodes list*/
$clusterId = 'sampleapp';
$nodes = array(
    'sampleapp_0' => array('ip'=>'127.0.0.1', 'port' => 20100)
);

$cfg = new ArakoonClientConfig ($clusterId, $nodes);
$arakoon = new Arakoon($cfg);


/*
 * SET/GET
 */
$key = 'key1'; 
$value = 'value1';
$arakoon->set($key, $value);
$val = $arakoon->get($key);
if($value != $val){
    $ErrFunctions[] = array('result' => FALSE, 'name' => 'SET/GET');
}else{
    $ErrFunctions[] = array('result' => TRUE, 'name' => 'SET/GET');
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
    $ErrFunctions[] = array('result' => FALSE, 'name' => 'SEQUENCE/EXISTS');
}else{
    $ErrFunctions[] = array('result' => TRUE, 'name' => 'SEQUENCE/EXISTS');
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
    $ErrFunctions[] = array('result' => FALSE, 'name' => 'RANGE');
}else{
    $ErrFunctions[] = array('result' => TRUE, 'name' => 'RANGE');
}

/*
 * RANGE ENTRIES
 */
$rkeys = array();
$rkeys = $arakoon->range_entries($keys[0], TRUE, $keys[4], TRUE);
if(count($rkeys)<=0){
    $ErrFunctions[] = array('result' => FALSE, 'name' => 'RANGE ENTRIES');
}else{
    $ErrFunctions[] = array('result' => TRUE, 'name' => 'RANGE ENTRIES');
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
    $ErrFunctions[] = array('result' => FALSE, 'name' => 'TEST AND SET');
}else{
    $ErrFunctions[] = array('result' => TRUE, 'name' => 'TEST AND SET');
}

/*
 * MULTI GET
 */
$values = array();
$values = $arakoon->multiGet($keys);

if(count($values) <= 0){
    $ErrFunctions[] = array('result' => FALSE, 'name' => 'MULTI GET');
}else{
    $ErrFunctions[] = array('result' => TRUE, 'name' => 'MULTI GET');
}


/*
 * DELETE
 */
$arakoon->delete($keys[0]);
$bool = $arakoon->exists($keys[0]);
if($bool){
    $ErrFunctions[] = array('result' => FALSE, 'name' => 'DELETE');
}else{
    $ErrFunctions[] = array('result' => TRUE, 'name' => 'DELETE');
}


/*
 * PREFIX
 */
$prefix = 'k';
$rkeys = array();
$rkeys = $arakoon->prefix($prefix);
if(count($rkeys) <= 0){
    $ErrFunctions[] = array('result' => FALSE, 'name' => 'PREFIX');
}else{
    $ErrFunctions[] = array('result' => TRUE, 'name' => 'PREFIX');
}

/*
 * EXPECT PROGRESS POSSIBLE
 */
$bool = $arakoon->expectProgressPossible();
if(!$bool){
    $ErrFunctions[] = array('result' => FALSE, 'name' => 'EXPECT PROGRESS POSSIBLE');
}else{
    $ErrFunctions[] = array('result' => TRUE, 'name' => 'EXPECT PROGRESS POSSIBLE');
}


/*
 * HELLO / WHO-MASTER
 */
$hello = $arakoon->hello("clientId", $clusterId);
$master = $arakoon->whoMaster();
if(strlen($hello) == 0 || strlen($master) == 0){
    $ErrFunctions[] = array('result' => FALSE, 'name' => 'HELLO / WHO-MASTER');
}else{
    $ErrFunctions[] = array('result' => TRUE, 'name' => 'HELLO / WHO-MASTER');
}


/*
 * Report
 */
foreach ($ErrFunctions as $Function) {
    $res = ($Function['result'] == TRUE)?'OK':'FAIL';
    arakoonLog(__FUNCTION__, "{$Function['name']} .... $res");
}


?>
