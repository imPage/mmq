<?php
require_once 'Mmq.php';
$mmq = new \Mmq();

//写队列
$mmq->set($mqkey, $content);

//读队列
$mmq->get($mqkey);
