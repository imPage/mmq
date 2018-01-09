<?php

/**
 * mongo队列简单实现
 *
 * 写操作：setMmq($mqkey, $content)
 * $mqkey 队列名 $content 打入队列的数据（数组） $type默认1 普通业务 2短消息
 * 读操作：getMmq($mqkey)
 * $mqkey 队列名 $type默认1 普通业务 2短消息
 * updateMmq($id, $result)
 * $id 取出数据的主键id
 * $result 业务处理结果（数组） 不管成功失败 如果需要请求多个接口 使用callMulti调用
 */
class Mmq
{
    private $mongo;

    public function __construct($collection = 'mongo_mq')
    {
        $di          = \Phalcon\DI::getDefault();
        $this->mongo = $di['mongo']->selectCollection($collection);
    }
    /**
     * 队列里打数据
     * @param  String $mqkey 队列名字
     * @param  Array $content 打入队列的数据
     * @param  Int $type 队列的类型
     * @return Array 取出的数据
     */
    public function set($mqkey, $content)
    {
        if (empty($mqkey) || empty($content)) {
            return fasle;
        }
        $data = array(
            'mqkey'      => $mqkey,
            'content'    => $content,
            'state'      => 0,
            'createtime' => time(),
            'updatetime' => time(),
            'statetime'  => time(),

        );
        $res          = $this->mongo->insertOne($data);
        $insertResult = $res->getInsertedId();
        if ($insertResult) {
            return true;
        }
        return false;
    }

    /**
     * 队列取数据
     * @param  String $mqkey 队列名字
     * @param  Int $type 队列的类型
     * @return Array 取出的数据
     */
    public function get($mqkey)
    {
        if (empty($mqkey)) {
            return false;
        }
        $res = $this->mongo->findOneAndUpdate(
            array(
                'mqkey' => $mqkey,
                'state' => 0,
            ),
            array(
                '$set' => array(
                    'state'      => 1,
                    'statetime'  => time(),
                    'updatetime' => time(),
                ),
            )
        );
        if (!$res) {
            return array();
        }
        return $res;
    }

    /**
     * 取出数据处理后更新
     * @param  Object $id 处理数据对应主键id
     * @param  Array $result 任务处理返回结果
     * @return boolean  true/false
     */
    public function up($id, $result)
    {
        if (empty($id) || empty($result)) {
            return false;
        }
        $condition = ['_id' => $id];
        $data      = array(
            'result'     => $result,
            'state'      => 2,
            'statetime'  => time(),
            'updatetime' => time(),
        );
        $res           = $this->mongo->updateOne($condition, ['$set' => $data]);
        $upsertedCount = $res->getModifiedCount();
        if ($upsertedCount) {
            return true;
        }
        return false;
    }
}
