package com.meetup.storm

import java.util

import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.Tuple
import play.api.libs.json.Json
import redis.clients.jedis.Jedis

class Persister extends BaseBasicBolt{
  var _jedis: Jedis = _
  var pre_timestamp = 0L

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext): Unit = {
    _jedis = new Jedis("localhost")
  }

  override def execute(input: Tuple, collector: BasicOutputCollector): Unit = {
    val message = input.getStringByField("message")
    val json = Json.parse(message)
    val group_lon = json("location")(0).toString.toDouble
    val group_lat = json("location")(1).toString.toDouble
    val group_name = json("group_name").toString
    val value = (group_lat, group_lon, group_name)
    val timestamp: Long = System.currentTimeMillis / 1000

    if (timestamp != pre_timestamp){
      pre_timestamp = timestamp
//      println(timestamp, value)
      _jedis.set(timestamp.toString,value.toString())
    }

  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {}
}
