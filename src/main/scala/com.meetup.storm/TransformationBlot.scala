package com.meetup.storm

import java.util

import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.{BasicOutputCollector, OutputFieldsDeclarer}
import org.apache.storm.topology.base.BaseBasicBolt
import org.apache.storm.tuple.{Fields, Tuple, Values}
import play.api.libs.json._
import play.api.libs.functional.syntax._

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by chengzhi on 7/25/17.
  */
class TransformationBlot extends BaseBasicBolt{
  var _collector: BasicOutputCollector = _
  val _country =  scala.collection.mutable.Map[String, String]()
  var _startTime: String = _

  override def prepare(stormConf: util.Map[_, _], context: TopologyContext): Unit = {
    val bufferedSource = scala.io.Source.fromFile("/Users/chengzhi/Documents/Github/Longpull_Storm/src/main/scala/countrydata.csv")
    for (line <- bufferedSource.getLines()){
      val cols = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
      _country += (cols(1)) -> cols(0).toUpperCase()
    }
    _startTime = (System.currentTimeMillis / 1000).toString
  }

  override def execute(input: Tuple, collector: BasicOutputCollector): Unit = {
    _collector = collector
    val message = input.getStringByField("message")
    val json = Json.parse(message)

    val (group_name, group_country, group_city, group_lon, group_lat, group_topics) = (json \ "group").get match {
      case x => {
        val group_name = (x \ "group_name").get.toString().drop(1).dropRight(1)
        val group_country = _country.getOrElse((x \ "group_country").get.toString.toUpperCase().drop(1).dropRight(1), "UN")
        val group_city = (x \ "group_city").get.toString.drop(1).dropRight(1)
        val group_topics_raw = (x \ "group_topics").asOpt[Array[Map[String,String]]].get
        val group_topics = group_topics_raw.flatMap(topic => topic.get("topic_name"))
        val group_lon = (x \ "group_lon").get.toString().toDouble
        val group_lat = (x \ "group_lat").get.toString().toDouble
        (group_name, group_country, group_city, group_lon, group_lat, group_topics)
      }
    }
    val response = json("response").toString
    val event_name = json("event")("event_name").toString
    val event_time = (json \ "event" \ "time").asOpt[String].getOrElse("")
    val event_url = json("event")("event_url").toString
    val (venue_name, venue_log, venue_lat) = (json \ "venue").asOpt[String] match {
      case None => ("",0.0,0.0)
      case _ => {
        val venue_name = json("venue")("venue_name").toString
        val venue_log = json("venue")("lon").toString match{
          case "" => 0.0
          case x => x.toDouble
        }
        val venue_lat = json("venue")("lat").toString match{
          case "" => 0.0
          case x => x.toDouble
        }
        (venue_name, venue_log, venue_lat)
      }
    }
    val update_date_time = json("mtime").toString
    val timestamp: String = (System.currentTimeMillis / 1000).toString

    val record = new Record(group_name, group_country, group_city, Array(group_lon, group_lat), group_topics, response, event_name, event_time,
    event_url, venue_name, venue_log, venue_lat, update_date_time, timestamp)
    implicit val residentReads = Json.writes[Record]
    val jsonOutput = Json.toJson(record)
    if (update_date_time>_startTime && !group_topics.exists(p=> p=="test mug") && event_name != "\"Nova Labs Robotics: SuGObots - Week Long Afternoons\""){
      _collector.emit(new Values(jsonOutput.toString()))
    }
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("message"))
  }
}
