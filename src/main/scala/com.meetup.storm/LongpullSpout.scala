package com.meetup.storm

import java.net.URI
import java.util

import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.{Fields, Values}
import scala.collection.JavaConverters._

class LongpullSpout extends BaseRichSpout{
  var _collector: SpoutOutputCollector = _
  var _clientEndPoint:WebsocketClientEndpoint = _
  var nextEmitIndex:Int = _
  var messages: util.List[String] = _

  override def nextTuple(): Unit = {
    messages.asScala.foreach{m =>
        _collector.emit(new Values(m))
    }
    messages.clear()
    Thread.sleep(2500)
  }

  override def open(conf: util.Map[_, _], context: TopologyContext, collector: SpoutOutputCollector): Unit = {
    _collector = collector
    nextEmitIndex = 0
    messages = new util.ArrayList[String]()
    _clientEndPoint = new WebsocketClientEndpoint(new URI("ws://stream.meetup.com/2/rsvps"))
    _clientEndPoint.addMessageHandler(new WebsocketClientEndpoint.MessageHandler() {
      override def handleMessage(message: String): Unit = {
        messages.add(message)
      }
    })
    Thread.sleep(2000)
  }

  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("message"))
  }
}
