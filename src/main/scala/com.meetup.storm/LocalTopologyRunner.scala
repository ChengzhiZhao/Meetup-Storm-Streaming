package com.meetup.storm

import org.apache.storm.{Config, LocalCluster}
import org.apache.storm.topology.TopologyBuilder
import org.elasticsearch.storm.EsBolt

import scala.collection.mutable
import scala.collection.JavaConverters._

/**
  * Created by chengzhi on 7/25/17.
  */
object LocalTopologyRunner {
  def main(args: Array[String]): Unit = {
    val conf =  mutable.HashMap[String,String]()
    conf.put("es.input.json", "true")
    conf.put("es.nodes", "localhost")
    conf.put("es.port", "9200")
    conf.put("es.index.auto.create", "true")

    val builder = new TopologyBuilder()
    builder.setSpout("longpull", new LongpullSpout())
    builder.setBolt("transform", new TransformationBlot()).shuffleGrouping("longpull")
    builder.setBolt("es_bolt", new EsBolt("storm/json-trips", conf.asJava)).shuffleGrouping("transform").addConfiguration(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5)
    builder.setBolt("persister", new Persister()).shuffleGrouping("transform")

    val topology = builder.createTopology()
    val cluster = new LocalCluster()
    val config = new Config()
    config.put("es.index.auto.create", "true")
//    config.setDebug(true)
    cluster.submitTopology("test-topology", config, topology)
  }
}
