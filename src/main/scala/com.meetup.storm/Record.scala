package com.meetup.storm

/**
  * Created by chengzhi on 7/29/17.
  */
case class Record(group_name: String, group_country: String, group_city: String, location: Array[Double], group_topics: Array[String],
                  response: String, event_name: String, event_time: String, event_url: String, venue_name: String,
                  venue_log: Double,venue_lat: Double, update_date_time: String, insert_date_time: String) {
}
