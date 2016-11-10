package com.actio.dpsystem

import java.text.SimpleDateFormat
import java.text.DateFormat


/**
  * Created by jim on 22/07/16.
  */


case class dpEvent(pipeInstanceId: String,
                   taskInstanceId: String,
                   theType: String,
                   theAction: String,
                   msg: String,
                   keyName: String = "",
                   counter: String = "",
                   theCount: Int = 0) {
  val timeStamp: Long = System.currentTimeMillis()
}

case class DPEventAggregator(runId: String) {

  var addevents: Boolean = true
  var eventList: List[dpEvent] = List()

  def disableEvents() : Unit = { addevents = false }

  // ============================================================

  // function add event to the list

  def info(pipeInstanceId: String, taskInstanceId: String, theAction: String, themsg: String, keyName: String, counter: String = "", count: Int = 0): Unit = {
    addEvent(dpEvent(pipeInstanceId, taskInstanceId, "INFO", theAction, themsg, keyName, counter, count))
  }

  def addEvent(theEntry: dpEvent): Unit = {
    if (addevents)
      this.eventList = this.eventList ::: List(theEntry)
  }

  def err(pipeInstanceId: String, taskInstanceId: String, theAction: String, themsg: String, keyName: String, counter: String = "", count: Int = 0): Unit = {
    addEvent(dpEvent(pipeInstanceId, taskInstanceId, "ERROR", theAction, themsg, keyName, counter, count))
  }

  def warn(pipeInstanceId: String, taskInstanceId: String, theAction: String, themsg: String, keyName: String, counter: String = "", count: Int = 0): Unit = {
    addEvent(dpEvent(pipeInstanceId, taskInstanceId, "WARN", theAction, themsg, keyName, counter, count))
  }

  def count(pipeInstanceId: String, taskInstanceId: String, themsg: String, keyName: String, counter: String = "", count: Int = 0): Unit = {
    info(pipeInstanceId, taskInstanceId, "COUNT", themsg, keyName, counter, count)
  }

  // ============================================================

  def dump(): Unit = {
    eventList.foreach(e => {
      var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.s'Z'")
      var resultdate: java.util.Date = new java.util.Date(e.timeStamp)
      println((sdf.format(resultdate) + "::" + runId + "::" + e))
    })

  }

  def getTime(time: Long): String = {
    var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.s'Z'")
    sdf.format(new java.util.Date(time))
  }
}
