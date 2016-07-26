package com.actio.dpsystem

import java.text.SimpleDateFormat
import java.text.DateFormat

/**
  * Created by jim on 22/07/16.
  */


case class dpEvent(instanceId: String,
                   theType: String,
                   theAction: String,
                   msg: String,
                   keyName: String = "",
                   counter: String = "",
                   theCount: Int = 0) {
  def timeStamp: Long = System.currentTimeMillis()
}

/*
object dpEvents {
  case class Start (msg : String) extends dpEvents
  case class Success (msg : String) extends dpEvents
  case class Failure (msg : String) extends dpEvents
  case class End (msg : String) extends dpEvents
  case class Info (msg : String) extends dpEvents
  case class Counter (msg : String) extends dpEvents
}
*/

case class DPEventAggregator(runId: String) {

  var eventList: List[dpEvent] = List()

  // ============================================================

  // function add event to the list

  def info(instanceId: String, theAction: String, themsg: String, keyName: String, counter: String = "", count: Int = 0): Unit = {
    addEvent(dpEvent(instanceId, "Info", theAction, themsg, keyName, counter, count))
  }

  def err(instanceId: String, theAction: String, themsg: String, keyName: String, counter: String = "", count: Int = 0): Unit = {
    addEvent(dpEvent(instanceId, "Error", theAction, themsg, keyName, counter, count))
  }

  def addEvent(theEntry: dpEvent): Unit = {
    this.eventList = this.eventList ::: List(theEntry)
  }

  def warn(instanceId: String, theAction: String, themsg: String, keyName: String, counter: String = "", count: Int = 0): Unit = {
    addEvent(dpEvent(instanceId, "Warn", theAction, themsg, keyName, counter, count))
  }

  // ============================================================

  def dump(): Unit = {
    eventList.foreach(e => {
      var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.s'Z'")
      var resultdate: java.util.Date = new java.util.Date(e.timeStamp)
      println((sdf.format(resultdate) + "::" + runId + "::" + e))
    })
  }
}
