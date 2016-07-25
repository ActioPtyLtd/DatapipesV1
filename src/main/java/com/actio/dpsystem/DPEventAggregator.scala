package com.actio.dpsystem

import java.text.SimpleDateFormat
import java.text.DateFormat

/**
  * Created by jim on 22/07/16.
  */


case class dpEvent(instanceId: String,
                   thetype: String,
                   msg: String,
                   keyName: String = "",
                   counter: String = "",
                   thecount: Int = 0) {
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
class DPEventAggregator(runId: String) extends DPSystemConfigurable {

  var eventList: List[dpEvent] = List()

  // ============================================================

  // function add event to the list

  def addEvent(instanceId: String, thetype: String, themsg: String, keyName: String): Unit = {
    addEvent(dpEvent(instanceId, thetype, themsg, keyName))
  }

  def addEventCounter(instanceId: String, thetype: String, themsg: String, keyName: String, counter: String, thecount: Int): Unit = {
    addEvent(dpEvent(instanceId, thetype, themsg, keyName, counter, thecount))
  }

  def addEvent(theEntry: dpEvent): Unit = {
    this.eventList = this.eventList ::: List(theEntry)
  }

  // ============================================================

  def dump(): Unit = {
    eventList.foreach(e => {
      var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm'Z'")
      var resultdate: java.util.Date = new java.util.Date(e.timeStamp)
      println((sdf.format(resultdate) + "::" + e))
    })
  }
}
