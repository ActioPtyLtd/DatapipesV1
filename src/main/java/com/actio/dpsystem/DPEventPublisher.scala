package com.actio.dpsystem

import com.actio.{DataArray, _}

import scala.collection.mutable
/**
  * Created by jim on 25/07/16.
  */

// run system pipelines for publishing
class DPEventPublisher(val dprun: DPSystemRuntime) {

  def getRun(): DataSet = DataArray(
    DataRecord(
      DataString(DPSystemConfigurable.CONFIG_NAME,dprun.getConfigName),
      DataString(DPSystemConfigurable.RUN_ID,dprun.getRunID)))

  import collection.JavaConverters._

  def getPipeline(): DataSet = {

    val pipes = dprun.getSysconf().pipelinesMap.values().asScala.map(n => (n.getRunID(), n.getInstanceID, n.name))

    DataArray(pipes.filter(f => Option(f._1).isDefined).map(p => DataRecord(List(
      DataString(DPSystemConfigurable.RUN_ID, p._1),
      DataString("pipelineRunId", p._2),
      DataString("pipelineName", p._3),
      DataString(DPSystemConfigurable.CONFIG_NAME,dprun.getConfigName)))).toList)

  }

  def getTasks(): DataSet = {
    val tasks = dprun.getSysconf().pipelinesMap.values().asScala.
      map(p => (p.getRunID(), p.getInstanceID(), p.getNodeList)).
      flatMap(p => p._3.asScala.map(n => (n.getInstanceID(), n.name, p._1, p._2)))

    DataArray(tasks.filter(f => Option(f._3).isDefined).
      map(p => DataRecord(List(
        DataString(DPSystemConfigurable.CONFIG_NAME,dprun.getConfigName),
        DataString("runId", p._3),
        DataString("pipelineRunId", p._4),
        DataString("taskRunId", p._1),
        DataString("taskName", p._2)))).toList)

  }

  def postProcessEvents(events: List[dpEvent]): List[dpEvent] = {
    var taskMapTSmin: Map[String, dpEvent] = Map()
    var taskMapTSmax: Map[String, dpEvent] = Map()
    var taskMaptheCount: Map[String, Int] = Map()
    var taskNames: Set[String] = Set()
    events.foreach(ev => {
      taskNames += ev.keyName
      val evmin = taskMapTSmin.get(ev.keyName)
      if (evmin.isEmpty || evmin.get.timeStamp > ev.timeStamp) {
        taskMapTSmin += (ev.keyName -> ev)
      }
      val evmax = taskMapTSmax.get(ev.keyName)
      if (evmax.isEmpty || evmax.get.timeStamp <= ev.timeStamp) {
        taskMapTSmax += (ev.keyName -> ev)
      }
      var evcount: Int = taskMaptheCount.get(ev.keyName).getOrElse(0)
      taskMaptheCount += (ev.keyName -> (evcount + ev.theCount))
    })
    var neweventsList: List[dpEvent] = List()
    val runstart = taskMapTSmin.get("run")
    if (!runstart.isEmpty) {
      neweventsList = neweventsList ::: List(runstart.get)
    }
    neweventsList = neweventsList ::: events.filter(ev => "ERROR".equals(ev.theType))
    taskNames.filter(taskName => !"run".equals(taskName)).foreach(taskname => {
      var eventEntry: List[dpEvent] = List()
      var ev: dpEvent = taskMapTSmin.get(taskname).get
      eventEntry = eventEntry ::: List(dpEvent(ev.pipeInstanceId,
        ev.taskInstanceId, "INFO", "START", "Starting Task",
        ev.keyName, "", 0, ev.timeStamp))
      ev = taskMapTSmax.get(taskname).get
      eventEntry = eventEntry ::: List(
          dpEvent(ev.pipeInstanceId, ev.taskInstanceId,
            "INFO", "PROGRESS", "Finishing Task", ev.keyName,
          "RecordCount", taskMaptheCount.getOrElse(ev.keyName, 0), ev.timeStamp))
      eventEntry = eventEntry ::: List(dpEvent(ev.pipeInstanceId, ev.taskInstanceId, "INFO", "FINISH", "Finished Task", ev.keyName, "", 0, ev.timeStamp))
      neweventsList = neweventsList ::: eventEntry
    })
    val runfinish = taskMapTSmax.get("run")
    if (!runfinish.isEmpty) {
      neweventsList = neweventsList ::: List(runfinish.get)
    }
    neweventsList.sortBy(ev => ev.timeStamp)
  }

  def GetEvents(): DataSet = {
    dprun.events.eventList = postProcessEvents(dprun.events.eventList)

    DataArray(dprun.events.eventList.map(e => DataRecord(List(
      DataString(DPSystemConfigurable.CONFIG_NAME,dprun.getConfigName),
      DataString("event_id", DPSystemConfigurable.getUUID ),
      DataString("event_time", dprun.events.getTime(e.timeStamp)),
      DataString("runId", dprun.events.runId),
      DataString("pipeline_run_id", e.pipeInstanceId),
      DataString("task_run_id", e.taskInstanceId),
      DataString("event_type", e.theType),
      DataString("action_type", e.theAction),
      DataString("keyName", e.keyName),
      DataString("message", e.msg),
      DataString("counter_value", e.theCount.toString),
      DataString("counter_label", e.counter),
      DataString("counter_total", e.theCount.toString)))))

  }

  def getConfigCreate(): DataSet = {
    getConfigCreate("")
  }

  def getConfigCreate(postfix:String): DataSet = {

    // get the config file name

    // check sys section if defined
    // otherwise use name of config file
     DataArray(DataRecord(
       DataString(DPSystemConfigurable.CONFIG_NAME, dprun.getConfigName),
       DataString(DPSystemConfigurable.CONFIG_DESCRIPTION, dprun.getSysconf.getSystemConfig(DPSystemConfigurable.CONFIG_DESCRIPTION)),
       DataString(DPSystemConfigurable.CONFIG_NAME_POSTFIX,dprun.getConfigName + "__" + postfix)
     ))
  }

  def getConfigPipe(): DataSet = {

    val pipes = dprun.getSysconf().pipelinesMap.values().asScala

    DataArray(pipes.map(p => DataRecord(
      DataString("configName", dprun.getConfigName),
      DataString("pipelineName", p.getName),
      DataString("source", p.getAttrib("source")),
      DataString("destination", p.getAttrib("destination")),
      DataString("sourceGeoLoc", p.getAttrib("sourceGeoLoc")),
      DataString("destinationGeoLoc", p.getAttrib("destinationGeoLoc"))
    )).toList)
  }

  def getConfigTask(): DataSet = {

    DataArray(dprun.getSysconf().pipelinesMap.values().asScala.flatMap(p =>
      p.getNodeList.asScala.map(t =>
        DataRecord(
          DataString("taskName", t.name),
          DataString("taskType", t.getType match {
            case "extract" => "EXTRACT"
            case "load" => "LOAD"
            case _ => "TRANSFORM"
          }),
          DataString("configName", dprun.getConfigName))
      )).toList.distinct)

  }


  def getConfigStage(): DataSet = {

    DataArray(dprun.getSysconf().pipelinesMap.values().asScala.flatMap(p =>
      p.getNodeList.asScala.zipWithIndex.map { case (t, x) =>
        DataRecord(
          DataString("taskName", t.name),
          DataString("taskType", t.getType match {
            case "extract" => "EXTRACT"
            case "load" => "LOAD"
            case _ => "TRANSFORM"
          }),
          DataString("pipelineName", p.name),
          DataString("configName", dprun.getConfigName),
          DataString("seq", (x+1).toString))
      }).toList)

  }

  def clearEvents: Unit =  {
    dprun.events.clearEvents
  }
}

