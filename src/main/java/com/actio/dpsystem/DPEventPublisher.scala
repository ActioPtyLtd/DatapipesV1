package com.actio.dpsystem

import com.actio.{DataArray, _}
/**
  * Created by jim on 25/07/16.
  */

// run system pipelines for publishing
class DPEventPublisher(val dprun: DPSystemRuntime) {

  def getRun(): DataSet = DataArray(
    DataRecord(
      DataString(DPSystemConfigurable.CONFIG_NAME,dprun.getSysconf.getConfigName),
      DataString(DPSystemConfigurable.RUN_ID,dprun.getRunID)))

  import collection.JavaConverters._

  def getPipeline(): DataSet = {

    val pipes = dprun.getSysconf().pipelinesMap.values().asScala.map(n => (n.getRunID(), n.getInstanceID, n.name))

    DataArray(pipes.filter(f => Option(f._1).isDefined).map(p => DataRecord(List(
      DataString(DPSystemConfigurable.RUN_ID, p._1),
      DataString("pipelineRunId", p._2),
      DataString("pipelineName", p._3),
      DataString(DPSystemConfigurable.CONFIG_NAME,dprun.getSysconf.getConfigName)))).toList)

  }

  def getTasks(): DataSet = {
    val tasks = dprun.getSysconf().pipelinesMap.values().asScala.
      map(p => (p.getRunID(), p.getInstanceID(), p.getNodeList)).
      flatMap(p => p._3.asScala.map(n => (n.getInstanceID(), n.name, p._1, p._2)))

    DataArray(tasks.filter(f => Option(f._3).isDefined).
      map(p => DataRecord(List(
        DataString(DPSystemConfigurable.CONFIG_NAME,dprun.getSysconf.getConfigName),
        DataString("runId", p._3),
        DataString("pipelineRunId", p._4),
        DataString("taskRunId", p._1),
        DataString("taskName", p._2)))).toList)

  }

  def GetEvents(): DataSet = {

    DataArray(dprun.events.eventList.map(e => DataRecord(List(
      DataString(DPSystemConfigurable.CONFIG_NAME,dprun.getSysconf.getConfigName),
      DataString("event_id", DPSystemConfigurable.getUUID()),
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
      DataString("counter_total", e.theCount.toString)))).toList)

  }

  def getConfigCreate(): DataSet = {

    // get the config file name

    // check sys section if defined
    // otherwise use name of config file
    DataRecord("initial", DataArray(DataRecord("record",
      List(DataString(DPSystemConfigurable.CONFIG_NAME, dprun.getSysconf.getConfigName),
        DataString(DPSystemConfigurable.CONFIG_DESCRIPTION, dprun.getSysconf.getSystemConfig(DPSystemConfigurable.CONFIG_DESCRIPTION))))))
  }

  def getConfigPipe(): DataSet = {

    val pipes = dprun.getSysconf().pipelinesMap.values().asScala.map(n => (n.name))

    DataArray(pipes.map(p => DataRecord(
      DataString("configName", dprun.getSysconf.getConfigName),
      DataString("pipelineName", p))).toList)
  }

  def getConfigTask(): DataSet = {

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
          DataString("configName", dprun.getSysconf.getConfigName),
          DataString("seq", (x + 1).toString))
      }).toList)

  }
}

