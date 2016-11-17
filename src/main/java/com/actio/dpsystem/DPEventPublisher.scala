package com.actio.dpsystem

import com.actio.{DataArray, _}
/**
  * Created by jim on 25/07/16.
  */

// run system pipelines for publishing
class DPEventPublisher(val dprun: DPSystemRuntime) {

  def pushConfig(): Unit = {

    // treat config as a datasource - return a dataset

    /*

     */

  }

  def getRun(): DataSet = DataArray(DataString(dprun.getRunID))

  import collection.JavaConverters._

  def getPipeline(): DataSet = {

    val pipes = dprun.getSysconf().pipelinesMap.values().asScala.map(n => (n.getRunID(), n.getInstanceID, n.name))

    DataArray(pipes.filter(f => Option(f._1).isDefined).map(p => DataRecord(List(DataString("runId", p._1), DataString("pipelineRunId", p._2), DataString("pipelineName", p._3)))).toList)

  }

  def getTasks(): DataSet = {
    val tasks = dprun.getSysconf().pipelinesMap.values().asScala.
      map(p => (p.getRunID(), p.getInstanceID(), p.getNodeList)).
      flatMap(p => p._3.asScala.map(n => (n.getInstanceID(), n.name, p._1, p._2)))

    DataArray(tasks.filter(f => Option(f._3).isDefined).
      map(p => DataRecord(List(DataString("runId", p._3), DataString("pipelineRunId", p._4), DataString("taskRunId", p._1), DataString("taskName", p._2)))).toList)

  }

  def GetEvents(): DataSet = {

    DataArray(dprun.events.eventList.map(e => DataRecord(List(
      DataString("event_id", DPSystemConfigurable.getUUID()),
      DataString("event_time", dprun.events.getTime(e.timeStamp)),
      DataString("runId", dprun.events.runId),
      DataString("pipeline_run_id", e.pipeInstanceId),
      DataString("task_run_id", e.taskInstanceId),
      DataString("event_type", e.theType),
      DataString("action_type", e.theAction),
      DataString("keyName", e.keyName),
      DataString("message", e.msg),
      DataString("counter_value", "0"),
      DataString("counter_label", "0")))).toList)

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

    // pipelines [ record_pipeline ( name , task_list ( list of tasks ) ) ]

    val pipes = dprun.getSysconf().pipelinesMap.values().asScala.map(n => ( n.name))

  //    DataRecord("root",
     DataArray(pipes.map(p => DataRecord(
            DataString("configName", dprun.getSysconf.getConfigName),
            DataString("pipelineName", p))).toList)
    //)
  }

  def getConfigTask(): DataSet = {

    DataString ("config","name")
  }

  //DPFnNode(runid, pipeid) -> taksk (taskid,name)

    // treat config as a datasource - return a dataset

    /*
        Given a pipeline

        1. Create Run
          - Config Name : System -> configName
          - RunID

        2. Create Pipeline
          - Pipeline Name : Nodes -> Pipeline
          - RUNID + PIPEUUID

        3. Create Taask
        - Task Name : Nodes ->
        - RUNID + PIPEUUID + TASKUUID

     */
}

class ConfigDataSource {

}