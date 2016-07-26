package com.actio

import com.actio.dpsystem.{ DPSystemConfigurable, DPSystemFactory }
import com.typesafe.config.{ Config, ConfigValueFactory }

import scala.collection.Iterator

/**
  * Created by mauri on 4/07/2016.
  */
class TaskMerge extends Task {

  lazy val attribute = config.getString("attribute")

  override def execute(): Unit = {

    super.setConfig(sysconf.getTaskConfig(this.node.getName).toConfig, sysconf.getMasterConfig)
    val dataSourceConfig = config.getConfig(DPSystemConfigurable.DATASOURCE_LABEL)
    val dataSource = DPSystemFactory.newDataSource(dataSourceConfig, masterConfig)

    val operation = TaskMerge.getOperation(dataSourceConfig)

    val queryResult = DataRecord(attribute, dataSource.executeQueryLabel(dataSet, operation).elems.toList)

    dataSet = merge(dataSet, queryResult)
  }

  def merge(ds1: DataSet, ds2: DataSet) = DataArray(List(DataRecord(ds2 :: ds1.headOption.get.elems.toList)))


  override def load(): Unit = ???

  override def extract(): Unit = ???

  private def configOption(config: Config, path: String) = if (config.hasPath(path)) Some(config.getString(path)) else None
}

object TaskMerge {

  def getOperation(config: Config) = {
    if (config.hasPath("query.update"))
      "update"
    else if (config.hasPath("query.create"))
      "create"
    else
      "read"
  }

}