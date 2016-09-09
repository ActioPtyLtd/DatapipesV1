package com.actio

import com.actio.dpsystem.DPSystemFactory
import com.actio.dpsystem.DPSystemConfigurable
import com.typesafe.config.ConfigValueFactory

/**
 * Created by mauri on 18/04/2016.
 */
class TaskLookup extends Task {
  import DataSetTableScala._

  def execute(): Unit = {
    super.setConfig(sysconf.getTaskConfig(this.node.getName).toConfig, sysconf.getMasterConfig)
    if(behavior.equalsIgnoreCase("legacy-lookup-merge")) {
      dataSet = DataRecord(DataArray(dataSet.elems.map(d =>
        DataRecord(d,DataArray(dataSource.read(d).elems.toList))).toList))
    }
    else {
      dataSet = DataRecord(DataArray(dataSet.elems.map(d =>
        DataRecord(DataArray(this.node.getName, dataSource.read(d).elems.toList) :: d.elems.toList)).toList))
    }

  }

  lazy val behavior: String =
    if (config.hasPath(DPSystemConfigurable.BEHAVIOR_LABEL)) {
        config.getString(DPSystemConfigurable.BEHAVIOR_LABEL)
      }
      else {
        ""
      }

  lazy val dataSource: DataSource = DPSystemFactory.newDataSource(config.getConfig(DPSystemConfigurable.DATASOURCE_LABEL), masterConfig)

  def load(): Unit = ???

  def extract(): Unit = ???

}
