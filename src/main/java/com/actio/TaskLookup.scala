package com.actio

import com.actio.dpsystem.DPSystemFactory
import com.actio.dpsystem.DPSystemConfigurable
import com.typesafe.config.ConfigValueFactory


/**
  * Created by mauri on 18/04/2016.
  */
class TaskLookup extends Task {
  import DataSetTableScala._

  def execute() = {
    super.setConfig(sysconf.getTaskConfig(this.node.getName).toConfig, sysconf.getMasterConfig)

    val dataSourceConfig = config.getConfig(DPSystemConfigurable.DATASOURCE_LABEL)
    val query = dataSourceConfig.getString("query.queryTemplate")

    val inClause = dataSet.getColumnValues(lookupColumn1).distinct.map("\'" + _ + "\'") mkString ","

    val dataSource = DPSystemFactory.newDataSource(dataSourceConfig.withValue("query.queryTemplate", ConfigValueFactory.fromAnyRef(query.replaceAllLiterally("$1", inClause))), masterConfig)
    dataSource.execute()

    val condition = (row1: List[String], row2: List[String]) => row1(dataSet.getOrdinalOfColumn(lookupColumn1)) == row2(dataSource.dataSet.getOrdinalOfColumn(lookupColumn2))

    //TODO: fix lookup transform
    //dataSet = dataSet.transformLookup(dataSource.dataSet, condition, _ => true)

  }

  def load(): Unit = ???

  def extract(): Unit = ???

  def lookupColumn1 = getConfig.getString("lookup1")
  def lookupColumn2 = getConfig.getString("lookup2")
}
