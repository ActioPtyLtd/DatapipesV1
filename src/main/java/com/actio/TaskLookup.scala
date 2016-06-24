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

    val tabledataset = DataSetTableScala(dataSet.schema, dataSet.elems.next) //assuming one batch

    val inClause = tabledataset.getColumnValues(lookupColumn1).distinct.map("\'" + _.replace("'","''") + "\'") mkString ","

    val dataSource = DPSystemFactory.newDataSource(dataSourceConfig.withValue("query.queryTemplate", ConfigValueFactory.fromAnyRef(query.replaceAllLiterally("$1", inClause))), masterConfig)

    dataSource.execute()

    val ord = dataSource.dataSet.schema.asInstanceOf[SchemaArray].content.asInstanceOf[SchemaRecord].fields.map(_.label).indexOf(lookupColumn2)  // assuming tablular

    val condition = (row1: List[String], row2: List[String]) =>
      if(row2 == Nil) false else row1(tabledataset.getOrdinalOfColumn(lookupColumn1)) == row2(ord)

    val dataList = dataSource.dataSet.elems.toList

    for(data <- dataList) {
      //TODO: union the resultsets instead of overwriting them here
      dataSet = DataSetTransforms.transformLookupFunc(tabledataset, DataSetTableScala(dataSource.dataSet.schema, data), condition, _ => true)
    }

    // nothing found, but make sure columns found in the data source schema gets added to the incoming table
    if(dataList.isEmpty)
      dataSet = DataSetTransforms.transformLookupFunc(tabledataset, DataSetTableScala(dataSource.dataSet.schema,DataArray(Nil)), condition, _ => true)
  }

  def load(): Unit = ???

  def extract(): Unit = ???

  def lookupColumn1 = getConfig.getString("lookup1")
  def lookupColumn2 = getConfig.getString("lookup2")
}
