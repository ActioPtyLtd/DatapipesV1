package com.actio

import com.actio.dpsystem.{DPSystemFactory, DPSystemConfigurable}
import com.typesafe.config.ConfigValueFactory

/**
  * Created by mauri on 2/05/2016.
  */
class TaskDataSourceUpdate extends Task {

  override def execute() = {
    super.setConfig(sysconf.getTaskConfig(this.node.getName).toConfig, sysconf.getMasterConfig)

    val dataSourceConfig = config.getConfig(DPSystemConfigurable.DATASOURCE_LABEL)
    val query = dataSourceConfig.getConfig("query").getString("read") + " WHERE " + dataSet.rows.map(r => keyColumns().map(c => c + " = '" + dataSet.getValue(r, c) + "'").mkString(" AND ")).mkString(" OR ")

    val dataSource = DPSystemFactory.newDataSource(dataSourceConfig.withValue("query.read", ConfigValueFactory.fromAnyRef(query)), masterConfig)
    dataSource.read(new DataSetTableScala())

    // this really doesn't look necessary. DataSetRS seems to lose the header.
    dataSource.dataSet.initBatch

    for (ds <- dataSource.dataSet) {
      val dataSourceds = DataSetTransforms.addHeader(ds, List("code", "patientid", "specialistid", "status", "invoiceissued", "total"))

      val newDataSet = DataSetTransforms.newRows(dataSet, dataSourceds, keyColumns())
      if (!newDataSet.isEmptyDataSet)
        dataSource.create(newDataSet)

      val updatedDataSet = DataSetTransforms.changes(dataSet, dataSourceds, keyColumns())
      if (!updatedDataSet.isEmptyDataSet)
        dataSource.update(updatedDataSet)
    }
  }

  override def load(): Unit = ???

  override def extract(): Unit = ???

  import scala.collection.JavaConverters._
  def keyColumns() = getConfig.getStringList("keys").asScala.toList
}
