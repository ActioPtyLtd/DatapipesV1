package com.actio

import com.actio.dpsystem.{DPSystemFactory, DPSystemConfigurable}
import com.typesafe.config.ConfigValueFactory

/**
  * Created by mauri on 2/05/2016.
  */
class TaskDataSourceUpdate extends Task {

  override def execute() = {
    super.setConfig(sysconf.getTaskConfig(this.node.getName).toConfig, sysconf.getMasterConfig)

    for (datasetdata <- dataSet.elems) {

      val tabledataset = DataSetTableScala(dataSet.schema, datasetdata)

      val dataSourceConfig = config.getConfig(DPSystemConfigurable.DATASOURCE_LABEL)
      val query = dataSourceConfig.getConfig("query").getString("read") + " WHERE " + datasetdata.elems.map(r => keyColumns().map(c => c + " = '" + r(c).stringOption.getOrElse("").replace("'", "''") + "'").mkString(" AND ")).mkString(" OR ")

      val dataSource = DPSystemFactory.newDataSource(dataSourceConfig.withValue("query.read", ConfigValueFactory.fromAnyRef(query)), masterConfig)
      dataSource.read(tabledataset) // ds doesn't matter here, our query doesn't need any parameters

      val fulldata = dataSource.dataSet.elems.toList

      if(fulldata.isEmpty)
        dataSource.create(dataSet)
      else {
        for (dsourcedata <- fulldata) {
          val newDataSet = DataSetTransforms.newRows(tabledataset, DataSetTableScala(dataSource.dataSet.schema, dsourcedata), keyColumns())
          if (!DataSetTransforms.isEmptyDataSet(newDataSet))
            dataSource.create(newDataSet)

          val updatedDataSet = DataSetTransforms.changes(tabledataset, DataSetTableScala(dataSource.dataSet.schema, dsourcedata), keyColumns())
          if (!DataSetTransforms.isEmptyDataSet(updatedDataSet))
            dataSource.update(updatedDataSet)
        }
      }
    }
  }

  override def load(): Unit = ???

  override def extract(): Unit = ???

  import scala.collection.JavaConverters._
  def keyColumns() = getConfig.getStringList("keys").asScala.toList

}
