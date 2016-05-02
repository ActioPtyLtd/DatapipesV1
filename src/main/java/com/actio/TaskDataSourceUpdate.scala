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
    val query = "SELECT * FROM invoice WHERE " + dataSet.rows.map(r => keyColumns().map(c => c + " = '" + dataSet.getValue(r, c) + "'").mkString(" AND ")).mkString(" OR ")

    val dataSource = DPSystemFactory.newDataSource(dataSourceConfig.withValue("query.queryTemplate", ConfigValueFactory.fromAnyRef(query)), masterConfig)
    dataSource.execute()

    // this really doesn't look necessary. DataSetRS seems to lose the header.
    dataSource.dataSet.initBatch
    val dataSourceFullds = DataSetTransforms.addHeader(dataSource.dataSet.getNextBatch, List("code", "patientid", "specialistid", "status", "invoiceissued", "total"))
    val dataSourceKeyOnlyds = DataSetTransforms.keep(dataSourceFullds, keyColumns())

    val newDataSet = DataSetTableScala(dataSet.header, dataSet.rows.filterNot(r => dataSourceKeyOnlyds.rows.contains(keyColumns().map(dataSet.getValue(r,_)))))
      //dataSource.create(newDataSet)

    val updatedDataSet =
        DataSetTableScala(dataSet.header, dataSet.rows.filter(r => {
          val option = dataSourceFullds.rows.find(ri => keyColumns().forall(c => dataSet.getValue(r, c) == dataSourceFullds.getValue(ri, c)))
          if(option.isDefined)
            !dataSet.header.forall(c => dataSet.getValue(r,c) == dataSource.dataSet.getValue(option.get, c))
          else
            false
        }))

      dataSource.update(updatedDataSet)
  }

  override def load(): Unit = ???

  override def extract(): Unit = ???

  def keyColumns() = List("code")
}
