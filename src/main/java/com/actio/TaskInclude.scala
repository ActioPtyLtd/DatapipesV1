package com.actio

import com.actio.dpsystem.{DPSystemConfigurable, DPSystemFactory}
import com.typesafe.config.ConfigValueFactory

/**
  * Created by mauri on 4/07/2016.
  */
class TaskInclude extends Task {

  override def execute(): Unit = {

    super.setConfig(sysconf.getTaskConfig(this.node.getName).toConfig, sysconf.getMasterConfig)
    val dataSourceConfig = config.getConfig(DPSystemConfigurable.DATASOURCE_LABEL)
    val dataSource = DPSystemFactory.newDataSource(dataSourceConfig, masterConfig)

    val findResult = dataSet.elems.toList.head.find(foreach).toSet.toList

    val include = DataArray(attribute,findResult.map(f => dataSource.read(f)).toList)

    dataSet = new DataSetFixedData(dataSet.schema,DataRecord("", include :: dataSet.elems.toList))
  }

  override def load(): Unit = ???

  override def extract(): Unit = ???


  lazy val foreach = "data.*.relationships.product.data"
  lazy val attribute = "products"
}
