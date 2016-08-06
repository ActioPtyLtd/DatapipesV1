package com.actio

import com.actio.dpsystem.{ DPSystemConfigurable, DPSystemFactory }
import com.typesafe.config.{ Config, ConfigValueFactory }

import scala.collection.Iterator

/**
 * Created by mauri on 4/07/2016.
 */
class TaskInclude extends Task {

  lazy val forEach = config.getString("foreach")
  lazy val attribute = config.getString(DPSystemConfigurable.ATTRIBUTE_LABEL)

  override def execute(): Unit = {

    super.setConfig(sysconf.getTaskConfig(this.node.getName).toConfig, sysconf.getMasterConfig)
    val dataSourceConfig = config.getConfig(DPSystemConfigurable.DATASOURCE_LABEL)
    val dataSource = DPSystemFactory.newDataSource(dataSourceConfig, masterConfig)

    val operation = TaskInclude.getOperation(dataSourceConfig)
    val iterate = if(forEach.isEmpty) List(dataSet) else TaskInclude.split(dataSet, forEach).toList

    val allQueryResults = iterate.map(d => (d, dataSource.executeQueryLabel(d, operation).elems.toList.head)).toList

    if (config.hasPath(DPSystemConfigurable.ATTRIBUTE_LABEL)) {
      dataSet = new DataSetFixedData(dataSet.schema, DataRecord("", List(DataArray(attribute, allQueryResults.map(_._2)), dataSet.elems.toList.head)))
    } else {
      dataSet = DataArray("", allQueryResults.map(r => DataRecord("", List(DataRecord("item", r._1.elems.toList), DataRecord("response", List(r._2))))))
    }
  }

  override def load(): Unit = ???

  override def extract(): Unit = ???

  private def configOption(config: Config, path: String) = if (config.hasPath(path)) Some(config.getString(path)) else None
}

object TaskInclude {

  def split(dataSet: DataSet, forEach: String): List[DataSet] = dataSet.elems.flatMap(g => g.find(forEach).map(splitGlobalAndLocal(g, _))).toList

  def splitGlobalAndLocal(dsGlobal: DataSet, dsLocal: DataSet) =
    DataRecord("", List(DataRecord("local", dsLocal.elems.toList), DataRecord("global", dsGlobal.elems.toList)))

  def getOperation(config: Config) = {
    if (config.hasPath("query.update"))
      "update"
    else if (config.hasPath("query.create"))
      "create"
    else
      "read"
  }

  def getAddHeader(ds: DataSet, item: DataSet, config: Config): DataSet = if (config.hasPath("responseAdd"))
    DataRecord("", ds :: getAddHeader(item, List(("job", "local.id"))).map(h => DataString(h._1, h._2)).toList)
  else
    ds

  def getAddHeader(ds: DataSet, headers: Seq[(String, String)]) = headers.map(h => h._1 -> ds.value(h._2).stringOption.getOrElse("")).toMap
}