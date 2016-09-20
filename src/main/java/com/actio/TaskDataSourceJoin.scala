package com.actio

import com.actio.dpsystem.{ DPSystemFactory, DPSystemConfigurable }
import com.typesafe.config.ConfigValueFactory
import scala.collection.mutable.Map

/**
 * Created by mauri on 2/05/2016.
 */

object DataSetCache {
  private val lookup = scala.collection.mutable.HashMap[String,Map[String, DataSet]]()

  def isTaskInitialised(taskName: String): Boolean = lookup.contains(taskName)

  def initaliseTask(taskName: String): Unit = {
    lookup += (taskName -> scala.collection.mutable.HashMap[String, DataSet]())
  }

  def add(taskName: String, key: String, ds: DataSet): Unit = {
    lookup(taskName) += (key -> ds)
  }

  def get(taskName: String, key: String): Option[DataSet] = lookup(taskName).get(key)

  def clear(): Unit = {
    lookup.clear()
  }
}

class TaskDataJoin extends Task {


  override def execute(): Unit = {

    super.setConfig(sysconf.getTaskConfig(this.node.getName).toConfig, sysconf.getMasterConfig)

    // load all from the data source
    if (!DataSetCache.isTaskInitialised(taskName)) {
      DataSetCache.initaliseTask(taskName)

      val dataSourceDataSet = dataSource.read(Nothin())
      val es = dataSourceDataSet.elems.flatMap(e => dataSourceIterate(e).elems).toList

      es.foreach(ds => {
        DataSetCache.add(
          taskName,
          MetaTerm.evalTemplate(ds, keyR).stringOption.getOrElse(""),
          ds
        )
      })
    }

    dataSet = DataRecord(DataArray(dataSet.elems
      .map(e => (e, DataSetCache.get(taskName, MetaTerm.evalTemplate(e, keyL).stringOption.getOrElse(""))))
      .map(m =>
        if(m._2.isDefined) {
          DataRecord(m._1.label, DataRecord(this.node.getName, List(m._2.get)) :: m._1.elems.toList)
        }
        else {
          DataRecord(m._1.label, Nothin(this.node.getName) :: m._1.elems.toList)
        })
      .toList))
  }

  def dataSource = DPSystemFactory.newDataSource(config.getConfig(DPSystemConfigurable.DATASOURCE_LABEL), masterConfig)

  def dataSourceIterate(e: DataSet): DataSet =
    if(config.hasPath("iterateR")) {
      MetaTerm.eval(e, iterateR)
    } else {
      e
    }

  override def load(): Unit = ???

  override def extract(): Unit = ???

  lazy val iterateR = getConfig.getString("iterateR")
  lazy val keyL = getConfig.getString("keyL")
  lazy val keyR = getConfig.getString("keyR")

  lazy val taskName = this.node.getName
}
