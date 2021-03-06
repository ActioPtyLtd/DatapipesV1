package com.actio

import com.actio.dpsystem.{ DPSystemFactory, DPSystemConfigurable }
import com.typesafe.config.ConfigValueFactory
import scala.collection.mutable.Map

/**
 * Created by mauri on 2/05/2016.
 */

object Cache {
  var dim: Option[Map[String, String]] = None

  def clear: Unit = { dim = None }
}

class TaskDataSourceUpdate extends Task {

  override def execute(): Unit = {
    super.setConfig(sysconf.getTaskConfig(this.node.getName).toConfig, sysconf.getMasterConfig)

    // load all from the data source
    if (Cache.dim.isEmpty) {
      val dataSourceDataSet = dataSource.read(Nothin())

      val es = dataSourceDataSet.elems.flatMap(e => dataSourceIterate(e).elems).toList

      Cache.dim = Some(scala.collection.mutable.HashMap[String, String](
        es
          .map(m =>
            (MetaTerm.evalTemplate(m, keyR).stringOption.getOrElse(""),
              MetaTerm.evalTemplate(m, changeR).stringOption.getOrElse(""))).toList: _*))
    }

    val dsIncoming = dataSet.elems
      .map(e =>
        (e,
          MetaTerm.evalTemplate(e, keyL).stringOption.getOrElse(""),
          MetaTerm.evalTemplate(e, changeL).stringOption.getOrElse("")
        ))
      .toList
      .groupBy(g => g._2)
      .map(f => f._2.head)
      .toList

    // work out which elements then need to be inserted or updated
    val inserts = dsIncoming.filter(d => Cache.dim.get.get(d._2).isEmpty)
    val updates = dsIncoming.filter(d => Cache.dim.get.get(d._2).isDefined)
      .filterNot(d => Cache.dim.get.get(d._2).contains(d._3))

    //TODO: can refactor to load (insert and/or update) these in the next task

    if (inserts.nonEmpty && dataSourceConf.hasPath("query.create")) {
      dataSource.create(DataArray("", inserts.map(_._1)))

      // update the dimension cache, so this isn't repeated again
      Cache.dim.get ++= inserts.map(m => (m._2, m._3))
    }

    if (updates.nonEmpty && dataSourceConf.hasPath("query.update")) {
      dataSource.update(DataArray("", updates.map(_._1)))

      // update the dimension cache, so this isn't repeated again
      Cache.dim.get ++= updates.map(m => (m._2, m._3))
    }

  }

  def dataSource = DPSystemFactory.newDataSource(dataSourceConf, masterConfig)
  def dataSourceConf = config.getConfig(DPSystemConfigurable.DATASOURCE_LABEL)

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
  lazy val changeL = getConfig.getString("changeL")
  lazy val changeR = getConfig.getString("changeR")

}
