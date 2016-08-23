package com.actio

import com.actio.dpsystem.{ DPSystemFactory, DPSystemConfigurable }
import com.typesafe.config.ConfigValueFactory
import scala.collection.mutable.Map
import scala.collection.JavaConverters._

/**
 * Created by mauri on 2/05/2016.
 */

object Cache {
  var dim: Option[Map[String, String]] = None
}

class TaskDataSourceUpdate extends Task {
  Cache.dim = None
  override def execute(): Unit = {
    super.setConfig(sysconf.getTaskConfig(this.node.getName).toConfig, sysconf.getMasterConfig)

    // load all from the data source
    if (Cache.dim.isEmpty) {
      val dataSourceDataSet = dataSource.read(Nothin())

      val tes = dataSourceDataSet.elems.toList
      val es = tes.flatMap(e => TestMeta.eval(e, iterateR).elems).toList

      Cache.dim = Some(scala.collection.mutable.HashMap[String, String](
        es
          .map(m =>
            (TestMeta.evalTemplate(m, keyR).stringOption.getOrElse(""),
              TestMeta.evalTemplate(m, changeR).stringOption.getOrElse(""))).toList: _*))
    }

    val dsIncoming = dataSet.elems
      .map(e =>
        (e,
          TestMeta.evalTemplate(e, keyL).stringOption.getOrElse(""),
          TestMeta.evalTemplate(e, changeL).stringOption.getOrElse(""))).toList

    // work out which elements then need to be inserted or updated
    val inserts = dsIncoming.filter(d => Cache.dim.get.get(d._2).isEmpty)
    val updates = dsIncoming.filter(d => Cache.dim.get.get(d._2).isDefined)
      .filterNot(d => Cache.dim.get.get(d._2).contains(d._3))

    //TODO: can refactor to load (insert and/or update) these in the next task

    if (inserts.nonEmpty && config.getConfig(DPSystemConfigurable.DATASOURCE_LABEL).hasPath("query.create")) {
      dataSource.execute(DataArray("", inserts.map(_._1)), config.getConfig(DPSystemConfigurable.DATASOURCE_LABEL).getString("query.create"))

      // update the dimension cache, so this isn't repeated again
      Cache.dim.get ++= inserts.map(m => (m._2, m._3))
    }

    if (updates.nonEmpty && config.getConfig(DPSystemConfigurable.DATASOURCE_LABEL).hasPath("query.update")) {
      dataSource.execute(DataArray("", updates.map(_._1)), config.getConfig(DPSystemConfigurable.DATASOURCE_LABEL).getString("query.update"))

      // update the dimension cache, so this isn't repeated again
      Cache.dim.get ++= updates.map(m => (m._2, m._3))
    }

  }

  def dataSource = DPSystemFactory.newDataSource(config.getConfig(DPSystemConfigurable.DATASOURCE_LABEL), masterConfig)

  override def load(): Unit = ???

  override def extract(): Unit = ???

  lazy val iterateR = getConfig.getString("iterateR")
  lazy val keyL = getConfig.getString("keyL")
  lazy val keyR = getConfig.getString("keyR")
  lazy val changeL = getConfig.getString("changeL")
  lazy val changeR = getConfig.getString("changeR")

}
