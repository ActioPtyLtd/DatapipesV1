package com.actio

import com.actio.dpsystem.{ DPSystemFactory, DPSystemConfigurable }
import com.typesafe.config.ConfigValueFactory
import scala.collection.mutable.Map
import scala.collection.JavaConverters._

/**
 * Created by mauri on 2/05/2016.
 */

object JoinCache {
  var dim: Map[String, DataSet] = scala.collection.mutable.HashMap()
}

class TaskDataJoin extends Task {

  override def execute(): Unit = {
    super.setConfig(sysconf.getTaskConfig(this.node.getName).toConfig, sysconf.getMasterConfig)

    // load all from the data source
    if (JoinCache.dim.isEmpty) {
      val dataSourceDataSet = dataSource.read(Nothin())

      val tes = dataSourceDataSet.elems.toList
      val es = tes.flatMap(e => TestMeta.eval(e, iterateR).elems).toList

      JoinCache.dim = scala.collection.mutable.HashMap[String, DataSet](
        es
          .map(m =>
            (TestMeta.evalTemplate(m, keyR).stringOption.getOrElse(""),
              m)).toList: _*)
    }

    dataSet = DataRecord(List(DataArray(dataSet.elems
      .map(e => (e, JoinCache.dim.get(TestMeta.evalTemplate(e, keyL).stringOption.getOrElse(""))))
      .map(m => if(m._2.isDefined) DataRecord(List(m._1, m._2.get)) else m._1 )
      .toList)))

  }

  def dataSource = DPSystemFactory.newDataSource(config.getConfig(DPSystemConfigurable.DATASOURCE_LABEL), masterConfig)

  override def load(): Unit = ???

  override def extract(): Unit = ???

  lazy val iterateR = getConfig.getString("iterateR")
  lazy val keyL = getConfig.getString("keyL")
  lazy val keyR = getConfig.getString("keyR")

}
