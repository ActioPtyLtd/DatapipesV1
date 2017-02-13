package com.actio

import com.actio.dpsystem.{ DPSystemFactory, DPSystemConfigurable }
import com.typesafe.config.ConfigValueFactory
import scala.collection.mutable.Map
import com.actio.dpsystem.Logging

/**
 * Created by mauri on 2/05/2016.
 */



class TaskDataFind extends Task with Logging {


  override def execute(): Unit = {

    super.setConfig(sysconf.getTaskConfig(this.node.getName).toConfig, sysconf.getMasterConfig)

    if (!DataSetCache.isTaskInitialised(taskName)) {
      DataSetCache.initaliseTask(taskName)

      val dataSourceDataSet = dataSource.read(Nothin())

      val all = DataArray(dataSourceDataSet
        .elems
        .flatMap(e => e.elems)
        .toList)
        //.groupBy(g => MetaTerm.evalTemplate(g, key).stringOption.getOrElse(""))

      val rec = //DataRecord(groups.map(g => DataArray(g._1, g._2)).toList)
        MetaTerm.eval(all, dataSetRight)

      DataSetCache.add(taskName,taskName,rec)
    }

    dataSet = MetaTerm.evalLambdas(findTerm, Seq(dataSet, DataSetCache.get(taskName, taskName).getOrElse(Nothin())))
  }

  def dataSource: DataSource = DPSystemFactory.newDataSource(config.getConfig(DPSystemConfigurable.DATASOURCE_LABEL), masterConfig)

  override def load(): Unit = ???

  override def extract(): Unit = ???

  lazy val findTerm: String = getConfig.getString("findTerm")

  lazy val taskName: String = this.node.getName

  lazy val dataSetRight: String = getConfig.getString("dataSetRight")

  override def clazz: Class[_] = classOf[TaskDataFind]
}
