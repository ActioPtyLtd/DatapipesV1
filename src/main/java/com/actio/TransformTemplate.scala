package com.actio

import com.actio.dpsystem.{DPSystemConfig, DPFnNode}
import scala.collection.JavaConverters._

/**
  * Created by mauri on 3/08/2016.
  */

class TransformTemplate extends TaskTransform {

  override def execute(): Unit = {
    val records = dataSet.elems.map(ds => DataRecord(ds :: templates.map(t => DataString(t._1, TestMeta.evalTemplate(ds,t._2).stringOption.getOrElse(""))).toList))
    val batch = DataRecord("batch", List(DataArray(records.toList)))
    dataSet = batch
  }

  lazy val templates = config.getObject("templates").asScala.map(kv => (kv._1, kv._2.unwrapped().toString)).toList
}
