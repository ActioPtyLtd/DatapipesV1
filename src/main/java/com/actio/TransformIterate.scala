package com.actio

/**
  * Created by mauri on 15/07/2016.
  * This can probably be moved to a transform function when the function parameters can be parsed more effectively (handle templates)
  */
class TransformIterate extends TaskTransform {

  override def execute(): Unit = {

    dataSet = DataArray(dataSet.headOption.get.find(iterate).toList)
  }

  lazy val iterate = config.getString("iterate")
}
