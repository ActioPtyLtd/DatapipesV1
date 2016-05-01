package com.actio

import java.lang.reflect.Method
import java.lang.reflect.Parameter
import com.actio.dpsystem.Logging
import scala.annotation.tailrec
import scala.collection.JavaConverters._

/**
  * Created by mauri on 27/04/2016.
  */

object UtilityFunctions extends Logging {

  def execute(ds: DataSet, fs: java.util.List[TransformFunction]): DataSet =
    fs.asScala.foldLeft[DataSet](ds)((s,f) => execute(f.getName, (s.asInstanceOf[Any] :: f.getParameters.toList).asJava))

  def execute(methodName: String, params: java.util.List[Any]) = {
    val method = Class.forName("com.actio.DataSetTransforms").getDeclaredMethods.find(_.getName.equalsIgnoreCase(methodName))

    if(method.isDefined)
      // assume return one data set right now
      method.get.invoke(null, getParamValues(method.get.getParameters, params) map (_.asInstanceOf[Object]) : _*).asInstanceOf[DataSet]
    else
      new DataSetTableScala() // returns an empty dataset if we cant find the method name
  }

  def getParamValues(methodParams: Array[Parameter], paramValues: java.util.List[Any]): List[Any] =
    getParamValues(methodParams.toList, paramValues.asScala.toList, Nil)

  @tailrec
  def getParamValues(methodParams: List[Parameter], paramValues: List[Any], result: List[Any]): List[Any] = {
    paramValues match {
      case Nil => result.reverse ::: List.fill(methodParams.length)(null)
      case pv => methodParams match {
        case Nil => result.reverse
        case h :: t =>
          if (h.getType == classOf[String] || h.getType == classOf[DataSet])
            getParamValues(t, pv.tail, pv.head :: result)
          else if (h.getType == classOf[List[String]]) {
            val l = pv.splitAt(pv.length - methodParams.length + 1)
            getParamValues(t, l._2, l._1 :: result)
          }
          else
            getParamValues(t, pv.tail, null :: result)
      }
    }
  }
}
