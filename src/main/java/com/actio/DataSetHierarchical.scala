package com.actio

import java.io.InputStream
import java.sql.ResultSet
import java.util

/**
  * Created by mauri on 14/04/2016.
  */
class DataSetHierarchical(val dSObject: DSObject) extends DataSet {

  def size(): Int = 0

  def dump() = {}

  def set(_results: ResultSet): Unit = ???

  def set(_results: util.List[String]): Unit = ???

  def getNextBatch: DataSet = ???

  def getColumnHeader: util.List[String] = ???

  def getResultSet: ResultSet = ???

  def getAsListOfColumnsBatch(batchLen: Int): util.List[util.List[String]] = ???

  def getColumnHeaderStr: String = ???

  def isNextBatch = ???

  def GetRow(): Array[String] = ???

  def getAsList: util.List[String] = ???

  def setWithFields(_results: util.List[util.List[String]]): Unit = ???

  def NextRow(): Boolean = ???

  def FromRowGetField(rowIndex: Int, label: String): String = ???

  def FromRowGetField(rowIndex: Int, label: Int): String = ???

  def initBatch() = ???

  def getAsListOfColumns: util.List[util.List[String]] = ???


  override def header: List[String] = dSObject.fields.head.value match {
    case a: DSArray => a.arr.flatMap(f => f match {
      case v: DSObject => v.fields.map(_.name)
      case _ => Nil
    }).distinct
    case _ => Nil
  }

  override def rows: List[List[String]] = dSObject.fields.head.value match {
    case rs: DSArray => rs.arr.map(r => r match {
      case kv: DSObject => header.map(h => kv.fields.find(f => f.name == h).getOrElse(DSNothing) match {
        case v: DSStringValue => v.value
        case _ => null
      })
      case _ => Nil
    })
    case _ => Nil
  }
}


case class DSField(name: String, value: DSValue)
case class DSObject(fields: List[DSField]) extends DSValue
sealed abstract class DSValue
case class DSStringValue(value: String) extends DSValue
case class DSArray(arr: List[DSValue]) extends DSValue
case object DSNothing extends DSValue
case object DSNull extends DSValue
