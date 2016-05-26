package com.actio


import java.sql.ResultSet
import java.util

/**
  * Created by mauri on 14/04/2016.
  */
class DataSetHierarchical(val dSObject: DSObject) extends DataSet {

  override def sizeOfBatch(): Int = 0

  def set(_results: util.List[String]): Unit = ???

  def getNextBatch: DataSet = ???

  def getColumnHeader: util.List[String] = ???

  def getAsListOfColumnsBatch(batchLen: Int): util.List[util.List[String]] = ???

  def getColumnHeaderStr: String = ???

  def hasNext = ???

  def getAsList: util.List[String] = ???

  def setWithFields(_results: util.List[util.List[String]]): Unit = ???

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

  override def next: Data = ???
}

/*

TODO: These need to map into RECORD, FIELD => Record === to a row, but allows for Hiearachical records

-- a record has fields.
-- fields are addressable by their ordinal position with a record, or by their label

-- fields can be records
-- a hierarchy of fields within records can be addressed by a 'dot' notation.


 */

case class DSField(name: String, value: DSValue)
case class DSObject(fields: List[DSField]) extends DSValue
sealed abstract class DSValue
case class DSStringValue(value: String) extends DSValue
case class DSArray(arr: List[DSValue]) extends DSValue
case object DSNothing extends DSValue
case object DSNull extends DSValue
