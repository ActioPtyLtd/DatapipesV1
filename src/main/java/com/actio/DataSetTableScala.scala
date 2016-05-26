package com.actio

import java.sql.ResultSet
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util

//import com.actio.DataSet

class DataSetTableScala(var header1: List[String], var rows1: List[List[String]]) extends DataSet {
  private var boolNext = true

  def this() = this(List(), List(List()))
  def this(rows: List[String]) = this(List(rows.head), rows.tail.map(List(_)))

  import scala.collection.JavaConverters._

  override def sizeOfBatch = rows1.length

  def set(_results: ResultSet) = ???

  def set(_results: util.List[String]) = ???

  def getNextBatch: DataSet = this

  def getColumnHeader: util.List[String] = header1.asJava

  def getAsListOfColumnsBatch(batchLen: Int): util.List[util.List[String]] = rows1.map(_.asJava).asJava

  def getResultSet: ResultSet = ???

  def getColumnHeaderStr: String = header mkString ","

  def hasNext: Boolean = {
    val bn = boolNext
    boolNext = false
    bn
  }

  def getAsList: util.List[String] = ???

  def setWithFields(_results: java.util.List[java.util.List[String]]) = { rows1 = _results.asScala.map(_.asScala.toList).toList}

  def initBatch: Unit = { }

  def FromRowGetField(rowIndex: Int, label: String): String = rows(rowIndex)(header.indexWhere(_ == label))

  def FromRowGetField(rowIndex: Int, label: Int): String = ???

  def getAsListOfColumns: util.List[util.List[String]] = rows1.map(_.asJava).asJava

  override def toString = (header1 mkString ", ") + "\n" + ("-" * (header1.map(_.length + 2).sum - 2)) + "\n" + (rows1 map (_ mkString ", ") mkString "\n") + "\n\n" + rows1.length + " rows.\n"

  override def next: Data = this.toData
}

object DataSetTableScala {
  def apply(text: String) = new DataSetTableScala(List("col1"), List(List(text)))

  def apply(rows2: List[List[String]]) = new DataSetTableScala(rows2.head.zipWithIndex map ("col" + _), rows2)

  def apply(header: List[String], rows: List[List[String]]) = new DataSetTableScala(header, rows)

  def apply(d: Data) = new DataSetTableScala(d("properties").asInstanceOf[DataRecord].fields.map(_.name).toList, d("data").values.map(_.asInstanceOf[DataRecord].fields.map(_.Data.asInstanceOf[DataString].str).toList).toList)

  def inPredicate[T](list: List[T]) = (i: T) => list.contains(i)
}


/*
trait TableScala {

  def transformSelectByOrdinal(columnOrdinals: List[Int]) = DataSetTableScala(columnOrdinals map (header(_)), rows map(r => columnOrdinals map(r(_))))

  def transformFilter(filter: List[String] => Boolean) = DataSetTableScala(header, rows filter filter)

*/
