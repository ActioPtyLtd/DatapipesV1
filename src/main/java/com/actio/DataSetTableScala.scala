package com.actio

import java.sql.ResultSet
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util

import scala.util.Try

class DataSetTableScala(val header1: List[String], val rows1: List[List[String]]) extends DataSet with SingleValueIterator {
  def this() = this(List(), List(List()))
  def this(rows: List[String]) = this(List(rows.head), rows.tail.map(List(_)))

  import scala.collection.JavaConverters._

  override def sizeOfBatch = rows1.length

  def set(_results: util.List[String]) = ???

  def getNextBatch: DataSet = this

  def getColumnHeader: util.List[String] = header1.asJava

  def getAsListOfColumnsBatch(batchLen: Int): util.List[util.List[String]] = rows1.map(_.asJava).asJava

  def getColumnHeaderStr: String = header mkString ","

  def getAsList: util.List[String] = ???

  def setWithFields(_results: java.util.List[java.util.List[String]]) = ??? //{ rows1 = _results.asScala.map(_.asScala.toList).toList}

  def initBatch: Unit = { }

  def getAsListOfColumns: util.List[util.List[String]] = rows1.map(_.asJava).asJava

  override def toString = (header1 mkString ", ") + "\n" + ("-" * (header1.map(_.length + 2).sum - 2)) + "\n" + (rows1 map (_ mkString ", ") mkString "\n") + "\n\n" + rows1.length + " rows.\n"

  def next: Data = DataArray(rows.map(r => DataRecord(header.map(h => DataField(h,DataString(this.getValue(r, h)).asInstanceOf[Data])).toList).asInstanceOf[Data]).toList)

  override def schema = SchemaArray(SchemaRecord(header1.map(h => new SchemaField(h, true, SchemaString(0))).toList))



  // moved from dataset

  import scala.collection.JavaConverters._

  def rows: List[List[String]] = getAsListOfColumns.asScala.map(_.asScala.toList).toList
  def header: List[String] = getColumnHeader.asScala.toList


  def getOrdinalOfColumn(columnName: String) = { val i = header.indexWhere(_ == columnName)
    if(i < 0) throw new Exception("Column " + columnName + " doesn't exist")
    i }

  def getOrdinalsWithPredicate(predicate: String => Boolean) = header.zipWithIndex filter(c => predicate(c._1)) map(_._2)

  def getColumnValues(columnName: String) = rows map(r => getValue(r, columnName))

  def getNextAvailableColumnName(columnName: String, n: Int) = {
    val pair = (columnName :: header) map(c => (c.replaceAll("\\d*$", ""), c.reverse takeWhile Character.isDigit match {
      case "" => 1
      case m => m.reverse.toInt + 1
    })) filter(_._1 == columnName.replaceAll("\\d*$", "")) maxBy(_._2)
    (pair._2 until (pair._2 + n)).map(pair._1 + _).toList
  }
  def getNextAvailableColumnName(columnName: String): String = getNextAvailableColumnName(columnName, 1).head

  def getValue(row: List[String], columnName: String) = row(getOrdinalOfColumn(columnName))

}

object DataSetTableScala {
  def apply(text: String) = new DataSetTableScala(List("col1"), List(List(text)))

  def apply(rows2: List[List[String]]) = new DataSetTableScala(rows2.head.zipWithIndex map ("col" + _), rows2)

  def apply(header: List[String], rows: List[List[String]]) = new DataSetTableScala(header, rows)

  def apply(schema: SchemaDefinition, data: Data) = new DataSetTableScala(schema.asInstanceOf[SchemaArray].content.asInstanceOf[SchemaRecord].fields.map(_.name).toList, data.values.map(_.asInstanceOf[DataRecord].fields.map(_.Data.asInstanceOf[DataString].str).toList).toList)

  // explicit version with error handling
  def getTable(schema: SchemaDefinition, data: Data): Either[SchemaMatchError, DataSetTableScala] =
    schema match {
      case SchemaArray(SchemaRecord(fields)) => Try(DataSetTableScala(fields.map(_.name).toList, data.values.map(_.asInstanceOf[DataRecord].fields.map(_.Data.asInstanceOf[DataString].str).toList).toList)).map(Right(_)).getOrElse(Left(DataDoesntMatchSchema))
      case SchemaArray(_) => Left(SchemaMatchRecordExpected)
      case _ => Left(SchemaMatchArrayExpected)
    }

  def inPredicate[T](list: List[T]) = (i: T) => list.contains(i)
}

