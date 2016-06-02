package com.actio

import java.sql.ResultSet
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.util

import scala.util.Try

class DataSetTableScala(val myschema: SchemaDefinition, val data: Data) extends DataSet {
  def this() = this(SchemaUnknown, NoData())


  private var boolNext = true

  import scala.collection.JavaConverters._

  def sizeOfBatch = rows.length

  def getNextBatch: DataSet = this

  def getColumnHeader: util.List[String] = header.asJava

  def getAsListOfColumnsBatch(batchLen: Int): util.List[util.List[String]] = rows.map(_.asJava).asJava

  def getColumnHeaderStr: String = header mkString ","

  def getAsList: util.List[String] = rows.map(r => r.map(Option(_).getOrElse("")) mkString ",").asJava

  override def getAsListOfColumns(): util.List[util.List[String]] = rows.map(_.asJava).asJava

  override def toString = (header mkString ", ") + "\n" + ("-" * (header.map(_.length + 2).sum - 2)) + "\n" + (rows map (_ mkString ", ") mkString "\n") + "\n\n" + rows.length + " rows.\n"

  def next: Data = {
    boolNext = false
    data
  }

  def rows: List[List[String]] = data.values.map(_.values.map(_.valueOption.orNull).toList).toList
  def header: List[String] = schema.asInstanceOf[SchemaArray].content.asInstanceOf[SchemaRecord].fields.map(_.label).toList


  override def schema = myschema

  def hasNext = boolNext

  import scala.collection.JavaConverters._



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
  def apply(schema: SchemaDefinition, data: Data) = new DataSetTableScala(schema, data)

  def apply(text: String): DataSetTableScala = apply(List("col1"), List(List(text)))

  def apply(rows2: List[List[String]]): DataSetTableScala = apply(rows2.head.zipWithIndex map ("col" + _), rows2)

  def apply(header: List[String], rows: List[List[String]]): DataSetTableScala = new DataSetTableScala(
    SchemaArray(SchemaRecord(header.map(SchemaString(_,0)).toList)),
    DataArray(rows.map(r => DataRecord((header zip r).map(p => if (p._2 == null) NoData(p._1) else DataString(p._2, p._1)).toList)).toList))

  def apply(ds: DataSet): DataSetTableScala = new DataSetTableScala(ds.schema, ds.next)

  // explicit version with error handling
  def getTable(schema: SchemaDefinition, data: Data): Either[SchemaMatchError, DataSetTableScala] =
    schema match {
      case SchemaArray(_,SchemaRecord(_,fields)) => Try(DataSetTableScala(fields.map(_.label).toList, data.values.map(_.values.map(_.valueOption.orNull).toList).toList)).map(Right(_)).getOrElse(Left(DataDoesntMatchSchema))
      case SchemaArray(_,_) => Left(SchemaMatchRecordExpected)
      case _ => Left(SchemaMatchArrayExpected)
    }

  def inPredicate[T](list: List[T]) = (i: T) => list.contains(i)
}

