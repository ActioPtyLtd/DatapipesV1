package com.actio

import java.sql.ResultSet
import java.util

//import com.actio.DataSet

class DataSetTableScala(var header: List[String], var rows: List[List[String]]) extends DataSet {
  var boolNext = false

  def this() = this(List(), List(List()))
  def this(rows: List[String]) = this(List("col1"), rows.map(List(_)))

  import scala.collection.JavaConverters._

  def size() = rows.length

  def dump() = {}

  def set(_results: ResultSet) = ???

  def set(_results: util.List[String]) = ???

  def getNextBatch: DataSet = this

  def getColumnHeader: util.List[String] = header.asJava

  def getAsListOfColumnsBatch(batchLen: Int): util.List[util.List[String]] = rows.map(_.asJava).asJava

  def getResultSet: ResultSet = ???

  def getColumnHeaderStr: String = header mkString ","

  def isNextBatch: Boolean = {
    boolNext = !boolNext
    boolNext
  }

  def GetRow(): Array[String] = rows.head.toArray

  def getAsList: util.List[String] = ???

  def setWithFields(_results: java.util.List[java.util.List[String]]) = { rows = _results.asScala.map(_.asScala.toList).toList}

  def NextRow(): Boolean = false

  def initBatch(): Unit = { }

  def FromRowGetField(rowIndex: Int, label: String): String = rows(rowIndex)(header.indexWhere(_ == label))

  def FromRowGetField(rowIndex: Int, label: Int): String = ???

  def getAsListOfColumns: util.List[util.List[String]] = rows.map(_.asJava).asJava
}

case class TableScala(header: List[String], rows: List[List[String]]) {

  def getOrdinalOfColumn(columnName: String) = header.indexWhere(_ == columnName)

  def getOrdinalsWithPredicate(predicate: String => Boolean) = header.zipWithIndex.filter(c => predicate(c._1)).map(_._2)

  def getColumnValues(columnName: String) = rows.map(r => getValue(r, columnName))

  def getNumberOfColumns = header.length

  def getEmptyRow = List.fill(getNumberOfColumns)(null)

  def getValue(row: List[String], columnName: String) = row(getOrdinalOfColumn(columnName))


  def transformFirstRowAsHeader = TableScala(rows.head.map(_.toString), rows.tail)

  def transformWithRowFunction(columnName: String, rowFunc: List[String] => String) = TableScala(columnName :: header, rows.map(r => rowFunc(r) :: r))

  def transformWithRowFunction(selectorFunc : String => Boolean, columnRenameFunc: String => String, valueFunc : String => String) = TableScala(header ::: header.filter(selectorFunc).map(columnRenameFunc), rows.map(r => r ::: getOrdinalsWithPredicate(selectorFunc).map(o => valueFunc(r(o)))))

  def transformToColumnsWithDelimiter(delim: String = ",") = TableScala(header.head.split(delim).toList, rows.map(_.head.split(delim).toList))

  def transformToRowsWithDelimiter(delim: String = ";") = TableScala(header, rows.head.head.split(delim).map(List(_)).toList)

  def transformCombineColumnsWithOtherTable(t2: TableScala, pos: Int) = TableScala(header.take(pos) ::: t2.header ::: header.drop(pos), (rows zip t2.rows).map(r => r._1.take(pos) ::: r._2 ::: r._1.drop(pos)))

  def transformUnion(t2: TableScala) = new TableScala(header, rows ::: t2.rows)

  def transformSelect(selectorFunc: String => Boolean) = TableScala(header.filter(c => selectorFunc(c)), rows.map(r => getOrdinalsWithPredicate(selectorFunc).map(c => r(c))))

  def transformSelect(columnOrdinals: List[Int]) = TableScala(columnOrdinals.map(header(_)), rows.map(r => columnOrdinals.map(c => r(c))))

  def transformFilter(filter: List[String] => Boolean) = TableScala(header, rows.filter(filter))

  def transformRename(selectorFunc: String => Boolean, renameFunc: String => String) = TableScala(header.map(c => if(selectorFunc(c)) renameFunc(c) else c), rows)

  def transformAddConstant(columnName : String, value: String) = transformWithRowFunction(columnName, _ => value)

  def transformDrop(selectorFunc: String => Boolean) = transformSelect(!selectorFunc(_))

  def transformConcat(columnName: String, selectorFunc: String => Boolean, delim: String = "") = transformWithRowFunction(columnName, r => getOrdinalsWithPredicate(selectorFunc).map(c => r(c)).mkString(delim) )

  def transformDiffNew(t2: TableScala, keySelectorFunc: String => Boolean) = TableScala(header, rows.filter(r => t2.rows.map(r2 => getOrdinalsWithPredicate(keySelectorFunc).map(c => r2(c))).contains(getOrdinalsWithPredicate(keySelectorFunc).map(r(_)))))

  def transformLookup(t2: TableScala, condition: (List[String],List[String]) => Boolean, lookupSelectorFunc: String => Boolean) = new TableScala(header ::: t2.header.filter(lookupSelectorFunc),
    rows.map(r1 => r1 :::
      t2.rows.find(condition(r1,_)).getOrElse(t2.getEmptyRow).zipWithIndex.filter(f => t2.getOrdinalsWithPredicate(lookupSelectorFunc).contains(f._2)).map(_._1)))

  def transformAll(transforms: List[TableScala => TableScala]) = transforms.foldLeft(this)((a,b) => b(a))


  // DataSet implementation


}




object TableScala {
  def apply(text: String) = new TableScala(List("col1"), List(List(text)))
  //def apply(rows1: List[String]) = new TableScala(List("col1"), rows1.map(List(_)))

  def apply(rows2: List[List[String]]) = new TableScala(rows2.head.zipWithIndex.map("col" + _), rows2)

//  def apply(dataSet: DataSet): TableScala = dataSet match {
//    case dataSet: TableScala => dataSet
//    case _ => TableScala(dataSet.getColumnHeader.asScala.toList, dataSet.getAsListOfColumns().asScala.map(_.asScala.toList).toList)
//  }

  def union(tables: List[TableScala]) = new TableScala(tables.head.header, tables.flatMap(t => t.rows))

  def inPredicate[T](list: List[T]) = (i: T) => list.contains(i)

}

object ScalaTest extends App {
  val t1 = TableScala("A,B,C;1,2,3;4,5,6").transformToRowsWithDelimiter().transformFirstRowAsHeader.transformToColumnsWithDelimiter().transformAddConstant("D", "9")
  println(t1)

  val t2 = TableScala("B;5;2;2;1").transformToRowsWithDelimiter().transformFirstRowAsHeader.transformToColumnsWithDelimiter()
  println(t2)

  val t3 = t2.transformLookup(t1, (r2,r1) => t2.getValue(r2, "B") == t1.getValue(r1, "B"), c => c == "C")
  println(t3)

  val t4 = t3.transformDrop(c => c == "B")
  println(t4)
}
