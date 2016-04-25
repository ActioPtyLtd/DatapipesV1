package com.actio

import java.sql.ResultSet
import java.util

//import com.actio.DataSet

class DataSetTableScala(var header: List[String], var rows: List[List[String]]) extends DataSet with TableScala {
  private var boolNext = false

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

  override def toString = (header mkString ", ") + "\n" + ("-" * (header.map(_.length + 2).sum - 2)) + "\n" + (rows map (_ mkString ", ") mkString "\n") + "\n\n" + rows.length + " rows.\n"
}

trait TableScala {

  def header: List[String]
  def rows: List[List[String]]

  def getOrdinalOfColumn(columnName: String) = header.indexWhere(_ == columnName)

  def getOrdinalsWithPredicate(predicate: String => Boolean) = header.zipWithIndex filter(c => predicate(c._1)) map(_._2)

  def getColumnValues(columnName: String) = rows map(r => getValue(r, columnName))

  def getNextAvailableColumnName(columnName: String, n: Int) = {
    val pair = (columnName :: header) map(c => (c.replaceAll("\\d*$", ""), c.reverse takeWhile Character.isDigit match {
      case "" => 1
      case m => m.reverse.toInt + 1
    })) filter(_._1 == columnName.replaceAll("\\d*$", "")) maxBy(_._2)
    (pair._2 to (pair._2 + n)).map(pair._1 + _).toList
  }
  def getNextAvailableColumnName(columnName: String): String = getNextAvailableColumnName(columnName, 1).head


  def getNumberOfColumns = header.length

  def getEmptyRow = List.fill(getNumberOfColumns)(null)

  def getValue(row: List[String], columnName: String) = row(getOrdinalOfColumn(columnName))


  def transformFirstRowAsHeader = DataSetTableScala(rows.head map(_.toString), rows.tail)

  def transformWithRowFunction(columnName: String, rowFunc: List[String] => String) = DataSetTableScala(columnName :: header, rows map(r => rowFunc(r) :: r))

  def transformWithRowFunction(selectorFunc : String => Boolean, columnRenameFunc: String => String, valueFunc : String => String) =  DataSetTableScala(header ::: (header filter selectorFunc map columnRenameFunc), rows map(r => r ::: getOrdinalsWithPredicate(selectorFunc).map(o => valueFunc(r(o)))))

  def transformToColumnsWithDelimiter(delim: String = ",") =  DataSetTableScala(header.head.split(delim).toList, rows map(_.head.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)", -1).toList map(_.replaceAll("^\"|\"$", ""))))

  def transformToRowsWithDelimiter(delim: String = ";") =  DataSetTableScala(header, rows.head.head.split(delim).map(List(_)).toList)

  def transformCombineColumnsWithOtherTable(t2: TableScala, pos: Int) = DataSetTableScala(header.take(pos) ::: t2.header ::: header.drop(pos), (rows zip t2.rows) map(r => r._1.take(pos) ::: r._2 ::: r._1.drop(pos)))

  def transformUnion(t2: TableScala) = DataSetTableScala(header, rows ::: t2.rows)

  def transformSelect(selectorFunc: String => Boolean) = DataSetTableScala(header filter selectorFunc, rows.map(r => getOrdinalsWithPredicate(selectorFunc) map(r(_))))

  def transformSelect(columnOrdinals: List[Int]) = DataSetTableScala(columnOrdinals map (header(_)), rows map(r => columnOrdinals map(r(_))))

  def transformFilter(filter: List[String] => Boolean) = DataSetTableScala(header, rows filter filter)

  def transformRename(selectorFunc: String => Boolean, renameFunc: String => String) = DataSetTableScala(header map(c => if(selectorFunc(c)) renameFunc(c) else c), rows)

  def transformAddConstant(value: String) = transformWithRowFunction(getNextAvailableColumnName("const"), _ => value)

  def transformDrop(selectorFunc: String => Boolean) = transformSelect(!selectorFunc(_))

  def transformConcat(selectorFunc: String => Boolean, delim: String = "") = transformWithRowFunction(getNextAvailableColumnName("concat"), r => getOrdinalsWithPredicate(selectorFunc) map(r(_)) mkString delim)

  def transformDiffNew(t2: TableScala, keySelectorFunc: String => Boolean) = DataSetTableScala(header, rows.filter(r => !t2.rows.map(r2 => t2.getOrdinalsWithPredicate(keySelectorFunc).map(c => r2(c))).contains(getOrdinalsWithPredicate(keySelectorFunc).map(r(_)))))

  def transformLookup(t2: TableScala, condition: (List[String],List[String]) => Boolean, lookupSelectorFunc: String => Boolean) = DataSetTableScala(header ::: t2.header.filter(lookupSelectorFunc),
    rows.map(r1 => r1 ::: t2.rows.find(condition(r1,_)).getOrElse(t2.getEmptyRow).zipWithIndex.filter(f => t2.getOrdinalsWithPredicate(lookupSelectorFunc).contains(f._2)).map(_._1)))

  def transformAll(transforms: List[TableScala => TableScala]) = transforms.foldLeft(this)((a,b) => b(a))

  def transformRearrangeTo(t2: TableScala) = rows map(r => (t2.header.map(getOrdinalOfColumn).toList ::: (header.toSet -- t2.header.toSet).map(t2.getOrdinalOfColumn).toList ) map(r(_)))

  // not a table right now
  def transformDiff(t2: TableScala, condition: (List[String],List[String]) => Boolean) = rows map(r1 => (r1,t2.rows.find(condition(r1,_)))) filter(_._2.isDefined) map(m => header zip m._1 zip m._2.get filterNot(f => f._1._2 == f._2) map(_._1))

  def transformSplitToRows(columnName: String, regex: String) = DataSetTableScala(getNextAvailableColumnName(columnName) :: header, rows flatMap(r => r(getOrdinalOfColumn(columnName)) split regex map (_ :: r )))

  def transformMatchToColumns(columnName: String, regexes: List[String]) = DataSetTableScala(getNextAvailableColumnName(columnName, regexes.length) ::: header, rows.map(r => {
    val matches = regexes map(_.r.findFirstMatchIn(r(getOrdinalOfColumn(columnName))).isDefined)

    if(matches.contains(true))
      List.fill(matches.indexOf(true))("") ::: (r(getOrdinalOfColumn(columnName)) :: List.fill(matches.length - matches.indexOf(true) - 1)("")) ::: r
    else
      List.fill(matches.length)("") ::: r
  }))

}




object DataSetTableScala {
  def apply(text: String) = new DataSetTableScala(List("col1"), List(List(text)))
  //def apply(rows1: List[String]) = new TableScala(List("col1"), rows1.map(List(_)))

  def apply(rows2: List[List[String]]) = new DataSetTableScala(rows2.head.zipWithIndex map ("col" + _), rows2)

  def apply(header: List[String], rows: List[List[String]]) = new DataSetTableScala(header, rows)

//  def apply(dataSet: DataSet): TableScala = dataSet match {
//    case dataSet: TableScala => dataSet
//    case _ => TableScala(dataSet.getColumnHeader.asScala.toList, dataSet.getAsListOfColumns().asScala.map(_.asScala.toList).toList)
//  }

  def union(tables: List[TableScala]) = new DataSetTableScala(tables.head.header, tables flatMap (_.rows))

  def inPredicate[T](list: List[T]) = (i: T) => list.contains(i)

  import scala.language.implicitConversions
  import scala.collection.JavaConverters._

  implicit def toDataSetTableScala(dataSet : DataSet): DataSetTableScala = DataSetTableScala(dataSet.getColumnHeader.asScala.toList, dataSet.getAsListOfColumns.asScala.map(_.asScala.toList).toList)

}

object ScalaTest extends App {
  val t1 = DataSetTableScala("A,B,C;1,2,3;4,5,6").transformToRowsWithDelimiter().transformFirstRowAsHeader.transformToColumnsWithDelimiter() //.transformAddConstant("D", "9")
  println(t1)

  val t2 = DataSetTableScala("B;5;2;2;1").transformToRowsWithDelimiter().transformFirstRowAsHeader.transformToColumnsWithDelimiter()
  println(t2)

  val t3 = t2.transformLookup(t1, (r2,r1) => t2.getValue(r2, "B") == t1.getValue(r1, "B"), c => c == "C")
  println(t3)

  val t4 = t3.transformDrop(c => c == "B")
  println(t4)


  val t5 = DataSetTableScala("A,B,C;1,9,3;4,5,9").transformToRowsWithDelimiter().transformFirstRowAsHeader.transformToColumnsWithDelimiter()
  //val t6 = t5.transformDiff(t1, (r2,r1) => t5.getValue(r2,"A") == t1.getValue(r1, "A"))

  //println(t6)

  val t7 = DataSetTableScala("A;19/4/84;2016-04-25").transformToRowsWithDelimiter().transformFirstRowAsHeader.transformToColumnsWithDelimiter()
  println(t7.transformMatchToColumns("A", List("/","-")))
}
