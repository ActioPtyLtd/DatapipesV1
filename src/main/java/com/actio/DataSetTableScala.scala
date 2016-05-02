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

  def size() = rows1.length

  def dump() = {}

  def set(_results: ResultSet) = ???

  def set(_results: util.List[String]) = ???

  def getNextBatch: DataSet = this

  def getColumnHeader: util.List[String] = header1.asJava

  def getAsListOfColumnsBatch(batchLen: Int): util.List[util.List[String]] = rows1.map(_.asJava).asJava

  def getResultSet: ResultSet = ???

  def getColumnHeaderStr: String = header mkString ","

  def isNextBatch: Boolean = {
    val bn = boolNext
    boolNext = false
    bn
  }

  def GetRow(): Array[String] = rows.head.toArray

  def getAsList: util.List[String] = ???

  def setWithFields(_results: java.util.List[java.util.List[String]]) = { rows1 = _results.asScala.map(_.asScala.toList).toList}

  def NextRow(): Boolean = false

  def initBatch(): Unit = { }

  def FromRowGetField(rowIndex: Int, label: String): String = rows(rowIndex)(header.indexWhere(_ == label))

  def FromRowGetField(rowIndex: Int, label: Int): String = ???

  def getAsListOfColumns: util.List[util.List[String]] = rows1.map(_.asJava).asJava

  override def toString = (header1 mkString ", ") + "\n" + ("-" * (header1.map(_.length + 2).sum - 2)) + "\n" + (rows1 map (_ mkString ", ") mkString "\n") + "\n\n" + rows1.length + " rows.\n"
}
/*
trait TableScala {


  def transformRowsOnColumn(columnName: String, valFunc: String => String, default: String) = transformWithRowFunction(getNextAvailableColumnName(columnName), v => try { valFunc(getValue(v, columnName)) } catch { case _: Exception => default })
  def transformRowsDateConvert(columnName: String, in: String, out: String, default: String) = transformRowsOnColumn(columnName, p => LocalDate.parse(p, DateTimeFormatter.ofPattern(in)).format(DateTimeFormatter.ofPattern(out)), default)



  def transformWithRowFunction(selectorFunc : String => Boolean, columnRenameFunc: String => String, valueFunc : String => String) =  DataSetTableScala(header ::: (header filter selectorFunc map columnRenameFunc), rows map(r => r ::: getOrdinalsWithPredicate(selectorFunc).map(o => valueFunc(r(o)))))



  def transformSplitToRows(delim: String = ";") =  DataSetTableScala(header, rows.head.head.split(delim).map(List(_)).toList)
  def transformSplitToRows(f: TransformFunction): DataSet = transformSplitToRows(f.getParameters.head)

  def transformCombineColumnsWithOtherTable(t2: TableScala) = DataSetTableScala(t2.header ::: header, (rows zip t2.rows) map(r => r._2 ::: r._1))

  def transformUnion(t2: TableScala) = DataSetTableScala(header, rows ::: t2.rows)

  def transformSelect(selectorFunc: String => Boolean) = DataSetTableScala(header filter selectorFunc, rows.map(r => getOrdinalsWithPredicate(selectorFunc) map(r(_))))
  def transformSelect(f: TransformFunction): DataSet = transformSelect(c => f.getParameters.contains(c))
  def transformSelectRegex(f: TransformFunction): DataSet = transformSelect(c => f.getParameters.head.matches(c))

  def transformSelectByOrdinal(columnOrdinals: List[Int]) = DataSetTableScala(columnOrdinals map (header(_)), rows map(r => columnOrdinals map(r(_))))
  def transformSelectByOrdinal(f: TransformFunction): DataSet = transformSelectByOrdinal(f.getParameters.map(_.toInt).toList)

  def transformFilter(filter: List[String] => Boolean) = DataSetTableScala(header, rows filter filter)

  def transformAddConstant(value: String) = transformWithRowFunction(getNextAvailableColumnName("const"), _ => value)
  def transformAddConstant(f: TransformFunction): DataSet = transformAddConstant(f.getParameters.head)

  def transformDrop(selectorFunc: String => Boolean) = transformSelect(!selectorFunc(_))
  def transformDrop(f: TransformFunction): DataSet = transformDrop(c => f.getParameters.head.matches(c))

  def transformConcat(selectorFunc: String => Boolean, delim: String = "") = transformWithRowFunction(getNextAvailableColumnName("concat"), r => getOrdinalsWithPredicate(selectorFunc) map(r(_)) mkString delim)
  def transformConcat(cols: List[String], delim: String): TableScala = transformConcat(c => cols.contains(c), delim)
  def transformConcat(f: TransformFunction): DataSet = transformConcat(c => f.getParameters.head.matches(c), f.getParameters.tail.headOption.getOrElse(""))

  def transformDiffNew(t2: TableScala, keySelectorFunc: String => Boolean) = DataSetTableScala(header, rows.filter(r => !t2.rows.map(r2 => t2.getOrdinalsWithPredicate(keySelectorFunc).map(c => r2(c))).contains(getOrdinalsWithPredicate(keySelectorFunc).map(r(_)))))

  def transformLookup(t2: TableScala, condition: (List[String],List[String]) => Boolean, lookupSelectorFunc: String => Boolean) = DataSetTableScala(header ::: t2.header.filter(lookupSelectorFunc),
    rows.map(r1 => r1 ::: t2.rows.find(condition(r1,_)).getOrElse(t2.getEmptyRow).zipWithIndex.filter(f => t2.getOrdinalsWithPredicate(lookupSelectorFunc).contains(f._2)).map(_._1)))

  def transformAll(transforms: List[TableScala => TableScala]) = transforms.foldLeft(this)((a,b) => b(a))

  def transformRearrangeTo(t2: TableScala) = rows map(r => (t2.header.map(getOrdinalOfColumn).toList ::: (header.toSet -- t2.header.toSet).map(t2.getOrdinalOfColumn).toList ) map(r(_)))

  // not a table right now
  def transformDiff(t2: TableScala, condition: (List[String],List[String]) => Boolean) = rows map(r1 => (r1,t2.rows.find(condition(r1,_)))) filter(_._2.isDefined) map(m => header zip m._1 zip m._2.get filterNot(f => f._1._2 == f._2) map(_._1))

  def transformMatchToColumns(columnName: String, regexes: List[String]) = DataSetTableScala(getNextAvailableColumnName(columnName, regexes.length) ::: header, rows.map(r => {
    val matches = regexes map(_.r.findFirstMatchIn(r(getOrdinalOfColumn(columnName))).isDefined)

    if(matches.contains(true))
      List.fill(matches.indexOf(true))("") ::: (r(getOrdinalOfColumn(columnName)) :: List.fill(matches.length - matches.indexOf(true) - 1)("")) ::: r
    else
      List.fill(matches.length)("") ::: r
  }))
  def transformMatchToColumns(f: TransformFunction): DataSet = transformMatchToColumns(f.getParameters.head, f.getParameters.tail.toList)

*/


object DataSetTableScala {
  def apply(text: String) = new DataSetTableScala(List("col1"), List(List(text)))
  //def apply(rows1: List[String]) = new TableScala(List("col1"), rows1.map(List(_)))

  def apply(rows2: List[List[String]]) = new DataSetTableScala(rows2.head.zipWithIndex map ("col" + _), rows2)

  def apply(header: List[String], rows: List[List[String]]) = new DataSetTableScala(header, rows)

//  def apply(dataSet: DataSet): TableScala = dataSet match {
//    case dataSet: TableScala => dataSet
//    case _ => TableScala(dataSet.getColumnHeader.asScala.toList, dataSet.getAsListOfColumns().asScala.map(_.asScala.toList).toList)
//  }

  //def union(tables: List[TableScala]) = new DataSetTableScala(tables.head.header, tables flatMap (_.rows))

  def inPredicate[T](list: List[T]) = (i: T) => list.contains(i)

  import scala.language.implicitConversions
  import scala.collection.JavaConverters._

  implicit def toDataSetTableScala(dataSet : DataSet): DataSetTableScala = DataSetTableScala(dataSet.header, dataSet.rows)

}
/*
object ScalaTest extends App {
  val t1 = DataSetTableScala("A,B,C;1,2,3;4,5,6").transformSplitToRows().transformSplitToColumns("col1").transformFirstRowAsHeader //.transformAddConstant("D", "9")
  println(t1)

  val t2 = DataSetTableScala("B;5;2;2;1").transformSplitToRows().transformFirstRowAsHeader
  println(t2)

  val t3 = t2.transformLookup(t1, (r2,r1) => t2.getValue(r2, "B") == t1.getValue(r1, "B"), c => c == "C")
  println(t3)

  val t4 = t3.transformDrop(c => c == "B")
  println(t4)


  val t5 = DataSetTableScala("A,B,C;1,9,3;4,5,9").transformSplitToRows().transformSplitToColumns("col1").transformFirstRowAsHeader
  //val t6 = t5.transformDiff(t1, (r2,r1) => t5.getValue(r2,"A") == t1.getValue(r1, "A"))

  //println(t6)

  val t7 = DataSetTableScala("A;19/04/1984;2016-04-25").transformSplitToRows().transformFirstRowAsHeader.transformMatchToColumns("A", List("/","-"))
  println(t7.transformRowsDateConvert("A1", "dd/MM/yyyy", "yyyy-MM-dd", "").transformConcat(List("A3","A2"), ""))

  println(t5.transformRename(List(("B","D"))))
}
 */