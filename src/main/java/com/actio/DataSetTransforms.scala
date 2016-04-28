package com.actio

/**
  * Created by mauri on 27/04/2016.
  */
object DataSetTransforms {

  def split2Rows(ds: DataSet, columnName: String, regex: String) =
    DataSetTableScala(ds.getNextAvailableColumnName(columnName) :: ds.header,
      ds.rows flatMap(r => r(ds.getOrdinalOfColumn(columnName)) split regex map (_ :: r )))

  def keepFunc(ds: DataSet, selectorFunc: String => Boolean) = DataSetTableScala(ds.header filter selectorFunc, ds.rows map(r => ds.getOrdinalsWithPredicate(selectorFunc) map(r(_))))
  def keep(ds: DataSet, cols: List[String]): DataSet = keepFunc(ds, c => cols.contains(c))
  def keepRegex(ds: DataSet, regex: String): DataSet = keepFunc(ds, c => regex.matches(c))

  def sumCols(ds: DataSet, arg1: String, arg2: String, rest: List[String]): DataSet = ds

  def addHeader(ds: DataSet, cols: List[String]) = DataSetTableScala(cols, ds.header :: ds.rows)

  def row1Header(ds: DataSet) = DataSetTableScala(ds.rows.head map(_.toString), ds.rows.tail)

  def splitToCols(ds: DataSet, columnName: String, delim: String = ",") = {
    val csvSplit = delim + "(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)" // TODO: should allow encapsulation to be paramaterised
    val numberOfNewCols = ds.getColumnValues(columnName).map(_.split(csvSplit, -1).length).max

    DataSetTableScala(ds.getNextAvailableColumnName(columnName, numberOfNewCols) ::: ds.header, ds.rows.map(r => ds.getValue(r, columnName).split(csvSplit, -1).padTo(numberOfNewCols, "").toList ::: r)) /// map(_.replaceAll("^\"|\"$", ""))))
  }

  def renameFunc(ds: DataSet, selectorFunc: String => Boolean, renameFunc: String => String) = DataSetTableScala(ds.header map(c => if(selectorFunc(c)) renameFunc(c) else c), ds.rows)
  def renamePair(ds: DataSet, colPairs: List[(String,String)]): DataSet = renameFunc(ds, c => colPairs.map(_._1).contains(c), r => colPairs.find(f => f._1 == r).get._2)
  def rename(ds: DataSet, cols: List[String]): DataSet = renamePair(ds, cols.grouped(2).map(m => (m.head, m.tail.headOption.getOrElse(ds.getNextAvailableColumnName(m.head)))).toList)


}
