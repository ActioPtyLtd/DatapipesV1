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

  def dropFunc(ds: DataSet, selectorFunc: String => Boolean) = keepFunc(ds, !selectorFunc(_))
  def drop(ds: DataSet, cols: List[String]): DataSet = dropFunc(ds, c => cols.contains(c))

  def addHeader(ds: DataSet, cols: List[String]) = DataSetTableScala(cols, ds.header :: ds.rows)

  def row1Header(ds: DataSet) = DataSetTableScala(ds.rows.head map(_.toString), ds.rows.tail)

  def split2ColsD(ds: DataSet, columnName: String, delim: String) = {
    val csvSplit = delim + "(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)" // TODO: should allow encapsulation to be paramaterised
    val numberOfNewCols = ds.getColumnValues(columnName).map(_.split(csvSplit, -1).length).max

    DataSetTableScala(ds.getNextAvailableColumnName(columnName, numberOfNewCols) ::: ds.header, ds.rows.map(r => ds.getValue(r, columnName).split(csvSplit, -1).padTo(numberOfNewCols, "").map(_.replaceAll("^\"|\"$", "")).toList ::: r)) /// map(_.replaceAll("^\"|\"$", ""))))
  }
  def split2Cols(ds: DataSet, columnName: String) = split2ColsD(ds, columnName, ",")

  def renameFunc(ds: DataSet, selectorFunc: String => Boolean, renameFunc: String => String) = DataSetTableScala(ds.header map(c => if(selectorFunc(c)) renameFunc(c) else c), ds.rows)
  def renamePair(ds: DataSet, colPairs: List[(String,String)]): DataSet = renameFunc(ds, c => colPairs.map(_._1).contains(c), r => colPairs.find(f => f._1 == r).get._2)
  def rename(ds: DataSet, cols: List[String]): DataSet = renamePair(ds, cols.grouped(2).map(m => (m.head, m.tail.headOption.getOrElse(ds.getNextAvailableColumnName(m.head)))).toList)

  def rowFunc(ds: DataSet, columnName: String, rowFunc: List[String] => String) = DataSetTableScala(columnName :: ds.header, ds.rows map(r => rowFunc(r) :: r))

  def sum(ds: DataSet, cols: List[String]) = rowFunc(ds, ds.getNextAvailableColumnName("sum"), r => (cols map (c => scala.math.BigDecimal(ds.getValue(r, c)))).sum.toString)

  def orderCols(ds: DataSet, cols: List[String]) = DataSetTableScala(cols, ds.rows map(r => cols map (ds.getValue(r, _))))

  def templateMerge(ds: DataSet, template: String) =
    DataSetTableScala(ds.getNextAvailableColumnName("template") :: ds.header, ds.rows.map(r => ds.header.foldLeft(template)((c,t) => t.replaceAll("@" + c, ds.getValue(r, c))) :: r))

  def prepare4statement(ds: DataSet, template: String) = orderCols(ds, "@(?<name>[a-zA-Z0-9]+)".r.findAllMatchIn(template).map(_.group(1)).toList)

  def changes(ds1: DataSet, ds2: DataSet, keyCols: List[String]) = DataSetTableScala(ds1.header, ds1.rows.filter(r => {
    val option = ds2.rows.find(ri => keyCols.forall(c => ds1.getValue(r, c) == ds2.getValue(ri, c)))
    if(option.isDefined)
      !ds1.header.forall(c => ds1.getValue(r,c) == ds2.getValue(option.get, c))
    else
      false
  }))

}
