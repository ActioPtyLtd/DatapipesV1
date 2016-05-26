package com.actio

import java.time.LocalDate
import java.time.format.DateTimeFormatter

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
  def valueFunc(ds: DataSet, col: String, f: String => String) = rowFunc(ds, ds.getNextAvailableColumnName(col), r => f(ds.getValue(r, col)))

  def sum(ds: DataSet, cols: List[String]) = rowFunc(ds, ds.getNextAvailableColumnName("sum"), r => (cols map (c => scala.math.BigDecimal(ds.getValue(r, c)))).sum.toString)

  def orderCols(ds: DataSet, cols: List[String]) = DataSetTableScala(cols, ds.rows map(r => cols map (ds.getValue(r, _))))

  def templateMerge(ds: DataSet, template: String) =
    DataSetTableScala(ds.getNextAvailableColumnName("template") :: ds.header, ds.rows.map(r => ds.header.foldLeft(template)((c,t) => t.replaceAll("@" + c, ds.getValue(r, c))) :: r))

  def prepare4statement(ds: DataSet, template: String) = orderCols(ds, "@(?<name>[-_a-zA-Z0-9]+)".r.findAllMatchIn(template).map(_.group(1)).toList)

  def changes(ds1: DataSet, ds2: DataSet, keyCols: List[String]) = DataSetTableScala(ds1.header, ds1.rows.filter(r => {
    val option = ds2.rows.find(ri => keyCols.forall(c => ds1.getValue(r, c) == ds2.getValue(ri, c)))
    if(option.isDefined)
      !ds1.header.forall(c => {val equal = ds1.getValue(r,c) == ds2.getValue(option.get, c) //TODO string equality doesn't work nicely for numerics & dates
        if(!equal)
          printf(c + ":" + ds1.getValue(r,c)) //TODO remove side-effect. I needed this to test diffs
        equal})
    else
      false
  }))

  def newRows(ds1: DataSet, ds2: DataSet, keyCols: List[String]) = {
    val ds2KeysOnly = keep(ds2, keyCols)
    DataSetTableScala(ds1.header, ds1.rows.filterNot(r => ds2KeysOnly.rows.contains(keyCols.map(ds1.getValue(r,_)))))
  }

  def match2cols(ds: DataSet, columnName: String, regexes: List[String]) = DataSetTableScala(ds.getNextAvailableColumnName(columnName, regexes.length) ::: ds.header, ds.rows.map(r => {
    val matches = regexes map(_.r.findFirstMatchIn(ds.getValue(r, columnName)).isDefined)

    if(matches.contains(true))
      List.fill(matches.indexOf(true))("") ::: (ds.getValue(r,columnName) :: List.fill(matches.length - matches.indexOf(true) - 1)("")) ::: r
    else
      List.fill(matches.length)("") ::: r
  }))

  def concatFunc(ds: DataSet, selectorFunc: String => Boolean, delim: String = "") = rowFunc(ds, ds.getNextAvailableColumnName("concat"), r => ds.getOrdinalsWithPredicate(selectorFunc) map(r(_)) mkString delim)
  def concat(ds: DataSet, cols: List[String], delim: String): DataSet = rowFunc(ds, ds.getNextAvailableColumnName("concat"), r => cols map(ds.getValue(r,_)) mkString delim)

  def const(ds: DataSet, value: List[String]) = DataSetTableScala(ds.getNextAvailableColumnName("const", value.length) ::: ds.header, ds.rows map (value ::: _) ) //rowFunc(ds, ds.getNextAvailableColumnName("const"), _ => value)

  def mergeCols(ds1: DataSet, ds2: DataSet) = DataSetTableScala(ds1.header ::: ds2.header, (ds1.rows zip ds2.rows) map(r => r._2 ::: r._1))

  def convDateValue(value: String, in: String, out: String) =
    try {
      LocalDate.parse(value, DateTimeFormatter.ofPattern(in)).format(DateTimeFormatter.ofPattern(out))
    }
    catch {
      case _: Exception => "1900-01-01"
    }

  def convDate(ds: DataSet, col: String, in: String, out: String) = valueFunc(ds, col, convDateValue(_, in, out))

  def defaultIfBlankValue(value: String, default: String) = if(value == null || value.trim.isEmpty) default else value
  def defaultIfBlank(ds: DataSet, cols: List[String], default: String) = cols.foldLeft(ds)((d,c) => valueFunc(d, c, defaultIfBlankValue(_,default)))

  def filterIfNotBlank(ds: DataSet, cols: List[String]) = DataSetTableScala(ds.header, ds.rows filter (r => cols.forall(!ds.getValue(r,_).trim.isEmpty)))

  def transformLookupFunc(ds1: DataSet, ds2: DataSet, condition: (List[String],List[String]) => Boolean, lookupSelectorFunc: String => Boolean) =
    DataSetTableScala((ds2.header.filter(lookupSelectorFunc) map ds1.getNextAvailableColumnName) ::: ds1.header,
      ds1.rows.map(r1 => ds2.rows.find(condition(r1,_)).getOrElse(ds2.getEmptyRow).zipWithIndex.filter(f => ds2.getOrdinalsWithPredicate(lookupSelectorFunc).contains(f._2)).map(_._1) ::: r1))

  def trimValue(value: String) = value.trim
  def trim(ds: DataSet, cols: List[String]) = cols.foldLeft(ds)((d,c) => valueFunc(d, c, trimValue))

  def mapOrElseValue(value: String, colPairs: Map[String,String], orElse: String) =  colPairs.getOrElse(value, orElse)
  def mapOrElse(ds: DataSet, col: String, colPairs: List[String], orElse: String) = {
    val pairMap = colPairs.grouped(2).map(g => (g.head, g.tail.headOption.getOrElse(""))).toMap
    valueFunc(ds, col, v => mapOrElseValue(v, pairMap, orElse))
  }

  def jsonObject(ds: DataSet, cols: List[String]) = rowFunc(ds, ds.getNextAvailableColumnName("json"), r => "{" + (cols filter (f => ds.getValue(r,f) !=null && ds.getValue(r,f).nonEmpty) map (c => "\"" + c + "\": \"" + ds.getValue(r, c) + "\"") mkString ",") + "}")

  def copy(ds: DataSet, from: String, to: List[String]) = DataSetTableScala(to ::: ds.header, ds.rows map (r => List.fill(to.size)(ds.getValue(r, from) ) ::: r))

}