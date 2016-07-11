package com.actio

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.util.Try

/**
  * Created by mauri on 27/04/2016.
  */

object DataSetTransforms {

  def filterValue(ds: DataSet, property: String, value: String): DataSet = DataArray(ds.elems.filter(f => f(property).stringOption.getOrElse("") == value).toList)

  def firstValue(ds: DataSet, property: String, value: String): DataSet = ds.elems.find(f => f.value(property).stringOption.getOrElse("") == value).getOrElse(Nothin())

  def convertDateFormat(ds: DataSet, in: String, out: String): DataSet = DataString(convDateValue(ds.stringOption.getOrElse(""), in, out))

  def ifNotBlankOrElse(ds: DataSet, other: String): DataSet = ds.stringOption.map(s => if (s.trim().isEmpty) DataString(other) else DataString(s)).getOrElse(DataString(other))

  def concatString(ds: DataSet): DataSet = DataString(ds.elems.map(_.stringOption.getOrElse("")).mkString(","))

  def ifEqualOrElse(ds: DataSet, equal: String, dsThen: DataSet, dsElse: DataSet): DataSet = if (ds.stringOption.getOrElse("") == equal) dsThen else dsElse


  /* below will need to be replaced when I have time */


  //TODO: think about error handling. Maybe change next: Either[Error, Data]
  def transformEachData(schemaFunc: (SchemaDefinition => SchemaDefinition), dataFunc: (DataSet => DataSet)): (DataSet => DataSet) = (ds: DataSet) =>
    new DataSet {
      override def elems = ds.elems map dataFunc

      override lazy val schema = schemaFunc(ds.schema)

      override def label: String = ""
    }

  def transformEachDataRecord(schemaFunc: (SchemaDefinition => SchemaDefinition), dataRecordFunc: (DataRecord) => (DataRecord)): (DataSet => DataSet) =
    transformEachData(schemaFunc, (d: DataSet) => DataArray(d.elems.map(r => dataRecordFunc(r.asInstanceOf[DataRecord])).toList))

  def transformDataSets(dsFuncs: (DataSet => DataSet)*): (DataSet => DataSet) = (ds: DataSet) => dsFuncs.foldLeft[DataSet](ds)((s, f) => f(s))

  def transformValue(name: String, key: String, dataFunc: DataSet => DataSet) = (ds: DataSet) => transformEachDataRecord(s => s, r =>
    DataRecord(dataFunc(r(key)) :: r.fields)
  )

  def productSchemaFunc(labels: List[String]) = (schema: SchemaDefinition) => SchemaArray(SchemaRecord(SchemaArray("attributes",
    SchemaRecord(List(SchemaString("name", 0), SchemaString("type", 0), SchemaString("value", 0)))) :: schema.asInstanceOf[SchemaArray].content.asInstanceOf[SchemaRecord].fields.filterNot(f => labels.contains(f.label))))

  def productDataFunc(labels: List[String]) = (data: DataSet) => DataArray(data.elems.map(r => DataRecord(
    DataArray("attributes", r.elems.filter(f => labels.contains(f.label) && f.stringOption.getOrElse("").nonEmpty).map(v => DataRecord(List(DataString("name", v.label), DataString("type", "string"), v.stringOption.map(o => DataString("value", o)).getOrElse(Nothin("value"))))).toList)
      :: r.elems.filter(f => !labels.contains(f.label)).toList)).toList)

  def productProperty(ds: DataSet, labels: List[String]): DataSet = transformEachData(productSchemaFunc(labels), productDataFunc(labels))(ds)

  def pick(ds: DataSet, labels: List[String]): DataSet = transformEachData(_.value(labels), _.value(labels map Label))(ds)

  //def updateLabel(ds: DataSet, label: String): DataSet =

  def updateLabelDataFunc(l: String) = (d: DataSet) => d match {
    case DataArray(_, a) => DataArray(l, a)
    case DataRecord(_, r) => DataRecord(l, r)
    case _ => Nothin(l) // fix this to include all types
  }

  def updateLabelSchemaFunc(l: String) = (d: SchemaDefinition) => d match {
    case SchemaArray(_, a) => SchemaArray(l, a)
    case SchemaRecord(_, r) => SchemaRecord(l, r)
    case _ => SchemaUnknown(l) // fix this to include all types
  }

  def addDataFunc(data: DataSet, addData: DataSet) = data match {
    case DataRecord(l, fs) => DataRecord(l, addData :: fs)
    case v => DataRecord(List(v, addData))
  }

  def addSchemaFunc(schema: SchemaDefinition, addSchema: SchemaDefinition) = schema match {
    case SchemaRecord(l, fs) => SchemaRecord(l, addSchema :: fs)
    case SchemaArray(l, es) => SchemaArray(l, es)
    case v => SchemaRecord(List(v, addSchema))
  }

  def addDataString(ds: DataSet, label: String, value: String) = transformEachData(schema => addSchemaFunc(schema, SchemaString(label, 0)), data => addDataFunc(data, DataString(label, value)))(ds)

  def label(ds: DataSet, label: String) = transformEachData(updateLabelSchemaFunc(label), updateLabelDataFunc(label))(ds)

  def addData(ds: DataSet, template: List[String]) = {
    val temp = template mkString ","
    val tFunc = mergeT(temp)
    transformEachData(schema => schema, data => addDataFunc(data, Data2Json.fromJson2Data(tFunc(data))))(ds)
  }

  def mergeT(template: String): DataSet => String = d => {
    val ra = "@(.*?)@".r.findAllMatchIn(template).map(_.group(1)).toList
    val res = ra.foldLeft[String](template)((t, e) => t.replace("@" + e + "@", d.value(e).stringOption.getOrElse("")))
    res
  }

  //TODO: likely will remove Batch, it's confusing
  type Batch = (SchemaDefinition, DataSet)

  def numeric(batch: Batch, field: String, precision: Int, scale: Int): DataSet = batch match {
    case (s, DataRecord(key, fs)) =>
      new DataSetFixedData(s, if (fs.map(_.label).contains(field))
        DataRecord(key, fs.map(f =>
          if (f.label == field)
            DataNumeric(f.label, BigDecimal(f.stringOption.getOrElse("0")))
          else
            f).toList)
      else
        DataRecord(key, fs))
    case (s, DataArray(key, a)) =>
      new DataSetFixedData(s, DataArray(key, a.map(m => numeric((s, m), field, precision, scale).headOption.get)))
    case (s, d) => new DataSetFixedData(s, d)
  }

  def bool(batch: Batch, field: String): DataSet = batch match {
    case (s, DataRecord(key, fs)) =>
      new DataSetFixedData(s, if (fs.map(_.label).contains(field))
        DataRecord(key, fs.map(f =>
          if (f.label == field)
            DataBoolean(f.label, f.stringOption.map(_.toBoolean).getOrElse(false))
          else
            f).toList)
      else
        DataRecord(key, fs))
    case (s, DataArray(key, a)) =>
      new DataSetFixedData(s, DataArray(key, a.map(m => bool((s, m), field).headOption.get)))
    case (s, d) => new DataSetFixedData(s, d)
  }

  def delim(str: String, d: DataSet) = DataString(d.elems.map(_.stringOption.getOrElse("")).mkString(str))

  // tabular functions, to be refactored

  def split2Rows(ds: DataSetTableScala, columnName: String, regex: String) =
    DataSetTableScala(ds.getNextAvailableColumnName(columnName) :: ds.header,
      ds.rows flatMap (r => r(ds.getOrdinalOfColumn(columnName)) split regex map (_ :: r)))

  def keepFunc(ds: DataSetTableScala, selectorFunc: String => Boolean) = DataSetTableScala(ds.header filter selectorFunc, ds.rows map (r => ds.getOrdinalsWithPredicate(selectorFunc) map (r(_))))

  def keep(ds: DataSetTableScala, cols: List[String]): DataSetTableScala = keepFunc(ds, c => cols.contains(c))

  def keepRegex(ds: DataSetTableScala, regex: String): DataSetTableScala = keepFunc(ds, c => regex.matches(c))

  def dropFunc(ds: DataSetTableScala, selectorFunc: String => Boolean) = keepFunc(ds, !selectorFunc(_))

  def drop(ds: DataSetTableScala, cols: List[String]): DataSetTableScala = dropFunc(ds, c => cols.contains(c))

  def addHeader(ds: DataSetTableScala, cols: List[String]) = DataSetTableScala(cols, ds.header :: ds.rows)

  def row1Header(ds: DataSetTableScala) = DataSetTableScala(ds.rows.head map (_.toString), ds.rows.tail)

  def split2ColsD(ds: DataSetTableScala, columnName: String, delim: String) = {
    val csvSplit = delim + "(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)" // TODO: should allow encapsulation to be paramaterised
    val numberOfNewCols = ds.getColumnValues(columnName).map(_.split(csvSplit, -1).length).max

    DataSetTableScala(ds.getNextAvailableColumnName(columnName, numberOfNewCols) ::: ds.header, ds.rows.map(r => ds.getValue(r, columnName).split(csvSplit, -1).padTo(numberOfNewCols, "").map(_.replaceAll("^\"|\"$", "")).toList ::: r)) /// map(_.replaceAll("^\"|\"$", ""))))
  }

  def split2Cols(ds: DataSetTableScala, columnName: String) = split2ColsD(ds, columnName, ",")

  def renameFunc(ds: DataSetTableScala, selectorFunc: String => Boolean, renameFunc: String => String) = DataSetTableScala(ds.header map (c => if (selectorFunc(c)) renameFunc(c) else c), ds.rows)

  def renamePair(ds: DataSetTableScala, colPairs: List[(String, String)]): DataSetTableScala = renameFunc(ds, c => colPairs.map(_._1).contains(c), r => colPairs.find(f => f._1 == r).get._2)

  def rename(ds: DataSetTableScala, cols: List[String]): DataSetTableScala = renamePair(ds, cols.grouped(2).map(m => (m.head, m.tail.headOption.getOrElse(ds.getNextAvailableColumnName(m.head)))).toList)

  def rowFunc(ds: DataSetTableScala, columnName: String, rowFunc: List[String] => String) = DataSetTableScala(columnName :: ds.header, ds.rows map (r => rowFunc(r) :: r))

  def valueFunc(ds: DataSetTableScala, col: String, f: String => String) = rowFunc(ds, ds.getNextAvailableColumnName(col), r => f(ds.getValue(r, col)))

  def sum(ds: DataSetTableScala, cols: List[String]) = rowFunc(ds, ds.getNextAvailableColumnName("sum"), r => (cols map (c => scala.math.BigDecimal(ds.getValue(r, c)))).sum.toString)

  def orderCols(ds: DataSetTableScala, cols: List[String]) = DataSetTableScala(cols, ds.rows map (r => cols map (ds.getValue(r, _))))

  def templateMerge(ds: DataSetTableScala, template: String) =
    DataSetTableScala(ds.getNextAvailableColumnName("template") :: ds.header, ds.rows.map(r => ds.header.foldLeft(template)((c, t) => t.replaceAll("@" + c, ds.getValue(r, c))) :: r))

  def prepare4statement(ds: DataSetTableScala, template: String) = orderCols(ds, "@(?<name>[-_a-zA-Z0-9]+)".r.findAllMatchIn(template).map(_.group(1)).toList)

  def changes(ds1: DataSetTableScala, ds2: DataSetTableScala, keyCols: List[String]) = DataSetTableScala(ds1.header, ds1.rows.filter(r => {
    val option = ds2.rows.find(ri => keyCols.forall(c => ds1.getValue(r, c) == ds2.getValue(ri, c)))
    if (option.isDefined)
      !ds1.header.forall(c => {
        val equal = ds1.getValue(r, c) == ds2.getValue(option.get, c) //TODO string equality doesn't work nicely for numerics & dates
        if (!equal)
          printf(c + ":" + ds1.getValue(r, c)) //TODO remove side-effect. I needed this to test diffs
        equal
      })
    else
      false
  }))

  def newRows(ds1: DataSetTableScala, ds2: DataSetTableScala, keyCols: List[String]) = {
    val ds2KeysOnly = keep(ds2, keyCols)
    DataSetTableScala(ds1.header, ds1.rows.filterNot(r => ds2KeysOnly.rows.contains(keyCols.map(ds1.getValue(r, _)))))
  }

  def match2cols(ds: DataSetTableScala, columnName: String, regexes: List[String]) = DataSetTableScala(ds.getNextAvailableColumnName(columnName, regexes.length) ::: ds.header, ds.rows.map(r => {
    val matches = regexes map (_.r.findFirstMatchIn(ds.getValue(r, columnName)).isDefined)

    if (matches.contains(true))
      List.fill(matches.indexOf(true))("") ::: (ds.getValue(r, columnName) :: List.fill(matches.length - matches.indexOf(true) - 1)("")) ::: r
    else
      List.fill(matches.length)("") ::: r
  }))

  def concatFunc(ds: DataSetTableScala, selectorFunc: String => Boolean, delim: String = "") = rowFunc(ds, ds.getNextAvailableColumnName("concat"), r => ds.getOrdinalsWithPredicate(selectorFunc) map (r(_)) mkString delim)

  def concat(ds: DataSetTableScala, cols: List[String], delim: String): DataSet = rowFunc(ds, ds.getNextAvailableColumnName("concat"), r => cols map (ds.getValue(r, _)) mkString delim)

  def const(ds: DataSetTableScala, value: List[String]) = DataSetTableScala(ds.getNextAvailableColumnName("const", value.length) ::: ds.header, ds.rows map (value ::: _)) //rowFunc(ds, ds.getNextAvailableColumnName("const"), _ => value)

  def mergeCols(ds1: DataSetTableScala, ds2: DataSetTableScala) = DataSetTableScala(ds1.header ::: ds2.header, (ds1.rows zip ds2.rows) map (r => r._2 ::: r._1))

  def convDateValue(value: String, in: String, out: String) =
    try {
      LocalDate.parse(value, DateTimeFormatter.ofPattern(in)).format(DateTimeFormatter.ofPattern(out))
    }
    catch {
      case _: Exception => "1900-01-01"
    }

  def convDate(ds: DataSetTableScala, col: String, in: String, out: String) = valueFunc(ds, col, convDateValue(_, in, out))

  def defaultIfBlankValue(value: String, default: String) = if (value == null || value.trim.isEmpty) default else value

  def defaultIfBlank(ds: DataSetTableScala, cols: List[String], default: String) = cols.foldLeft(ds)((d, c) => valueFunc(d, c, defaultIfBlankValue(_, default)))

  def filterIfNotBlank(ds: DataSetTableScala, cols: List[String]) = DataSetTableScala(ds.header, ds.rows filter (r => cols.forall(!ds.getValue(r, _).trim.isEmpty)))

  def transformLookupFunc(ds1: DataSetTableScala, ds2: DataSetTableScala, condition: (List[String], List[String]) => Boolean, lookupSelectorFunc: String => Boolean) =
    DataSetTableScala((ds2.header.filter(lookupSelectorFunc) map ds1.getNextAvailableColumnName) ::: ds1.header,
      ds1.rows.map(r1 => ds2.rows.find(condition(r1, _)).getOrElse(getEmptyRow(ds2)).zipWithIndex.filter(f => ds2.getOrdinalsWithPredicate(lookupSelectorFunc).contains(f._2)).map(_._1) ::: r1))


  def trimValue(value: String) = value.trim

  def trim(ds: DataSetTableScala, cols: List[String]) = cols.foldLeft(ds)((d, c) => valueFunc(d, c, trimValue))

  def mapOrElseValue(value: String, colPairs: Map[String, String], orElse: String) = colPairs.getOrElse(value, orElse)

  def mapOrElse(ds: DataSetTableScala, col: String, colPairs: List[String], orElse: String) = {
    val pairMap = colPairs.grouped(2).map(g => (g.head, g.tail.headOption.getOrElse(""))).toMap
    valueFunc(ds, col, v => mapOrElseValue(v, pairMap, orElse))
  }

  def jsonObject(ds: DataSetTableScala, cols: List[String]) = rowFunc(ds, ds.getNextAvailableColumnName("json"), r => "{" + (cols filter (f => ds.getValue(r, f) != null && ds.getValue(r, f).nonEmpty) map (c => "\"" + c + "\": \"" + ds.getValue(r, c) + "\"") mkString ",") + "}")

  def copy(ds: DataSetTableScala, from: String, to: List[String]) = DataSetTableScala(to ::: ds.header, ds.rows map (r => List.fill(to.size)(ds.getValue(r, from)) ::: r))

  def coalesceValue(vals: List[String]) = vals.find(v => v.trim().nonEmpty).getOrElse("")

  def coalesce(ds: DataSetTableScala, cols: List[String]) = rowFunc(ds, ds.getNextAvailableColumnName("coalesce"), r => coalesceValue(cols.map(ds.getValue(r, _)).toList))

  // helpers
  def getEmptyRow(ds: DataSetTableScala) = List.fill(ds.header.length)(null)

  def isEmptyDataSet(ds: DataSetTableScala) = ds.rows.isEmpty


}