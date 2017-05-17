package com.actio

import java.security.MessageDigest
import java.text.{DecimalFormat, SimpleDateFormat}
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.net.URLEncoder
import java.util.Date

import org.apache.commons.codec.binary.Hex
import org.apache.commons.lang.time.DateUtils

import scala.annotation.tailrec
import scala.util.Try

/**
  * Created by mauri on 27/04/2016.
  */
//noinspection ScalaStyle,ScalaStyle

object TransformsDataSet {

  //TODO: likely will remove Batch, it's confusing
  type Batch = (SchemaDefinition, DataSet)

  /**
    * Sorts the DataSet by a specified property - Currently only supports dates
    *
    * @param ds         the DataSet containing the items to sort
    * @param property   the property to sort by
    * @param dataType   the data type of the property to sort by
    * @param dataFormat the format of the property value
    * @param direction  the direction to sort by
    * @return           a new DataSet containing the sorted items
    */
  def orderBy(ds: DataSet, property: String, dataType: String, dataFormat: String,  direction: String): DataSet = {
    orderByRE(ds, property, dataType, dataFormat, direction, 0)
  }

  def orderByRE(ds: DataSet, property: String, dataType: String, dataFormat: String,  direction: String, level: Int): DataSet = {
    @tailrec
    var dataFormatOption = Option(dataFormat)
    var orderedSet = ds
    if(dataFormat == "")
      dataFormatOption = None
    val elementCount = ds.elems.length
    if(elementCount > 1)
      {
        if(ds.elems.forall(x => x.value(property).toOption.isDefined)) {
          orderedSet = ds match {
            case DataArray(label, arrayElems) => dataType.toLowerCase() match {
              case "date" =>
                if (dataFormatOption.isDefined)
                  DataArray(label, sortByDate(arrayElems, property, dataFormatOption.get, direction))
                else
                  ds
              case _ => ds
            }
            case DataRecord(label, elements) => dataType.toLowerCase() match {
              case "date" =>
                if (dataFormatOption.isDefined)
                  DataRecord(label, sortByDate(elements, property, dataFormatOption.get, direction))
                else
                  ds
              case _ => var sortedElements = elements.sortBy(x => x.value(property).stringOption)
                if (direction.equalsIgnoreCase("desc"))
                  sortedElements = sortedElements.reverse
                DataArray(label, sortedElements)
            }
            case _ => ds
          }
        }
        else
          orderedSet = DataRecord("item", ds.elems.toList)
    }
    else if (elementCount == 1) {
      if(ds.headOption.isDefined) {
        if (level == 0) {
          orderedSet = ds match {
            case DataArray(label, _) => DataArray(label, List[DataSet] {
              orderByRE(ds.elems.next(), property, dataType, dataFormat, direction, (level + 1))
            })
            case DataRecord(label, _) => DataRecord(label, List[DataSet] {
              orderByRE(ds.elems.next(), property, dataType, dataFormat, direction, (level + 1))
            })
            case DataSetHttpResponse(label, _, _, _, body) => DataRecord(label, List[DataSet] {
              orderByRE(body, property, dataType, dataFormat, direction, (level + 1))
            })
            case _ => ds
          }
        }
      }
    }
    if (level == 0)
      DataRecord("orderBy", List[DataSet]{orderedSet})
    else
      orderedSet
  }

  def sortByDate(items: List[DataSet], property: String, dateFormat: String, direction: String): List[DataSet] = {
    implicit val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.toEpochDay)
    val sortedItems = items.sortBy(x => LocalDate.parse(x.value(property).stringOption.getOrElse(""), DateTimeFormatter.ofPattern(dateFormat)).toEpochDay)(Ordering[Long])
    if(direction.equalsIgnoreCase("desc"))
      sortedItems.reverse
    else
      sortedItems
  }

  def take(ds: DataSet, numberOfItems: Int): DataSet = {
    val elementCount = ds.elems.length
    var takenSet = ds
    if(elementCount > 1)
      takenSet = DataArray(ds.label, ds.elems.take(numberOfItems).toList)
    else if(elementCount == 1 && ds.headOption.isDefined) {
      takenSet = ds.headOption.get match {
        case DataArray(label, arrayElems) => DataArray(label, arrayElems.take(numberOfItems))
        case DataRecord(label, elems) =>
          if (elems.forall(x => x.label == elems.head.label))
            DataArray(label, elems.take(numberOfItems))
          else
            DataArray("root", List(DataRecord(label, elems.take(numberOfItems))))
        case _ => DataArray(ds.label, ds.elems.take(numberOfItems).toList)
      }
    }
    DataRecord("take", List[DataSet]{takenSet})
  }

  def chunk(ds: DataSet, numberOfItemsPerChunk: Int): DataSet = {
    DataRecord("chunk", ds.elems.grouped(numberOfItemsPerChunk).map(p => DataArray("piece", p.toList )).toList)
  }

  def getDataSetWithHierarchy(ds: DataSet, hierarchyPath:Array[String]): List[DataSet] = {
    @tailrec
    var itemsToReturn = List[DataSet]()
    val nextItemInHierarchy = hierarchyPath.head
    val endOfHierarchyReached = if (hierarchyPath.tail.length == 0) true else false
    if (endOfHierarchyReached) {
      if(nextItemInHierarchy.equals("*")) {
        val l =  ds.elems.toList
        itemsToReturn = itemsToReturn:::l
      }
      else
        itemsToReturn = itemsToReturn:+ds.value(nextItemInHierarchy)
    }
    else {
      if(nextItemInHierarchy.equals("_"))
        itemsToReturn = itemsToReturn:::ds.elems.foldLeft(List[DataSet]()) { (z:List[DataSet], f:DataSet) => z:::getDataSetWithHierarchy(f, hierarchyPath.tail) }
      else
        itemsToReturn = itemsToReturn:::getDataSetWithHierarchy(ds.value(nextItemInHierarchy), hierarchyPath.tail)
    }
    itemsToReturn
  }

  /**
    * Flattens a hierarcy structure by copying items at the specified hierarchy level into the root
    * supports merging items at different hierarchy levels as "includes"
    *
    * @param ds   the dataset containing the hierarchy structure
    * @param args comma separated list of . notation hierarchy i.e. fieldA._.* will copy all the grandchildren of fieldA without knowing the child field names
    *             any hierarchy definitions prefixed with + will get appended to the copied items
    * @return     DataArray
    */
  def flattenStructure(ds: DataSet, args: List[String]): DataSet =
  {
    val fieldsToFlatten = args.filterNot(x => x.startsWith("+"))
    val fieldsToInclude = args.filter(x => x.startsWith("+")).map(x => x.substring(1))
    var flattenedList = List[DataSet]()
    for (field <- fieldsToFlatten) {
      val hierarchy = field.split('.')
      flattenedList = flattenedList:::getDataSetWithHierarchy(ds, hierarchy)
    }
    for ( field <- fieldsToInclude) {
      val hierarchy = field.split('.')
      val dataSetToInclude = getDataSetWithHierarchy(ds, hierarchy)
      flattenedList = flattenedList.map(x => DataRecord(x.label,x.elems.toList:::dataSetToInclude))
    }
    DataRecord("flatternedList", List[DataSet](DataArray("item",flattenedList)))
  }

  /**
    * Attempts to map the DataSet to DataSetTableScala with custom sub DataSets
    *
    * @param ds DataSet to map
    * @return   DataSetTableScala
    */
  def mapToDataSetTableScala(ds: DataSet): DataSetTableScala = {
    ds match {
      case DataSetHttpResponse(_,_,_,_,body) => body.schema match {
        case SchemaRecord(_, fields) =>
          if (fields.head.label.isEmpty())
            DataSetTableScala(SchemaArray("", fields.head), body)
          else
            DataSetTableScala(SchemaArray("", body.schema), body)
        case _ => DataSetTableScala(body.schema, ds.headOption.get)
      }
      case DataArray(_,arrayElems) => ds.schema match {
        case SchemaRecord(_, fields) =>
          if (fields.head.label.isEmpty())
            DataSetTableScala(SchemaArray("", fields.head), ds)
          else
            DataSetTableScala(SchemaArray("", arrayElems.head.schema), ds)
        case _ => DataSetTableScala(ds.schema, ds)
      }
      case DataRecord(_, fields) => DataSetTableScala(SchemaArray("",ds.headOption.get.schema), ds)
      case _ => DataSetTableScala(SchemaArray("",ds.schema), ds)//DataSetTableScala(ds)
    }

  }

  def dateFormat(ds: DataSet, format: String): DataSet = {
    try {
      DataString(new SimpleDateFormat(format).format(ds.asInstanceOf[DataDate].date))
    }
    catch {
      case _: Exception => DataString(new SimpleDateFormat(format).format(new SimpleDateFormat("dd/MM/yyyy").parse("1/1/1900")))
    }
  }

  def parseDate(dateStr: String, format: String): DataSet = {
    try {
      DataDate(new SimpleDateFormat(format).parse(dateStr))
    }
    catch {
      case e: Exception => throw new Exception(String.format("Unable to parseDate %s, with format %s",dateStr, format))
    }
  }

  def parseDateWithDefault(dateStr: String, format: String): DataSet = {
    try {
      DataDate(new SimpleDateFormat(format).parse(dateStr))
    }
    catch {
      case e: Exception => {
        DataString(new SimpleDateFormat(format).format(new Date))
      }
    }
  }

  def today(dateOffset: Int): DataSet = DataDate(DateUtils.addDays(new java.util.Date(), dateOffset))

  def filterValue(ds: DataSet, property: String, value: String): DataSet = DataArray(ds.elems.filter(f => f(property).stringOption.getOrElse("") == value).toList)

  def firstValue(ds: DataSet, property: String, value: String): DataSet = ds.elems.find(f => f.value(property).stringOption.getOrElse("") == value).getOrElse(Nothin())

  def convertDateFormat(ds: DataSet, in: String, out: String): DataSet = DataString(convDateValue(ds.stringOption.getOrElse(""), in, out))

  def ifNotBlankOrElse(ds: DataSet, other: String): DataSet = ds.stringOption.map(s => if (s.trim().isEmpty) DataString(other) else DataString(s)).getOrElse(DataString(other))

  def concatString(ds: DataSet): DataSet = DataString(ds.elems.filter(_.stringOption.isDefined).map(_.stringOption.get).mkString(","))

  //	  "{1-9}-{9-12}-{13-16}-{17-20}-{21-32}"
  def toUUIDFormat(str: String): DataSet = DataString(str.substring(0,8)+"-"+str.substring(8,12)+"-"+str.substring(12,16)+"-"+str.substring(16,20)+
    "-"+str.substring(20,32))

  def splitTrim(instr: String, delim: String): DataSet =  DataArray( instr.split(delim).toList.map( s => DataString(s.trim)  ))

  def ifEqualOrElse(ds: DataSet, equal: String, dsThen: DataSet, dsElse: DataSet): DataSet = if (ds.stringOption.getOrElse("") == equal) dsThen else dsElse

  def nothing(): DataSet = Nothin()

  def isBlank(ds: DataSet) = DataBoolean(ds.stringOption.exists(_.isEmpty))

  def isNull(ds: DataSet) = DataBoolean(ds.toOption.isEmpty)

  def isEmpty(ds: DataSet): DataSet = DataBoolean(ds.elems.isEmpty)

  def size(ds: DataSet): DataSet = DataNumeric(ds.elems.size)

  def contains(ds: List[DataSet], d: DataSet) = DataBoolean(ds.exists(i => i.stringOption == d.stringOption))

  def strContains(str: String, targetStr: String): DataSet = DataBoolean(if(str == null || targetStr == null) false else str.contains(targetStr))

  def strNotContains(str: String, targetStr: String): DataSet = DataBoolean(if(str == null || targetStr == null) false else !str.contains(targetStr))

  def substring(str: String, start: Int): DataSet = if (start < str.length) DataString(str.substring(start)) else DataString("")

  def substringWithEnd(str: String, start: Int, end: Int) = DataString(str.substring(start,if(str.length-1 < end) str.length-1 else end))

  def capitalise(str: String): DataSet = DataString(str.toUpperCase)

  def quoteOption(ds: DataSet) = ds.stringOption.map(s => if (s.isEmpty) DataString("null") else DataString("\"" + s + "\"")).getOrElse(DataString("null"))

  def urlEncode(str: String): DataSet = DataString(URLEncoder.encode(str, "UTF-8"))

  def replaceAll(str: String, find: String, replaceWith: String): DataSet =
    try{ DataString(str.replaceAll(find, replaceWith))
    }
    catch {
      case _: Throwable => DataString(str)
    }

  def cleanStr(str: String) : DataSet =
    try {
      DataString(str.replace("[","_").replace("]","_").replace(".","_").replace("\n","").replace("\"","'"))
    }
    catch {
      case _: Throwable => DataString(str)
    }

  def sha256(str: String): DataSet = DataString(org.apache.commons.codec.digest.DigestUtils.sha256Hex(str))

  // single quote escape
  def sq(str: String): DataSet = if(str == null) DataString("") else DataString(str.replace("'","''"))

  def numeric(value: String): DataSet = DataNumeric(Try(BigDecimal(value)).getOrElse(BigDecimal(0)))

  def numericFormat(value: String, format: String) =  {

//    if (value == "" && format == "#0")
 //     DataString("0")
 //   else
      DataString(new DecimalFormat(format).format(Try(BigDecimal(value)).getOrElse(BigDecimal(0))))
  }

  def sign(value: String): DataSet = DataNumeric(Try(BigDecimal(value).signum).getOrElse(0))

  def integer(value: String): DataSet = DataNumeric(Try(BigDecimal(value.toInt)).getOrElse(BigDecimal(0)))

  def round(value: String, scale: Int): DataSet = roundWithMode(value,scale,"HALF_UP")

  def roundWithMode(value: String, scale: Int, mode: String="HALF_UP"): DataSet = {
    val roundingMode =
      if (mode == None) BigDecimal.RoundingMode.HALF_UP
      else mode.toUpperCase() match {
        case "HALF_EVEN" =>
          BigDecimal.RoundingMode.HALF_EVEN
        case "HALF_DOWN" =>
          BigDecimal.RoundingMode.HALF_DOWN
        case "UP" =>
          BigDecimal.RoundingMode.UP
        case "DOWN" =>
          BigDecimal.RoundingMode.DOWN
        case "CEILING" =>
          BigDecimal.RoundingMode.CEILING
        case "FLOOR" =>
          BigDecimal.RoundingMode.FLOOR
        case _ =>
          BigDecimal.RoundingMode.HALF_UP
      }
    DataNumeric(Try(BigDecimal(value).setScale(scale, roundingMode)).getOrElse(BigDecimal(0)))
  }

  def csvWithHeader(ds: DataSet, delim: String): DataSet = {
    val csvSplit = delim + "(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)" // TODO: should allow encapsulation to be paramaterised
    val rows = ds.elems.map(r => r(0).stringOption.getOrElse("").split(csvSplit, -1).map(c => c.replaceAll("^\"|\"$", ""))).toList
    if(rows.length == 0)
      Nothin()
      else
      DataArray(rows.tail.map(r => DataRecord(rows.head.zipWithIndex.map(c => DataString(c._1, r(c._2))).toList)).toList)
  }

  def removeTrailingZeros(value: String): DataSet = DataNumeric(Try(BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP)).getOrElse(BigDecimal(0)).underlying().stripTrailingZeros())

  def batch(ds: DataSet): DataSet = DataRecord("", List(ds))

  def sumValues(ls: List[DataSet]) = DataNumeric(ls.foldLeft(BigDecimal(0))((d,l) => d + Try(BigDecimal(l.stringOption.getOrElse("0"))).getOrElse(BigDecimal(0))))

  def orElse(ds: DataSet, or: DataSet): DataSet = ds.toOption.getOrElse(or)

  def md5(ls: List[DataSet]) = {
    val str = ls map (_.stringOption.getOrElse("")) mkString ""
    val m = MessageDigest.getInstance("MD5")

    m.update(str.getBytes(),0,str.length)
    DataString(Hex.encodeHexString(m.digest))
  }

  def maprecord(ds: DataSet): DataSet = DataArray(ds.elems.map(e => DataRecord(e)).toList)

  /* below will need to be replaced when I have time */

  def equal(ds1: DataSet, ds2: DataSet) = DataBoolean(ds1 == ds2)

  def transformDataSets(dsFuncs: (DataSet => DataSet)*): (DataSet => DataSet) = (ds: DataSet) => dsFuncs.foldLeft[DataSet](ds)((s, f) => f(s))

  def transformValue(name: String, key: String, dataFunc: DataSet => DataSet) = (ds: DataSet) => transformEachDataRecord(s => s, r =>
    DataRecord(dataFunc(r(key)) :: r.fields)
  )

  def transformEachDataRecord(schemaFunc: (SchemaDefinition => SchemaDefinition), dataRecordFunc: (DataRecord) => (DataRecord)): (DataSet => DataSet) =
    transformEachData(schemaFunc, (d: DataSet) => DataArray(d.elems.map(r => dataRecordFunc(r.asInstanceOf[DataRecord])).toList))

  def productProperty(ds: DataSet, labels: List[String]): DataSet = transformEachData(productSchemaFunc(labels), productDataFunc(labels))(ds)

  def productSchemaFunc(labels: List[String]) = (schema: SchemaDefinition) => SchemaArray(SchemaRecord(SchemaArray("attributes",
    SchemaRecord(List(SchemaString("name", 0), SchemaString("type", 0), SchemaString("value", 0)))) :: schema.asInstanceOf[SchemaArray].content.asInstanceOf[SchemaRecord].fields.filterNot(f => labels.contains(f.label))))

  def productDataFunc(labels: List[String]) = (data: DataSet) => DataArray(data.elems.map(r => DataRecord(
    DataArray("attributes", r.elems.filter(f => labels.contains(f.label) && f.stringOption.getOrElse("").nonEmpty).map(v => DataRecord(List(DataString("name", v.label), DataString("type", "string"), v.stringOption.map(o => DataString("value", o)).getOrElse(Nothin("value"))))).toList)
      :: r.elems.filter(f => !labels.contains(f.label)).toList)).toList)

  def pick(ds: DataSet, labels: List[String]): DataSet = transformEachData(_.value(labels), _.value(labels map Label))(ds)

  def toJsonStr(ds: DataSet): DataSet = DataString("jsonstr", Data2Json.toJsonString(ds))

  //def updateLabel(ds: DataSet, label: String): DataSet =

  //TODO: think about error handling. Maybe change next: Either[Error, Data]
  def transformEachData(schemaFunc: (SchemaDefinition => SchemaDefinition), dataFunc: (DataSet => DataSet)): (DataSet => DataSet) = (ds: DataSet) =>
    new DataSet {
      override def elems = ds.elems map dataFunc

      override lazy val schema = schemaFunc(ds.schema)

      override def label: String = ""
    }

  def addDataString(ds: DataSet, label: String, value: String) = transformEachData(schema => addSchemaFunc(schema, SchemaString(label, 0)), data => addDataFunc(data, DataString(label, value)))(ds)

  def addSchemaFunc(schema: SchemaDefinition, addSchema: SchemaDefinition) = schema match {
    case SchemaRecord(l, fs) => SchemaRecord(l, addSchema :: fs)
    case SchemaArray(l, es) => SchemaArray(l, es)
    case v => SchemaRecord(List(v, addSchema))
  }

  def label(ds: DataSet, label: String) = transformEachData(updateLabelSchemaFunc(label), updateLabelDataFunc(label))(ds)

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

  def addData(ds: DataSet, template: List[String]) = {
    val temp = template mkString ","
    val tFunc = mergeT(temp)
    transformEachData(schema => schema, data => addDataFunc(data, Data2Json.fromJson2Data(tFunc(data))))(ds)
  }

  def addDataFunc(data: DataSet, addData: DataSet) = data match {
    case DataRecord(l, fs) => DataRecord(l, addData :: fs)
    case v => DataRecord(List(v, addData))
  }

  def mergeT(template: String): DataSet => String = d => {
    val ra = "@(.*?)@".r.findAllMatchIn(template).map(_.group(1)).toList
    val res = ra.foldLeft[String](template)((t, e) => t.replace("@" + e + "@", d.value(e).stringOption.getOrElse("")))
    res
  }

  def delim(str: String, d: DataSet) = DataString(d.elems.map(_.stringOption.getOrElse("")).mkString(str))

  // tabular functions, to be refactored

  def split2Rows(ds: DataSetTableScala, columnName: String, regex: String) =
    DataSetTableScala(ds.getNextAvailableColumnName(columnName) :: ds.header,
      ds.rows flatMap (r => r(ds.getOrdinalOfColumn(columnName)) split regex map (_ :: r)))

  def keepRegex(ds: DataSetTableScala, regex: String): DataSetTableScala = keepFunc(ds, c => regex.matches(c))

  def drop(ds: DataSetTableScala, cols: List[String]): DataSetTableScala = dropFunc(ds, c => cols.contains(c))

  def dropFunc(ds: DataSetTableScala, selectorFunc: String => Boolean) = keepFunc(ds, !selectorFunc(_))

  def addHeader(ds: DataSetTableScala, cols: List[String]) = DataSetTableScala(cols, ds.header :: ds.rows)

  def row1Header(ds: DataSetTableScala) = DataSetTableScala(ds.rows.head map (_.toString), ds.rows.tail)

  def split2Cols(ds: DataSetTableScala, columnName: String) = split2ColsD(ds, columnName, ",")

  def split2ColsD(ds: DataSetTableScala, columnName: String, delim: String) = {
    val csvSplit = delim + "(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)" // TODO: should allow encapsulation to be paramaterised
    val numberOfNewCols = ds.getColumnValues(columnName).map(_.split(csvSplit, -1).length).max

    DataSetTableScala(ds.getNextAvailableColumnName(columnName, numberOfNewCols) ::: ds.header, ds.rows.map(r => ds.getValue(r, columnName).split(csvSplit, -1).padTo(numberOfNewCols, "").map(_.replaceAll("^\"|\"$", "")).toList ::: r)) /// map(_.replaceAll("^\"|\"$", ""))))
  }

  def rename(ds: DataSetTableScala, cols: List[String]): DataSetTableScala = renamePair(ds, cols.grouped(2).map(m => (m.head, m.tail.headOption.getOrElse(ds.getNextAvailableColumnName(m.head)))).toList)

  def renamePair(ds: DataSetTableScala, colPairs: List[(String, String)]): DataSetTableScala = renameFunc(ds, c => colPairs.map(_._1).contains(c), r => colPairs.find(f => f._1 == r).get._2)

  def renameFunc(ds: DataSetTableScala, selectorFunc: String => Boolean, renameFunc: String => String) = DataSetTableScala(ds.header map (c => if (selectorFunc(c)) renameFunc(c) else c), ds.rows)

  def sum(ds: DataSetTableScala, cols: List[String]) = rowFunc(ds, ds.getNextAvailableColumnName("sum"), r => (cols map (c => scala.math.BigDecimal(ds.getValue(r, c)))).sum.toString)

  def templateMerge(ds: DataSetTableScala, template: String) =
    DataSetTableScala(ds.getNextAvailableColumnName("template") :: ds.header, ds.rows.map(r => ds.header.foldLeft(template)((c, t) => t.replaceAll("@" + c, ds.getValue(r, c))) :: r))

  def prepare4statement(ds: DataSetTableScala, template: String) = orderCols(ds, "@(?<name>[-_a-zA-Z0-9]+)".r.findAllMatchIn(template).map(_.group(1)).toList)

  def orderCols(ds: DataSetTableScala, cols: List[String]) = DataSetTableScala(cols, ds.rows map (r => cols map (ds.getValue(r, _))))

  def changes(ds1: DataSetTableScala, ds2: DataSetTableScala, keyCols: List[String]) = DataSetTableScala(ds1.header, ds1.rows.filter(r => {
    val option = ds2.rows.find(ri => keyCols.forall(c => ds1.getValue(r, c) == ds2.getValue(ri, c)))
    if (option.isDefined)
      !ds1.header.intersect(ds2.header).forall(c => {
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

  def keep(ds: DataSetTableScala, cols: List[String]): DataSetTableScala = keepFunc(ds, c => cols.contains(c))

  def keepFunc(ds: DataSetTableScala, selectorFunc: String => Boolean) = DataSetTableScala(ds.header filter selectorFunc, ds.rows map (r => ds.getOrdinalsWithPredicate(selectorFunc) map (r(_))))

  def match2cols(ds: DataSetTableScala, columnName: String, regexes: List[String]) = DataSetTableScala(ds.getNextAvailableColumnName(columnName, regexes.length) ::: ds.header, ds.rows.map(r => {
    val matches = regexes map (_.r.findFirstMatchIn(ds.getValue(r, columnName)).isDefined)

    if (matches.contains(true))
      List.fill(matches.indexOf(true))("") ::: (ds.getValue(r, columnName) :: List.fill(matches.length - matches.indexOf(true) - 1)("")) ::: r
    else
      List.fill(matches.length)("") ::: r
  }))

  def concatFunc(ds: DataSetTableScala, selectorFunc: String => Boolean, delim: String = "") = rowFunc(ds, ds.getNextAvailableColumnName("concat"), r => ds.getOrdinalsWithPredicate(selectorFunc) map (r(_)) mkString delim)

  def rowFunc(ds: DataSetTableScala, columnName: String, rowFunc: List[String] => String) = DataSetTableScala(columnName :: ds.header, ds.rows map (r => rowFunc(r) :: r))

  def concat(ds: DataSetTableScala, cols: List[String], delim: String): DataSet = rowFunc(ds, ds.getNextAvailableColumnName("concat"), r => cols map (ds.getValue(r, _)) mkString delim)

  def const(ds: DataSetTableScala, value: List[String]) = DataSetTableScala(ds.getNextAvailableColumnName("const", value.length) ::: ds.header, ds.rows map (value ::: _)) //rowFunc(ds, ds.getNextAvailableColumnName("const"), _ => value)

  def mergeCols(ds1: DataSetTableScala, ds2: DataSetTableScala) = DataSetTableScala(ds1.header ::: ds2.header, (ds1.rows zip ds2.rows) map (r => r._2 ::: r._1))

  def convDate(ds: DataSetTableScala, col: String, in: String, out: String) = valueFunc(ds, col, convDateValue(_, in, out))

  def convDateValue(value: String, in: String, out: String) =
    try {
      if(value.contains(":"))
        LocalDateTime.parse(value, DateTimeFormatter.ofPattern(in)).format(DateTimeFormatter.ofPattern(out))
      else
        LocalDate.parse(value, DateTimeFormatter.ofPattern(in)).format(DateTimeFormatter.ofPattern(out))
    }
    catch {
      case _: Exception => "1900-01-01 00:00:00.0"
    }

  def defaultIfBlank(ds: DataSetTableScala, cols: List[String], default: String) = cols.foldLeft(ds)((d, c) => valueFunc(d, c, defaultIfBlankValue(_, default)))

  def defaultIfBlankValue(value: String, default: String) = if (value == null || value.trim.isEmpty) default else value

  def filterIfNotBlank(ds: DataSetTableScala, cols: List[String]) = DataSetTableScala(ds.header, ds.rows filter (r => cols.forall(!ds.getValue(r, _).trim.isEmpty)))

  def transformLookupFunc(ds1: DataSetTableScala, ds2: DataSetTableScala, condition: (List[String], List[String]) => Boolean, lookupSelectorFunc: String => Boolean) =
    DataSetTableScala((ds2.header.filter(lookupSelectorFunc) map ds1.getNextAvailableColumnName) ::: ds1.header,
      ds1.rows.map(r1 => ds2.rows.find(condition(r1, _)).getOrElse(getEmptyRow(ds2)).zipWithIndex.filter(f => ds2.getOrdinalsWithPredicate(lookupSelectorFunc).contains(f._2)).map(_._1) ::: r1))

  // helpers
  def getEmptyRow(ds: DataSetTableScala) = List.fill(ds.header.length)(null)

  def trim(ds: DataSetTableScala, cols: List[String]) = cols.foldLeft(ds)((d, c) => valueFunc(d, c, v => v.trim))

  def trimValue(value: String) = DataString(Option(value).getOrElse("").trim)

  def mapOrElse(v: String, colPairs: List[DataSet], orElse: String) = {
    val pairMap = colPairs.map(p => p.stringOption.getOrElse("")).grouped(2).map(g => (g.head, g.tail.headOption.getOrElse(""))).toMap
    DataString(pairMap.getOrElse(v, orElse))
  }

  def valueFunc(ds: DataSetTableScala, col: String, f: String => String) = rowFunc(ds, ds.getNextAvailableColumnName(col), r => f(ds.getValue(r, col)))

  def mapOrElseValue(value: String, colPairs: Map[String, String], orElse: String) = colPairs.getOrElse(value, orElse)

  def jsonObject(ds: DataSetTableScala, cols: List[String]) = rowFunc(ds, ds.getNextAvailableColumnName("json"), r => "{" + (cols filter (f => ds.getValue(r, f) != null && ds.getValue(r, f).nonEmpty) map (c => "\"" + c + "\": \"" + ds.getValue(r, c) + "\"") mkString ",") + "}")

  def copy(ds: DataSetTableScala, from: String, to: List[String]) = DataSetTableScala(to ::: ds.header, ds.rows map (r => List.fill(to.size)(ds.getValue(r, from)) ::: r))

  def coalesce(vals: List[DataSet]) = vals.find(v => v.toOption.isDefined).getOrElse(Nothin())

  def blankAsNull(str: String) = if(str.isEmpty) Nothin() else DataString(str)

  def isEmptyDataSet(ds: DataSetTableScala) = ds.rows.isEmpty

  def deDup(ds: DataSetTableScala, col: String): DataSetTableScala = {

    // col is the label
    var hdr: List[String] = ds.header
    var rows: List[List[String]] = ds.rows
    var colIdx: Int = hdr.indexOf(col)
    var colln = rows.groupBy((f: List[String]) => f(colIdx)).map(_._2.head).toList

    return DataSetTableScala(hdr, colln)
  }

  def distinct(ds: DataSet, col: String) = {
    DataRecord(DataArray(ds.label, ds.elems.toList.groupBy((row: DataSet) => row(col)).map(_._2.head).toList))
  }

  // ================
  // CUSTOM FUNCTIONS
  //

  def subStringRegexp(instr: String, regexp: String) : String =
  {
    val rexp = regexp.r
    instr match {
      case rexp(x) => x
      case _ => ""
    }
  }

  def getSubStringRegexp(instr: String, regexp: String) : DataSet = {
    DataString(subStringRegexp(instr,regexp))
  }

  def getNumericRegexp(instr: String) : DataSet = {
    //  "(\d*\.?\d*)"

    DataString(subStringRegexp(instr,"""[^\+\-\d]*([\+\-\d]*\.?\d*).*"""))
  }

  def getLeftTrimRight(instr: String, len: Integer) : DataSet =
  {
    DataString(subStringRegexp(instr,"""(.*).{"""+len+"""}$"""))
  }

  // custom parse get the numeric part of a string
  def getNumericPrism(instr: String): DataSet = {
    val outstr  = subStringRegexp(instr,"""([\+-]?\d*\.?\d*).*""")
    DataString(outstr)
  }

  // custom parse get the numeric part of a string
  def getDirectionPrism(instr: String, checkStr: String, prismtype: String): DataSet = {
    // extract the direction In, Out, Up, Down from instr otherwise
    // convert True/False into horizontal(t=i,f=o) vertical(t=u,f=d)
    var outstr =
      subStringRegexp(instr.toUpperCase,"""^[ +-]*[\d]*\.?[\d \^]*[bB]?([uUdDiIoO]?).*""")

    if (outstr == "")
      if (prismtype == "H")
        if (checkStr == "T")
          outstr = "I"
        else
          outstr = "O"
      else if (prismtype == "V")
        if (checkStr == "T")
          outstr = "U"
        else
          outstr = "D"

    DataString(outstr)
  }

}
