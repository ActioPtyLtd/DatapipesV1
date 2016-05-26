package com.actio

/**
  * Created by jim on 7/8/2015.
  */

import com.actio.dpsystem.DPSystemConfigurable
import com.typesafe.config.Config
import java.sql.ResultSet

abstract class DataSet(val batchSize: Int) extends DPSystemConfigurable with Iterator[Data] {
  var key: DataSetKey = new DataSetKey

  def this() = this(500)

  def setChunk(chunkStart: Int, chunkEnd: Int, maxChunk: Int) {
    key.chunkStart = chunkStart
    key.chunkEnd = chunkEnd
    key.maxChunk = maxChunk
  }

  // TODO: change to protected later
  var customHeader: String = null

  @throws(classOf[Exception])
  def sizeOfBatch: Int

  def getSchema: Schema = {
    return schema
  }

  def setSchema(schema: Schema) {
    this.schema = schema
  }

  private var schema: Schema = null

  private[actio] def getOutputDelimiter: String = {
    return outputDelimiter
  }

  def setOutputDelimiter(outputDelimiter: String) {
    this.outputDelimiter = outputDelimiter
  }

  def getCustomHeader: String = {
    return customHeader
  }

  def setCustomHeader(customHeader: String) {
    this.customHeader = customHeader
  }

  private[actio] var outputDelimiter: String = ","

  @throws(classOf[Exception])
  def set(_results: java.util.List[String])

  @throws(classOf[Exception])
  def setWithFields(_results: java.util.List[java.util.List[String]])

  @throws(classOf[Exception])
  def getAsList: java.util.List[String]

  @throws(classOf[Exception])
  def getAsListOfColumns: java.util.List[java.util.List[String]]

  @throws(classOf[Exception])
  def initBatch

  @throws(classOf[Exception])
  def getAsListOfColumnsBatch(batchLen: Int): java.util.List[java.util.List[String]]

  @throws(classOf[Exception])
  def getColumnHeader: java.util.List[String]

  @throws(classOf[Exception])
  def getColumnHeaderStr: String

  @throws(classOf[Exception])
  override def setConfig(_conf: Config, _master: Config) {
    super.setConfig(_conf, _master)
    if (config.hasPath("customHeader")) customHeader = config.getString("customHeader")
    if (config.hasPath("outputDelimiter")) outputDelimiter = config.getString("outputDelimiter")
  }

  @throws(classOf[Exception])
  def dump = { }

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

  def hasNext: Boolean

  def next: Data //= getNextBatch.toData

  def toData = DataRecord(List(DataField("properties", DataRecord(header.map(DataField(_,NoData)).toList).asInstanceOf[Data]),
                          DataField("data",DataArray(rows.map(r => DataRecord(
                            header.map(h => DataField(h,DataString(this.getValue(r, h)).asInstanceOf[Data])).toList).asInstanceOf[Data]).toList)))).asInstanceOf[Data]



  // not sure why I need this, but it prevents a compiler errors
  override def minBy[B](f: Data => B)(implicit cmp: Ordering[B]): Data = null
  override def maxBy[B](f: Data => B)(implicit cmp: Ordering[B]): Data = null
  override def max[B >: Data](implicit cmp: Ordering[B]): Data = null
  override def min[B >: Data](implicit cmp: Ordering[B]): Data = null
}