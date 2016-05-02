package com.actio

/**
  * Created by jim on 7/8/2015.
  */

import com.actio.dpsystem.DPSystemConfigurable
import com.typesafe.config.Config
import java.sql.ResultSet
import java.util.LinkedList

object DataSet {
  def flattenRows(rows: java.util.List[java.util.List[String]], _outputDelimiter: String): java.util.List[String] = {
    val newRows: java.util.List[String] = new java.util.LinkedList[String]
    import scala.collection.JavaConversions._
    for (columns <- rows) newRows.add(columnsToRow(columns, _outputDelimiter))
    return newRows
  }

  def columnsToRow(columns: java.util.List[String], _outputDelimiter: String): String = {
    var row: String = ""
    var count: Int = 1
    val maxCols: Int = columns.size
    import scala.collection.JavaConversions._
    for (field <- columns) {
      row = row + field
      if (({
        count += 1; count - 1
      }) < maxCols) row = row + _outputDelimiter
    }
    return row
  }
}

abstract class DataSet extends DPSystemConfigurable {
  var key: DataSetKey = new DataSetKey

  def setKey(_key: DataSetKey) {
    key = _key
  }

  def setChunk(chunkStart: Int, chunkEnd: Int, maxChunk: Int) {
    key.chunkStart = chunkStart
    key.chunkEnd = chunkEnd
    key.maxChunk = maxChunk
  }

  // TODO: change to protected later
  var customHeader: String = null

  @throws(classOf[Exception])
  def size: Int

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

  private[actio] def getBatchSize: Int = {
    return batchSize
  }

  def setBatchSize(batchSize: Int) {
    this.batchSize = batchSize
  }

  private[actio] var batchSize: Int = 500

  @throws(classOf[Exception])
  def GetRow: Array[String]

  @throws(classOf[Exception])
  def NextRow: Boolean

  @throws(classOf[Exception])
  def set(_results: ResultSet)

  @throws(classOf[Exception])
  def set(_results: java.util.List[String])

  @throws(classOf[Exception])
  def setWithFields(_results: java.util.List[java.util.List[String]])

  @throws(classOf[Exception])
  def getResultSet: ResultSet

  @throws(classOf[Exception])
  def getAsList: java.util.List[String]

  @throws(classOf[Exception])
  def getAsListOfColumns: java.util.List[java.util.List[String]]

  @throws(classOf[Exception])
  def initBatch

  @throws(classOf[Exception])
  def isNextBatch: Boolean

  @throws(classOf[Exception])
  def getNextBatch: DataSet

  @throws(classOf[Exception])
  def getAsListOfColumnsBatch(batchLen: Int): java.util.List[java.util.List[String]]

  @throws(classOf[Exception])
  def getColumnHeader: java.util.List[String]

  @throws(classOf[Exception])
  def getColumnHeaderStr: String

  @throws(classOf[Exception])
  def FromRowGetField(rowIndex: Int, label: String): String

  @throws(classOf[Exception])
  def FromRowGetField(rowIndex: Int, label: Int): String

  @throws(classOf[Exception])
  override def setConfig(_conf: Config, _master: Config) {
    super.setConfig(_conf, _master)
    if (config.hasPath("customHeader")) customHeader = config.getString("customHeader")
    if (config.hasPath("outputDelimiter")) outputDelimiter = config.getString("outputDelimiter")
  }

  @throws(classOf[Exception])
  def dump

  import scala.collection.JavaConverters._

  def rows: List[List[String]] = getAsListOfColumns.asScala.map(_.asScala.toList).toList
  def header: List[String] = getColumnHeader.asScala.toList


  def getOrdinalOfColumn(columnName: String) = header.indexWhere(_ == columnName)

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


  def getNumberOfColumns = header.length

  def getEmptyRow = List.fill(getNumberOfColumns)(null)

  def getValue(row: List[String], columnName: String) = row(getOrdinalOfColumn(columnName))

  def isEmpty = rows.isEmpty

}