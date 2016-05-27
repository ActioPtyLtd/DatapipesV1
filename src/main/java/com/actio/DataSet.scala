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

  def schema: SchemaDefinition = SchemaUnknown

  private[actio] def getOutputDelimiter: String = {
    return outputDelimiter
  }

  def setOutputDelimiter(outputDelimiter: String) {
    this.outputDelimiter = outputDelimiter
  }

  def getCustomHeader: String = {
    return customHeader
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



  def hasNext: Boolean

  def next: Data //= getNextBatch.toData





  // not sure why I need this, but it prevents compiler errors
  override def minBy[B](f: Data => B)(implicit cmp: Ordering[B]): Data = null
  override def maxBy[B](f: Data => B)(implicit cmp: Ordering[B]): Data = null
  override def max[B >: Data](implicit cmp: Ordering[B]): Data = null
  override def min[B >: Data](implicit cmp: Ordering[B]): Data = null
}