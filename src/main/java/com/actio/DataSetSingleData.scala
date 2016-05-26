package com.actio

import java.sql.ResultSet
import java.util

/**
  * Created by mauri on 26/05/2016.
  */
trait SingleValueIterator {
  private var boolNext = true

  def hasNext: Boolean = {
    val bn = boolNext
    boolNext = false
    bn
  }
}

class DataSetSingleData(d: Data) extends DataSet with SingleValueIterator {
  @throws(classOf[Exception])
  override def sizeOfBatch: Int = ???

  override def next: Data = d

  @throws(classOf[Exception])
  override def set(_results: util.List[String]): Unit = ???

  @throws(classOf[Exception])
  override def getColumnHeader: util.List[String] = ???

  @throws(classOf[Exception])
  override def getAsListOfColumnsBatch(batchLen: Int): util.List[util.List[String]] = ???

  @throws(classOf[Exception])
  override def getColumnHeaderStr: String = ???

  @throws(classOf[Exception])
  override def getAsList: util.List[String] = ???

  @throws(classOf[Exception])
  override def setWithFields(_results: util.List[util.List[String]]): Unit = ???

  @throws(classOf[Exception])
  override def initBatch: Unit = ???

  @throws(classOf[Exception])
  override def getAsListOfColumns: util.List[util.List[String]] = ???
}