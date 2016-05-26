package com.actio

import java.sql.ResultSet
import java.util

/**
  * Created by mauri on 4/05/2016.
  */
class DataSetDBStream(val rs: ResultSet) extends DataSet {
  var first = 0
  var more = true
  var header1: List[String] = Nil

  def next = {
    var i = 0
    var rows1: List[List[String]] = Nil
    if(more) {
      do {
        rows1 = header.map(h => Option(rs.getObject(h))).map(v => if (v.isEmpty) null else v.get.toString).toList :: rows1
        i += 1
        more = rs.next()
      } while(more && i < batchSize)
    }
    DataSetTableScala(header1, rows1).toData
  }

  def hasNext = {
    if (!more)
      false
    else {
      more = rs.next()
      first += 1
      (first==1) || more
    }
  }

  override def initBatch = {
    val metaData= rs.getMetaData
    val ordinals = 1 to metaData.getColumnCount
    header1 = (ordinals map metaData.getColumnName).toList
  }


  // really hope to be able to remove most of these

  @throws(classOf[Exception])
  override def sizeOfBatch: Int = batchSize

  @throws(classOf[Exception])
  override def set(_results: util.List[String]): Unit = ???


  import scala.collection.JavaConverters._

  @throws(classOf[Exception])
  override def getColumnHeader: util.List[String] = header1.asJava

  @throws(classOf[Exception])
  override def getAsListOfColumnsBatch(batchLen: Int): util.List[util.List[String]] = ???

  @throws(classOf[Exception])
  override def getColumnHeaderStr: String = ???

  @throws(classOf[Exception])
  override def getAsList: util.List[String] = ???

  @throws(classOf[Exception])
  override def setWithFields(_results: util.List[util.List[String]]): Unit = ???

  @throws(classOf[Exception])
  override def getAsListOfColumns: util.List[util.List[String]] = ???
}
