package com.actio

import java.io.{InputStream, BufferedReader}
import java.sql.ResultSet
import java.util

/**
  * Created by mauri on 14/04/2016.
  */
class DataSetFileStream(val reader: InputStream) extends DataSet {
  var lineheader: String = null
  var iterable: Iterator[Seq[String]] = null
  var header1: List[String] = Nil

  def next = {
    val lines = iterable.next().toList

    if(lineheader == null) {
      lineheader = lines.head
      //rows1 = lines.tail map (List(_))
      header1 = List(lineheader)
      new DataSetTableScala(lines).toData
    }
    else {
      //rows1 = lines map (List(_))
      new DataSetTableScala(lineheader :: lines).toData
    }
  }

  def hasNext = iterable.hasNext

  override def initBatch() = {
    iterable = scala.io.Source.fromInputStream(reader,"windows-1252").getLines().grouped(100)
  }

  // really hope to be able to remove most of these

  @throws(classOf[Exception])
  override def sizeOfBatch: Int = 100

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
