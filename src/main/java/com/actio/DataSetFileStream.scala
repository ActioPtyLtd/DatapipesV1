package com.actio

import java.io.{InputStream, BufferedReader}
import java.sql.ResultSet
import java.util

/**
  * Created by mauri on 14/04/2016.
  */
class DataSetFileStream(val reader: InputStream) extends DataSet {
  private var lineheader: String = null
  private var iterable: Iterator[Seq[String]] = null
  private var header1: List[String] = Nil
  private val col1 = "col1"

  def next = {
    val lines = iterable.next().toList

    if(lineheader == null) {
      lineheader = lines.head
      //rows1 = lines.tail map (List(_))
      header1 = List(lineheader)
      //TODO: change this to create Data instead of this
      lines2data(lines)
    }
    else {
      //rows1 = lines map (List(_))
      //TODO: change this to create Data instead of this
      lines2data(lineheader :: lines)
    }
  }

  private def lines2data(lines: List[String]) = new DataArray(lines.map(l => new DataRecord(List(DataField(col1, DataString(l))))))

  def hasNext = iterable.hasNext

  override def initBatch() = {
    iterable = scala.io.Source.fromInputStream(reader,"windows-1252").getLines().grouped(100)
  }

  override def schema = SchemaArray(SchemaRecord(List(SchemaField("col1", true, SchemaString(0)))))

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
