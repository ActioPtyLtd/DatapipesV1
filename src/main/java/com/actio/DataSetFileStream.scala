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

  override def sizeOfBatch(): Int = 0

  def dump() = { }

  def set(_results: ResultSet): Unit = ???

  def set(_results: util.List[String]): Unit = ???

  def getNextBatch: DataSet = {
    val lines = iterable.next().toList

    if(lineheader == null) {
      lineheader = lines.head
      new DataSetTableScala(lines)
    }
    else
      new DataSetTableScala(lineheader :: lines)
  }

  def getColumnHeader: util.List[String] = ???

  def getResultSet: ResultSet = ???

  def getAsListOfColumnsBatch(batchLen: Int): util.List[util.List[String]] = ???

  def getColumnHeaderStr: String = ???

  def isNextBatch = iterable.hasNext

  def GetRow(): Array[String] = ???

  def getAsList: util.List[String] = ???

  def setWithFields(_results: util.List[util.List[String]]): Unit = ???

  def NextRow(): Boolean = ???

  def FromRowGetField(rowIndex: Int, label: String): String = ???

  def FromRowGetField(rowIndex: Int, label: Int): String = ???

  def initBatch() = {
    iterable = scala.io.Source.fromInputStream(reader,"windows-1252").getLines().grouped(100).toIterator
  }

  def getAsListOfColumns: util.List[util.List[String]] = ???
}
