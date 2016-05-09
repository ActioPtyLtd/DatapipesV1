package com.actio

import java.io.{InputStream, BufferedReader}
import java.sql.ResultSet
import java.util

/**
  * Created by mauri on 14/04/2016.
  */
class DataSetFileStream(val reader: InputStream) extends DataSetTableScala {
  var lineheader: String = null
  var iterable: Iterator[Seq[String]] = null

  override def getNextBatch: DataSet = {
    val lines = iterable.next().toList

    if(lineheader == null) {
      lineheader = lines.head
      rows1 = lines.tail map (List(_))
      header1 = List(lineheader)
      new DataSetTableScala(lines)
    }
    else {
      rows1 = lines map (List(_))
      new DataSetTableScala(lineheader :: lines)
    }
  }

  override def isNextBatch = iterable.hasNext

  override def initBatch() = {
    iterable = scala.io.Source.fromInputStream(reader,"windows-1252").getLines().grouped(100)
  }
}
