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
  init()

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

  def init() = {
    iterable = scala.io.Source.fromInputStream(reader,"windows-1252").getLines().grouped(100)
  }

  override def schema = SchemaArray(SchemaRecord(List(SchemaField("col1", true, SchemaString(0)))))

}
