package com.actio

import java.io.{InputStream, BufferedReader}
import java.sql.ResultSet
import java.util

/**
  * Created by mauri on 14/04/2016.
  */
class DataSetFileStream(private val reader: InputStream) extends DataSet {

  private var lineheader: String = null
  private val col1 = "col1"
  private val iterable = scala.io.Source.fromInputStream(reader,"windows-1252").getLines().grouped(100)


  override lazy val elems = new Iterator[DataSet] {
    override def hasNext: Boolean = iterable.hasNext

    override def next(): DataSet = {
      val lines = iterable.next().toList

      if (lineheader == null) {
        lineheader = lines.head
        lines2data(lines)
      }
      else {
        lines2data(lineheader :: lines)
      }
    }
  }

  private def lines2data(lines: List[String]) = DataArray(lines.map(l => DataRecord(List(DataString(col1,l)))))

  override def schema = SchemaArray(SchemaRecord(List(SchemaString(col1,0))))

  override def label: String = ""
}
