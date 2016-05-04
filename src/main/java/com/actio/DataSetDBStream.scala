package com.actio

import java.sql.ResultSet
import java.util

/**
  * Created by mauri on 4/05/2016.
  */
class DataSetDBStream(val rs: ResultSet) extends DataSetTableScala {
  var iterator: Iterator[Seq[List[String]]] = null

  override def getNextBatch = {
    header1 = header
    rows1 = iterator.next().toList
    DataSetTableScala(header, rows)
  }

  override def isNextBatch = iterator.hasNext

  override def initBatch = {
    val metaData= rs.getMetaData
    val ordinals = 1 to metaData.getColumnCount()
    header1 = (ordinals map metaData.getColumnName).toList

    iterator = new Iterator[List[String]] {
      def hasNext = rs.next()
      def next() = ordinals.map(rs.getObject(_).toString).toList
    }.grouped(100).toIterator
  }
}
