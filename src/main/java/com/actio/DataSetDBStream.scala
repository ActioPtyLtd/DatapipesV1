package com.actio

import java.sql.ResultSet
import java.util

/**
  * Created by mauri on 4/05/2016.
  */
class DataSetDBStream(val rs: ResultSet) extends DataSetTableScala {
  var first = 0
  var more = true

  override def getNextBatch = {
    var i = 0
    if(more) {
      do {
        rows1 = header.map(h => Option(rs.getObject(h))).map(v => if (v.isEmpty) null else v.get.toString).toList :: rows1
        i += 1
        more = rs.next()
      } while(more && i < getBatchSize)
    }
    DataSetTableScala(header, rows)
  }

  override def isNextBatch = {
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
    rows1 = Nil
  }
}
