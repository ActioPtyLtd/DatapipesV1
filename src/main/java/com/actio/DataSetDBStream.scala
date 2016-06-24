package com.actio

import java.sql.{ResultSetMetaData, ResultSet, Types}
import java.util

/**
  * Created by mauri on 4/05/2016.
  */

class DataSetDBStream(private val rs: ResultSet, val batchSize: Int) extends DataSet {

  private val metaData = rs.getMetaData
  private val ordinals = 1 to metaData.getColumnCount
  private val header = (ordinals map metaData.getColumnName).toList

  private val myschema = SchemaArray(SchemaRecord(ordinals.map(o => {
    val t = metaData.getColumnType(o)

    if(t == Types.BIGINT || t == Types.DECIMAL || t == Types.DOUBLE || t == Types.FLOAT || t == Types.INTEGER || t == Types.NUMERIC)
      SchemaNumber(metaData.getColumnName(o), metaData.getColumnDisplaySize(o), 0)
    else if(t == Types.DATE || t == Types.TIME || t == Types.TIMESTAMP)
      SchemaDate(metaData.getColumnName(o), "yyyy-MM-dd")
    else
      SchemaString(metaData.getColumnName(o), metaData.getColumnDisplaySize(o))
  }).toList))

  override lazy val elems = new Iterator[DataSet] {
    override def hasNext: Boolean = rs.next()

    override def next() = {
      var i = 0
      var recs: List[DataRecord] = Nil

      do {
        recs = DataRecord(header.map(c => (c,Option(rs.getObject(c)))).map(v => if (v._2.isEmpty) Nothin(v._1) else DataString(v._1, v._2.get.toString))) :: recs
        i += 1
      } while(rs.next() && i < batchSize)

      DataArray(recs)
    }
  }

  override def schema: SchemaDefinition = myschema

  override def label: String = ""
}
