package com.actio

import java.sql.{ResultSetMetaData, ResultSet, Types}
import java.util

/**
  * Created by mauri on 4/05/2016.
  */

class DataSetDBStream(val rs: ResultSet, val batchSize: Int) extends DataSet {
  private var header: List[String] = Nil
  private var myschema:SchemaDefinition = SchemaUnknown
  init()

  def next = {
    var i = 0

    var recs: List[DataRecord] = Nil
    do {
      recs = DataRecord(header.map(c => (c,Option(rs.getObject(c)))).map(v => DataField(v._1, if (v._2.isEmpty) NoData else DataString(v._2.toString)))) :: recs
      i += 1
    } while(rs.next() && i < batchSize)

    DataArray(recs)
  }

  def hasNext = rs.next()

  def init() = {
    val metaData= rs.getMetaData
    val ordinals = 1 to metaData.getColumnCount

    header = (ordinals map metaData.getColumnName).toList

    myschema = SchemaArray(SchemaRecord(ordinals.map(o => SchemaField(metaData.getColumnName(o), true, {
      val t = metaData.getColumnType(o)

      if(t == Types.BIGINT || t == Types.DECIMAL || t == Types.DOUBLE || t == Types.FLOAT || t == Types.INTEGER || t == Types.NUMERIC)
        SchemaNumber(metaData.getColumnDisplaySize(o), 0)
      else if(t == Types.DATE || t == Types.TIME || t == Types.TIMESTAMP)
        SchemaDate("yyyy-MM-dd")
      else
        SchemaString(metaData.getColumnDisplaySize(o))
    })).toList))
  }

  override def schema: SchemaDefinition = myschema


}
