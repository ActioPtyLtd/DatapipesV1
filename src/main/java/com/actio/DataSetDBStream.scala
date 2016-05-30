package com.actio

import java.sql.{ResultSetMetaData, ResultSet, Types}
import java.util

/**
  * Created by mauri on 4/05/2016.
  */

class DataSetDBStream(val rs: ResultSet) extends DataSet {
  private var header: List[String] = Nil
  private var myschema:SchemaDefinition = SchemaUnknown

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

  override def initBatch = {
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


  // really hope to be able to remove most of these

  @throws(classOf[Exception])
  override def sizeOfBatch: Int = batchSize

  @throws(classOf[Exception])
  override def set(_results: util.List[String]): Unit = ???


  import scala.collection.JavaConverters._

  @throws(classOf[Exception])
  override def getColumnHeader: util.List[String] = header.asJava

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
