package com.actio

import java.sql.{ResultSetMetaData, ResultSet, Types}
import java.util

/**
  * Created by mauri on 4/05/2016.
  */

class DataSetDBStream(val rs: ResultSet) extends DataSet {
  private var header1: List[String] = Nil
  private var myschema:SchemaDefinition = SchemaUnknown

  def next = {
    var i = 0
    var rows1: List[List[String]] = Nil
      do {
        rows1 = header1.map(h => Option(rs.getObject(h))).map(v => if (v.isEmpty) null else v.get.toString).toList :: rows1
        i += 1
      } while(rs.next() && i < batchSize)

    //TODO: change this to create Data instead of this
    DataSetTableScala(header1, rows1).next
  }

  def hasNext = rs.next()

  override def initBatch = {
    val metaData= rs.getMetaData
    val ordinals = 1 to metaData.getColumnCount
    header1 = (ordinals map metaData.getColumnName).toList
    myschema = SchemaArray(SchemaRecord(ordinals.map(o => SchemaField(metaData.getColumnName(o), true, {
      val t = metaData.getColumnType(o)
      if(t == Types.BIGINT || t == Types.DECIMAL || t == Types.DOUBLE || t == Types.FLOAT || t == Types.INTEGER || t == Types.NUMERIC)
        SchemaNumber(metaData.getColumnDisplaySize(o))
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
