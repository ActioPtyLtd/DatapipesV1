package com.actio

import java.sql.{ ResultSetMetaData, ResultSet, Types }
import java.util

import scala.collection.mutable

/**
 * Created by mauri on 4/05/2016.
 */

class DataSetDBStream(private val rs: ResultSet, val batchSize: Int) extends DataSet {

  private val metaData = rs.getMetaData
  private val ordinals = 1 to metaData.getColumnCount
  private val header = ordinals.map(o => metaData.getColumnName(o)).toList

  private val mySchema = ordinals.map(o => {
    val t = metaData.getColumnType(o)

    if (t == Types.BIGINT || t == Types.DECIMAL || t == Types.DOUBLE || t == Types.FLOAT || t == Types.INTEGER || t == Types.NUMERIC) {
      SchemaNumber(metaData.getColumnName(o), metaData.getColumnDisplaySize(o), 0)
    }
    else if (t == Types.DATE || t == Types.TIME || t == Types.TIMESTAMP) {
      SchemaDate(metaData.getColumnName(o), "yyyy-MM-dd")
    }
    else {
      SchemaString(metaData.getColumnName(o), metaData.getColumnDisplaySize(o))
    }
  }).toList

  override lazy val elems = new Iterator[DataSet] {
    private var hNext = rs.next()

    override def hasNext: Boolean = hNext

    override def next() = {
      var i = 0
      var recs = mutable.MutableList[DataRecord]()

      do {
        recs += DataRecord("row", (header zip mySchema).map(c => (c._1, Option(rs.getObject(c._1)), c._2)).map(v =>
          if (v._2.isDefined) {
            v._3 match {
              case _: SchemaNumber => DataNumeric(v._1,
                BigDecimal(BigDecimal(v._2.get.toString)
                  .underlying()
                  .stripTrailingZeros()
                  .toPlainString))
              case _: SchemaDate => DataDate(v._1, v._2.get.asInstanceOf[java.util.Date])
              case _ => DataString(v._1, v._2.get.toString)
            }
          }
          else {
            Nothin(v._1)
          }))

        i += 1
        hNext = rs.next()
      } while ( hNext && i < batchSize)

      DataArray(recs.toList)
    }
  }

  override def schema: SchemaDefinition = SchemaArray(SchemaRecord(mySchema))

  override def label: String = ""
}
