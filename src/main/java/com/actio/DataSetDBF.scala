package com.actio

import java.io._
import java.text.SimpleDateFormat
import com.linuxense.javadbf._
import java.util

/**
 * Created by mauri on 14/04/2016.
 */

object DataSetDBF {

  val ROW = "row"

  def field2ds(row: Array[Object], fields: List[DBFField]): DataRecord = DataRecord(ROW, fields.zipWithIndex.map(f => {
    val t = f._1.getType()

    if (Option(row(f._2)).isDefined) {
      if (t == DBFDataType.NUMERIC || t == DBFDataType.FLOATING_POINT || t == DBFDataType.LONG || t == DBFDataType.CURRENCY) {
        DataNumeric(f._1.getName, BigDecimal(row(f._2).toString))
      } else if (t == DBFDataType.DATE) {
        DataString(f._1.getName, new SimpleDateFormat("yyyy-MM-dd").format(row(f._2).asInstanceOf[java.util.Date]))
      } else if (t == DBFDataType.TIMESTAMP) {
        DataString(f._1.getName, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(row(f._2).asInstanceOf[java.util.Date]))
      } else {
        DataString(f._1.getName, row(f._2).toString.trim())
      }
    } else {
      Nothin(f._1.getName)
    }
  }).toList)
}

class DataSetDBF(private val reader: InputStream) extends DataSet {
  private val stream = new DBFReader(reader)
  private val fields = (0 until stream.getFieldCount).map(stream.getField).toList

  private val batchSize = 100

  override lazy val elems = new Iterator[DataSet] {
    private var row = stream.nextRecord()

    override def hasNext: Boolean = row != null

    override def next(): DataSet = {
      var i = 0
      var recs: List[DataRecord] = Nil

      do {
        recs = DataSetDBF.field2ds(row, fields) :: recs

        i += 1
        row = stream.nextRecord()
      } while (row != null && i < batchSize)

      DataArray(recs)
    }
  }

  override def schema: SchemaDefinition = SchemaUnknown

  override def label: String = ""
}
