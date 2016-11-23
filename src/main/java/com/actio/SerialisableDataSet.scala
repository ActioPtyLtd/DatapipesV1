package com.actio

import boopickle.Default._
import java.nio.ByteBuffer

sealed trait SerialisableDataSet

object SerialisableDataSet {

  case class SerialisableNothin(label: String) extends SerialisableDataSet
  case class SerialisableDataString(label: String, str: String) extends SerialisableDataSet
  case class SerialisableDataNumeric(label: String, num: BigDecimal) extends SerialisableDataSet
  case class SerialisableDataDate(label: String, date: Long) extends SerialisableDataSet
  case class SerialisableDataBoolean(label: String, bool: Boolean) extends SerialisableDataSet
  case class SerialisableDataRecord(label: String, fields: List[SerialisableDataSet]) extends SerialisableDataSet
  case class SerialisableDataArray(label: String, arrayElems: List[SerialisableDataSet]) extends SerialisableDataSet

  def convert(ds: DataSet): SerialisableDataSet = ds match {
    case Nothin(label) => SerialisableNothin(label)
    case DataString(label, str) => SerialisableDataString(label, str)
    case DataNumeric(label, num) => SerialisableDataNumeric(label, num)
    case DataDate(label, date) => SerialisableDataDate(label, date.getTime())
    case DataBoolean(label, bool) => SerialisableDataBoolean(label, bool)
    case DataRecord(label, fields) => SerialisableDataRecord(label, fields.map(m => convert(m)))
    case DataArray(label, arrayElems) => SerialisableDataArray(label, arrayElems.map(m => convert(m)))
    case d => SerialisableDataRecord(d.label, d.elems.map(m => convert(m)).toList )
  }

  def convert(ds: SerialisableDataSet): DataSet = ds match {
    case SerialisableNothin(label) => Nothin(label)
    case SerialisableDataString(label, str) => DataString(label, str)
    case SerialisableDataNumeric(label, num) => DataNumeric(label, num)
    case SerialisableDataDate(label, date) => DataDate(label, new java.util.Date(date))
    case SerialisableDataBoolean(label, bool) => DataBoolean(label, bool)
    case SerialisableDataRecord(label, fields) => DataRecord(label, fields.map(m => convert(m)))
    case SerialisableDataArray(label, arrayElems) => DataArray(label, arrayElems.map(m => convert(m)))
  }

  def unpickle(bytes: Array[Byte]): DataSet = {
    val bb = ByteBuffer.wrap(new Array[Byte](bytes.length))
    bb.put(bytes)
    bb.flip()
    SerialisableDataSet.convert(Unpickle[SerialisableDataSet].fromBytes(bb))
  }

}
