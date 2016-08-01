package com.actio

import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * Created by mauri on 25/05/2016.
 */

case class DataString(label: String, str: String) extends DataSet {

  override def stringOption: Option[String] = Option(str)

  override def schema: SchemaDefinition = SchemaString(label, 0)
}

object DataString {

  def apply(str: String): DataString = DataString("", str)
}

case class DataNumeric(label: String, num: BigDecimal) extends DataSet {

  override def stringOption: Option[String] = Some(num.toString())

  override def schema: SchemaDefinition = SchemaNumber(label, 0, 0)
}

object DataNumeric {

  def apply(num: BigDecimal): DataNumeric = DataNumeric("", num)
}

case class DataBoolean(label: String, bool: Boolean) extends DataSet {

  override def stringOption: Option[String] = Some(bool.toString)

  override def schema: SchemaDefinition = SchemaBoolean(label)
}

object DataBoolean {

  def apply(bool: Boolean): DataBoolean = DataBoolean("", bool)
}

case class DataRecord(label: String, fields: List[DataSet]) extends DataSet {

  override def apply(field: String): DataSet = fields.find(f => f.label == field).getOrElse(Nothin())

  override def apply(ord: Int): DataSet = fields.lift(ord).getOrElse(Nothin())

  override def elems: Iterator[DataSet] = fields.toIterator

  override def schema: SchemaDefinition = SchemaRecord(label, fields.map(_.schema))
}

object DataRecord {

  def apply(fields: List[DataSet]): DataRecord = new DataRecord("", fields)
}

case class DataArray(label: String, arrayElems: List[DataSet]) extends DataSet {

  override def apply(ord: Int): DataSet = arrayElems.lift(ord).getOrElse(Nothin())

  override def elems: Iterator[DataSet] = arrayElems.toIterator

  //TODO: could update this to check for maximum amount of fields rather than just first
  override def schema: SchemaDefinition = SchemaArray(label, arrayElems.headOption.map(_.schema).getOrElse(SchemaUnknown))
}

object DataArray {

  def apply(arrayElems: DataSet*): DataArray = new DataArray("", arrayElems.toList)
  def apply(arrayElems: List[DataSet]): DataArray = new DataArray("", arrayElems)
}

sealed abstract class Key
case class Ord(ord: Int) extends Key
case class Label(label: String) extends Key

// TODO: could use help of a library to serialise properly
object Data2Json {

  def toJsonString(data: DataSet): String =
    data match {
      case DataString(_, s) => "\"" + s + "\""
      case DataRecord(key, fs) =>
        toField(key) +
          "{" + fs.map(f =>
            (if (!f.isInstanceOf[DataRecord] && !f.isInstanceOf[DataArray]) toField(f.label) else "")
              + toJsonString(f)).mkString(",") +
          "}"
      case DataArray(key, ds) =>
        toField(key) +
          "[" + ds.map(d => toJsonString(d)).mkString(",") + "]"
      case Nothin(_) => "null"
      case DataNumeric(_, num) => num.setScale(2, BigDecimal.RoundingMode.HALF_UP).underlying().stripTrailingZeros().toPlainString
      case DataBoolean(_, bool) => bool.toString
    }

  def toField(name: String): String = if (name.isEmpty) "" else "\"" + name + "\": "

  def fromJson4s2Data(label: String, v: JValue): DataSet =
    v match {
      case (js: JString) => DataString(label, js.s)
      case (ji: JInt) => DataNumeric(label, BigDecimal(ji.num))
      case (jDecimal: JDecimal) => DataNumeric(label, jDecimal.num)
      case (jDouble: JDouble) => DataNumeric(label, jDouble.num)
      case (jb: JBool) => DataBoolean(label, jb.value)
      case (ja: JArray) => DataArray(label, ja.arr.map(a => fromJson4s2Data("", a)).toList)
      case (jo: JObject) => DataRecord(label, jo.obj.map(o => fromJson4s2Data(o._1, o._2)).toList)
      case _ => Nothin(label)
    }

  def fromJson2Data(string: String): DataSet = fromJson4s2Data("", parse(string))
}

