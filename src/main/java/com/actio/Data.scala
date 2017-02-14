package com.actio

import java.io.InputStream
import java.text.SimpleDateFormat

import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * Created by mauri on 25/05/2016.
 */

case class DataString(label: String, str: String) extends DataSet {

  def this(_str: String) {
    this ("string",_str)
  }
  override def stringOption: Option[String] = Option(str)

  override def schema: SchemaDefinition = SchemaString(label, 0)
}

object DataString {

  def apply(str: String): DataString = new DataString( str)

}

case class DataNumeric(label: String, num: BigDecimal) extends DataSet {

  override def stringOption: Option[String] = Some(num.toString())

  override def schema: SchemaDefinition = SchemaNumber(label, 0, 0)
}

object DataNumeric {

  private val name = "numeric"

  def apply(num: BigDecimal): DataNumeric = DataNumeric(name, num)

  def apply(num: Int): DataNumeric = DataNumeric(name, BigDecimal(num))
}

case class DataDate(label: String, date: java.util.Date) extends DataSet {

  override def stringOption: Option[String] = Some(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S").format(date))

  override def schema: SchemaDefinition = SchemaDate(label, "yyyy-MM-dd")
}

object DataDate {

  def apply(date: java.util.Date): DataDate = DataDate("date", date)
}

case class DataBoolean(label: String, bool: Boolean) extends DataSet {

  override def stringOption: Option[String] = Some(bool.toString)

  override def schema: SchemaDefinition = SchemaBoolean(label)
}

object DataBoolean {

  def apply(bool: Boolean): DataBoolean = DataBoolean("bool", bool)
}

case class DataRecord(label: String, fields: List[DataSet]) extends DataSet {

  lazy val mapFields: Map[String, DataSet] = fields.map(f => f.label -> f).toMap

  override def apply(field: String): DataSet = mapFields.getOrElse(field, Nothin())

  override def apply(ord: Int): DataSet = fields.lift(ord).getOrElse(Nothin())

  override def elems: Iterator[DataSet] = fields.toIterator

  override def schema: SchemaDefinition = SchemaRecord(label, fields.map(_.schema))
}

object DataRecord {

  def apply(fields: List[DataSet]): DataRecord = new DataRecord("record", fields)
  def apply(fields: DataSet*): DataRecord = new DataRecord("record", fields.toList)
  def apply(label: String, fields: DataSet*): DataRecord = new DataRecord(label, fields.toList)
}

case class DataArray(label: String, arrayElems: List[DataSet]) extends DataSet {

  override def apply(ord: Int): DataSet = arrayElems.lift(ord).getOrElse(Nothin())

  override def elems: Iterator[DataSet] = arrayElems.toIterator

  //TODO: could update this to check for maximum amount of fields rather than just first
  override def schema: SchemaDefinition = SchemaArray(label, arrayElems.headOption.map(_.schema).getOrElse(SchemaUnknown))
}

object DataArray {

  def apply(arrayElems: DataSet*): DataArray = new DataArray("array", arrayElems.toList)
  def apply(arrayElems: List[DataSet]): DataArray = new DataArray("array", arrayElems)
}

sealed abstract class Key
case class Ord(ord: Int) extends Key
case class Label(label: String) extends Key

// TODO: could use help of a library to serialise properly
object Data2Json {

  def toJsonString(data: DataSet): String =
    data match {
      case DataString(_, s) => "\"" + s + "\""
    //  case DataString(l , s) => "\"" +l + "\" :  \"" + s + "\""
      case DataRecord(key, fs) =>
        toField(key) +
          "{" + fs.map(f =>
            (if (!f.isInstanceOf[DataRecord] && !f.isInstanceOf[DataArray]) toField(f.label) else "")
              + toJsonString(f)).mkString(",") +
          "}"
      case DataArray(key, ds) =>
        toField(key) +
          "[" + ds.map(d => "{"+ toJsonString(d) + "}").mkString(",") + "]"
      case Nothin(_) => "null"
      case DataNumeric(_, num) => num.setScale(2, BigDecimal.RoundingMode.HALF_UP).underlying().stripTrailingZeros().toPlainString
      case DataBoolean(_, bool) => bool.toString
      case DataDate(_, date) => date.toString
      case e => toField(e.label) +
        "{" + e.elems.map(toJsonString).mkString(",") + "}"
    }

  def toField(name: String): String = if (name.isEmpty) "" else "\"" + name + "\": "

  def fromJson4s2Data(label: String, v: JValue): DataSet =
    v match {
      case (js: JString) => DataString(label, js.s)
      case (ji: JInt) => DataNumeric(label, BigDecimal(ji.num))
      case (jDecimal: JDecimal) => DataNumeric(label, jDecimal.num)
      case (jDouble: JDouble) => DataNumeric(label, jDouble.num)
      case (jb: JBool) => DataBoolean(label, jb.value)
      case (ja: JArray) => DataArray(label, ja.arr.map(a => fromJson4s2Data("item", a)).toList)
      case (jo: JObject) => DataRecord(label, jo.obj.map(o => fromJson4s2Data(o._1, o._2)).toList)
      case _ => Nothin(label)
    }

  def fromJson2Data(string: String): DataSet = fromJson4s2Data("", parse(string))

  def fromFileStream2Json2Data(label:String, inputStream:InputStream): DataSet = DataRecord("fileContent", List(DataRecord(label,fromJson4s2Data("record", parse(inputStream)).elems.toList)))
}

