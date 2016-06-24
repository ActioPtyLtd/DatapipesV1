package com.actio

/**
  * Created by mauri on 25/05/2016.
  */

case class DataString(label: String = "", str: String) extends DataSet {

  override def stringOption = Option(str)

  override def schema = SchemaString(label, 0)
}

case class DataNumeric(label: String, num: BigDecimal) extends DataSet {

  override def stringOption = Some(num.toString())

  override def schema = SchemaNumber(label,0,0)
}

case class DataBoolean(label: String, bool: Boolean) extends DataSet {

  override def stringOption = Some(bool.toString)

  override def schema = SchemaBoolean(label)
}

case class DataRecord(label: String, fields: List[DataSet]) extends DataSet {

  override def apply(field: String) = fields.find(f => f.label == field).getOrElse(Nothin())

  override def elems = fields.toIterator

  override def schema = SchemaRecord(label, fields.map(_.schema))
}

object DataRecord {

  def apply(fields: List[DataSet]) = new DataRecord("", fields)
}

case class DataArray(label: String, arrayElems: List[DataSet]) extends DataSet {

  override def apply(ord: Int) = arrayElems.lift(ord).getOrElse(Nothin())

  override def elems = arrayElems.toIterator

  override def schema = SchemaArray(label, arrayElems.head.schema)      // could update this to check for maximum amount of fields rather than just first
}

object DataArray {

  def apply(arrayElems: List[DataSet]) = new DataArray("", arrayElems)
}

sealed abstract class Key
case class Ord(ord: Int) extends Key
case class Label(label: String) extends Key

// TODO: could use help of a library to serialise properly
object Data2Json {

  def toJsonString(data: DataSet): String =
    data match {
      case DataString(_, s) => "\"" + s + "\""
      case DataRecord(key, fs) => toField(key) + "{" + fs.map(f =>
        (if (!f.isInstanceOf[DataRecord] && !f.isInstanceOf[DataArray])
          toField(f.label)
        else "")
          + toJsonString(f)).mkString(",") + "}"
      case DataArray(key, ds) => toField(key) + "[" + ds.map(d => toJsonString(d)).mkString(",") + "]"
      case Nothin(_) => "null"
      case DataNumeric(_, num) => num.setScale(2, BigDecimal.RoundingMode.HALF_UP).underlying().stripTrailingZeros().toPlainString
      case DataBoolean(_, bool) => bool.toString
    }

  def toField(name: String) = if(name.isEmpty) "" else "\"" + name + "\": "

  import org.json4s._
  import org.json4s.jackson.JsonMethods._

  def fromJson4s2Data(label: String,v: JValue): DataSet =
    v match {
      case (js: JString) => DataString(label, js.s)
      case (ji: JInt) => DataNumeric(label, BigDecimal(ji.num))
      case (jdec: JDecimal) => DataNumeric(label, jdec.num)
      case (jdou: JDouble) => DataNumeric(label, jdou.num)
      case (jb: JBool) => DataBoolean(label, jb.value)
      case (ja: JArray) => DataArray(label, ja.arr.map(a => fromJson4s2Data("", a)).toList)
      case (jo: JObject) => DataRecord(label, jo.obj.map(o => fromJson4s2Data(o._1, o._2)).toList)
      case _ => Nothin(label)
    }

  def fromJson2Data(string: String) =
    fromJson4s2Data("", parse(string)) // org.json4s.string2JsonInput(string)


}

