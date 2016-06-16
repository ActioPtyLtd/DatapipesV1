package com.actio

/**
  * Created by mauri on 25/05/2016.
  */

sealed abstract class Data {

  def apply(ord: Int): Data = NoData()

  def apply(field: String): Data = NoData()

  def stringOption: Option[String] = None

  def toOption: Option[Data] = this match {
    case NoData(_) => None
    case data => Some(data)
  }

  def value(keys: List[Key]): Data = keys match {
    case Nil => this
    case Ord(ord)::t => this(ord).value(t)
    case Label(lbl)::t => this(lbl).value(t)
    case _ => NoData() }

  def elems: Iterable[Data] = Iterable.empty

  def isEmpty = this.toOption.isDefined

  def label: String

  def schema: SchemaDefinition = SchemaUnknown
}

case class NoData(label: String) extends Data

object NoData {

  def apply() = new NoData("")
}

case class DataString(label: String = "", str: String) extends Data {

  override def stringOption = Option(str)

  override def schema = SchemaString(label, 0)
}

case class DataNumeric(label: String, num: BigDecimal) extends Data {

  override def stringOption = Some(num.toString())

  override def schema = SchemaNumber(label,0,0)
}

case class DataBoolean(label: String, bool: Boolean) extends Data {

  override def stringOption = Some(bool.toString)

  override def schema = SchemaBoolean(label)
}

case class DataRecord(label: String, fields: List[Data]) extends Data {

  override def apply(field: String) = fields.find(f => f.label == field).getOrElse(NoData())

  override def elems = fields

  override def schema = SchemaRecord(label, fields.map(_.schema))
}

object DataRecord {

  def apply(fields: List[Data]) = new DataRecord("", fields)
}

case class DataArray(label: String, arrayElems: List[Data]) extends Data {

  override def apply(ord: Int) = arrayElems.lift(ord).getOrElse(NoData())

  override def elems = arrayElems

  override def schema = SchemaArray(label, arrayElems.head.schema)      // could update this to check for maximum amount of fields rather than just first
}

object DataArray {

  def apply(arrayElems: List[Data]) = new DataArray("", arrayElems)
}

sealed abstract class Key
case class Ord(ord: Int) extends Key
case class Label(label: String) extends Key

// TODO: could use help of a library to serialise properly
object Data2Json {

  def toJsonString(data: Data): String =
    data match {
      case DataString(_, s) => "\"" + s + "\""
      case DataRecord(key, fs) => toField(key) + "{" + fs.map(f =>
        (if (!f.isInstanceOf[DataRecord] && !f.isInstanceOf[DataArray])
          toField(f.label)
        else "")
          + toJsonString(f)).mkString(",") + "}"
      case DataArray(key, ds) => toField(key) + "[" + ds.map(d => toJsonString(d)).mkString(",") + "]"
      case NoData(_) => "null"
      case DataNumeric(_, num) => num.setScale(2, BigDecimal.RoundingMode.HALF_UP).underlying().stripTrailingZeros().toPlainString
      case DataBoolean(_, bool) => bool.toString
    }

  def toField(name: String) = if(name.isEmpty) "" else "\"" + name + "\": "

  import org.json4s._
  import org.json4s.jackson.JsonMethods._

  def fromJson4s2Data(label: String,v: JValue): Data =
    v match {
      case(js: JString) => DataString(label, js.s)
      case(ji: JInt) => DataNumeric(label, BigDecimal(ji.num))
      case(jdec: JDecimal) => DataNumeric(label, jdec.num)
      case(jdou: JDouble) => DataNumeric(label, jdou.num)
      case(jb: JBool) => DataBoolean(label, jb.value)
      case(ja: JArray) => DataArray(label, ja.arr.map(a => fromJson4s2Data("", a)).toList)
      case(jo: JObject ) => DataRecord(label, jo.obj.map(o => fromJson4s2Data(o._1, o._2)).toList)
      case _ => NoData(label)

  }

  def fromJson2Data(string: String) =
    fromJson4s2Data("", parse(string)) // org.json4s.string2JsonInput(string)


}