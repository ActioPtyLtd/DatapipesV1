package com.actio

/**
  * Created by mauri on 25/05/2016.
  */

sealed abstract class Data(val label: String) {
  def apply(ord: Int): Data = NoData()
  def apply(field: String): Data = NoData()
  def valueOption: Option[String] = None

  def toOption: Option[Data] = this match {
    case NoData(_) => None
    case data => Some(data)
  }

  def value(keys: List[Key]): Data = keys match {
    case Nil => this
    case Ord(ord)::t => this(ord).value(t)
    case Label(label)::t => this(label).value(t)
    case _ => NoData() }

  def values: Iterable[Data] = Iterable.empty

  def isEmpty = this.toOption.isDefined
}

case class NoData(key: String = "") extends Data(key)

case class DataString(str: String, key: String = "") extends Data(key) {
  override def valueOption = Option(str)
}

case class DataNumeric(num: BigDecimal, key: String = "") extends Data(key) {
  override def valueOption = Some(num.toString())
}

//case class DataField(name: String, data: Data)

case class DataRecord(fields: List[Data], key: String = "") extends Data(key) {
  override def apply(field: String) = fields.find(f => f.label == field).getOrElse(NoData())
  override def values = fields
}

case class DataArray(elems: List[Data], key: String = "") extends Data(key) {
  override def apply(ord: Int) = elems.lift(ord).getOrElse(NoData())
  override def values = elems
}

sealed abstract class Key
case class Ord(ord: Int) extends Key
case class Label(label: String) extends Key




object Data2Json {
  def toJsonString(data: Data): String = data match {
    case DataString(s, _) => "\"" + s + "\""
    case DataRecord(fs, key) => toField(key) + "{" + fs.map(f => toField(f.label) + toJsonString(f)).mkString(",") + "}"
    case DataArray(ds, key) => toField(key) + "[" + ds.map(d => toJsonString(d)).mkString(",") + "]"
    case NoData(_) => "null"
    case DataNumeric(num, _) => num.setScale(2).toString()
  }

  def toField(name: String) = if(name.isEmpty) "" else "\"" + name + "\": "


}