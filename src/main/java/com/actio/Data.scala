package com.actio

/**
  * Created by mauri on 25/05/2016.
  */

sealed abstract class Data {
  def apply(ord: Int): Data = NoData
  def apply(field: String): Data = NoData
  def valueOption: Option[String] = None

  def toOption: Option[Data] = this match {
    case NoData => None
    case data => Some(data)
  }

  def value(keys: List[Key]): Data = keys match {
    case Nil => this
    case Ord(ord)::t => this(ord).value(t)
    case Label(label)::t => this(label).value(t)
    case _ => NoData }

  def values: Iterable[Data] = Iterable.empty

  def isEmpty = this.toOption.isDefined
}

case object NoData extends Data

case class DataString(str: String) extends Data {
  override def valueOption = Option(str)
}

case class DataNumeric(num: BigDecimal) extends Data {
  override def valueOption = Some(num.toString())
}

case class DataField(name: String, data: Data)

case class DataRecord(fields: List[DataField]) extends Data {
  override def apply(field: String) = fields.find(f => f.name == field).map(_.data).getOrElse(NoData)
  override def values = fields.map(_.data)
}

case class DataArray(elems: List[Data]) extends Data {
  override def apply(ord: Int) = elems.lift(ord).getOrElse(NoData)
  override def values = elems
}

sealed abstract class Key
case class Ord(ord: Int) extends Key
case class Label(label: String) extends Key




object Data2Json {
  def toJsonString(data: Data): String = data match {
    case DataString(s) => "\"" + s + "\""
    case DataRecord(fs) => "{" + fs.map(f => "\"" + f.name + "\": " + toJsonString(f.data)).mkString(",") + "}"
    case DataArray(ds) => "[" + ds.map(d => toJsonString(d)).mkString(",") + "]"
    case NoData => "null"
    case DataNumeric(num) => num.setScale(2).toString()
  }


}