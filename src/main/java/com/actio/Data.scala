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
    case Ord(ord)::t => this.apply(ord).value(t)
    case Label(label)::t => this.apply(label).value(t)
    case _ => NoData }

  def values: Iterable[Data] = Iterable.empty

  def isEmpty = this.toOption.isDefined
}

case object NoData extends Data

case class DataString(str: String) extends Data {
  override def valueOption = Option(str)
}
case class DataField(name: String, Data: Data)

case class DataRecord(fields: List[DataField]) extends Data {
  override def apply(field: String) = fields.find(f => f.name == field).map(_.Data).getOrElse(NoData)
  override def values = fields.map(_.asInstanceOf[Data])
}

case class DataArray(elems: List[Data]) extends Data {
  override def apply(ord: Int) = elems.lift(ord).getOrElse(NoData)
  override def values = elems
}

sealed abstract class Key
case class Ord(ord: Int) extends Key
case class Label(label: String) extends Key

object mytest extends App {

    val d: Data = new DataSetTableScala(List("c1","c2"), List(List("13","15"),List("16","17"))).toData
    val t = DataSetTableScala(d)
    print(t)

}