package com.actio

/**
  * Created by jim on 7/8/2015.
  */

import com.actio.dpsystem.DPSystemConfigurable
import com.typesafe.config.Config

class DataSetArray(dataSets: List[DataSet]) extends DataSet {

  override def apply(ord: Int): DataSet = dataSets.lift(ord).getOrElse(Nothin())

  override def apply(field: String): DataSet = Nothin()

  override def elems: Iterator[DataSet] = dataSets.toIterator

  override def schema: SchemaDefinition = dataSets.headOption.map(_.schema).getOrElse(SchemaUnknown)

  def label: String = ""
}

abstract class DataSet extends LinkedTree[DataSet]{

  def apply(ord: Int): DataSet = Nothin()

  def apply(field: String): DataSet = Nothin()

  override def elems: Iterator[DataSet] = Iterator.empty

  def unknown: DataSet = Nothin()

  override def schema: SchemaDefinition = SchemaUnknown

  override def toOption: Option[DataSet] = this match {
    case Nothin(_) => None
    case data => Some(data)
  }

}

case class Nothin(val label: String) extends DataSet

object Nothin {
  def apply(): DataSet = Nothin("")
}