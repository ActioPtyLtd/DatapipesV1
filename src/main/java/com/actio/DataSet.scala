package com.actio

/**
  * Created by jim on 7/8/2015.
  */

import com.actio.dpsystem.DPSystemConfigurable
import com.typesafe.config.Config

class DataSetArray(dataSets: List[DataSet]) extends DataSet {

  override def apply(ord: Int): DataSet = dataSets.lift(ord).getOrElse(new NoDataSet())

  override def apply(field: String): DataSet = new NoDataSet()

  override def elems: Iterable[DataSet] = dataSets

  override def schema: SchemaDefinition = dataSets.headOption.map(_.schema).getOrElse(SchemaUnknown)

  override def hasNext: Boolean = dataSets.headOption.exists(_.hasNext)

  override def next(): Data = dataSets.headOption.map(_.next()).getOrElse(NoData())
}

abstract class DataSet extends DPSystemConfigurable with Iterator[Data] with DataGeneric[DataSet] {

  def apply(ord: Int): DataSet = new NoDataSet()

  def apply(field: String): DataSet = new NoDataSet()

  override def elems: Iterable[DataSet] = List(this)

  def label = ""

  def toOption: Option[DataSet] = Some(this) // fix this

  def unknown: DataSet = new NoDataSet()

  override def schema: SchemaDefinition = SchemaUnknown

  //not sure why I need this, but it prevents compiler errors
  override def minBy[B](f: Data => B)(implicit cmp: Ordering[B]): Data = null

  override def maxBy[B](f: Data => B)(implicit cmp: Ordering[B]): Data = null

  override def max[B >: Data](implicit cmp: Ordering[B]): Data = null

  override def min[B >: Data](implicit cmp: Ordering[B]): Data = null
}

class NoDataSet extends DataSet {
  override def hasNext: Boolean = false

  override def next(): Data = NoData()
}