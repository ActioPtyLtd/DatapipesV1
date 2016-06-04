package com.actio

/**
  * Created by jim on 7/8/2015.
  */

import com.actio.dpsystem.DPSystemConfigurable
import com.typesafe.config.Config

case class DataSetArray(dataSets: List[DataSet]) extends DataSet {

  override def get(ord: Int): Option[DataSet] = Option(dataSets(ord))

  override def get(field: String): Option[DataSet] = None

  override def elems: Iterable[DataSet] = dataSets

  override def schema: SchemaDefinition = SchemaUnknown

  override def hasNext: Boolean = false

  override def next(): Data = NoData()
}

abstract class DataSet extends DPSystemConfigurable with Iterator[Data] {

  def get(ord: Int): Option[DataSet] = None

  def get(field: String): Option[DataSet] = None

  def elems: Iterable[DataSet] = List(this)

  def schema: SchemaDefinition = SchemaUnknown

  //not sure why I need this, but it prevents compiler errors
  override def minBy[B](f: Data => B)(implicit cmp: Ordering[B]): Data = null

  override def maxBy[B](f: Data => B)(implicit cmp: Ordering[B]): Data = null

  override def max[B >: Data](implicit cmp: Ordering[B]): Data = null

  override def min[B >: Data](implicit cmp: Ordering[B]): Data = null
}