package com.actio

/**
  * Created by jim on 7/8/2015.
  */

import com.actio.dpsystem.DPSystemConfigurable
import com.typesafe.config.Config
import java.sql.ResultSet

//TODO: update this when we start using it to capture multiple datasets
case class DataSetArray(dataSets: List[DataSet]) extends DataSet {
  override def get(ord: Int): Option[DataSet] = None
  override def get(field: String): Option[DataSet] = None

  override def values: Iterable[DataSet] = dataSets

  override def schema: SchemaDefinition = SchemaUnknown

  override def hasNext: Boolean = ???

  override def next(): Data = ???
}

abstract class DataSet extends DPSystemConfigurable with Iterator[Data] {

  def get(ord: Int): Option[DataSet] = None
  def get(field: String): Option[DataSet] = None

  def values: Iterable[DataSet] = List(this)

  def schema: SchemaDefinition = SchemaUnknown

  def getRecords = this.flatMap(d => d.values)


  //TODO: get rid of these and replace the data methods
  def getAsListOfColumns(): java.util.List[java.util.List[String]] = ???

  def getCustomHeader(): String = ???


  // not sure why I need this, but it prevents compiler errors
  override def minBy[B](f: Data => B)(implicit cmp: Ordering[B]): Data = null
  override def maxBy[B](f: Data => B)(implicit cmp: Ordering[B]): Data = null
  override def max[B >: Data](implicit cmp: Ordering[B]): Data = null
  override def min[B >: Data](implicit cmp: Ordering[B]): Data = null
}