package com.actio

/**
  * Created by mauri on 17/06/2016.
  */
trait DataGeneric[D] {

  def apply(ord: Int): D

  def apply(field: String): D

  def toOption: Option[D]

  def elems: Iterable[D] = Iterable.empty

  //def isEmpty = this.toOption.isDefined

  def label: String

  def schema: SchemaDefinition = SchemaUnknown

  def unknown: D

}