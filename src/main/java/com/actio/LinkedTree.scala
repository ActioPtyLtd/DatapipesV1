package com.actio

/**
  * Created by mauri on 17/06/2016.
  */
trait LinkedTree[D <: LinkedTree[D]] {

  def apply(ord: Int): D

  def apply(field: String): D

  def toOption: Option[D]

  def elems: Iterable[D] = Iterable.empty

  def label: String

  def schema: SchemaDefinition = SchemaUnknown

  def unknown: D

  def find(keys: List[FindCriteria]): List[D] = keys match {
    case Nil => List(this.asInstanceOf[D])
    case (h::t) => h match {
      case (f: FindOrd) => this(f.ord).find(t)
      case (f: FindLabel) => this(f.label).find(t)
      case FindAll => this.elems.flatMap(e => e.find(t)).toList
    }
  }

  def find(keys: String): List[D] = find(keys.split("\\.").map(s => {
    if (s == "*")
      FindAll
    else if(s.startsWith("#"))
      FindOrd(s.replace("#","").toInt)
    else
      FindLabel(s)
  }).toList)
}

sealed abstract class FindCriteria
case class FindOrd(ord: Int) extends FindCriteria
case class FindLabel(label: String) extends FindCriteria
case object FindAll extends FindCriteria