package com.actio

/**
  * Created by mauri on 17/06/2016.
  */
abstract class LinkedTree[D <: LinkedTree[D]] {

  def apply(ord: Int): D

  def apply(field: String): D

  def apply(key: Key): D = key match {
    case Ord(o) => apply(o)
    case Label(l) => apply(l)
  }

  def toOption: Option[D]

  def elems: Iterator[D]

  def label: String

  def schema: SchemaDefinition = SchemaUnknown

  def unknown: D

  def stringOption: Option[String] = None

  def find(keys: List[FindCriteria]): Iterator[D] = keys match {
    case Nil => List(this.asInstanceOf[D]).toIterator
    case (h::t) => h match {
      case (f: FindOrd) => this(f.ord).find(t)
      case (f: FindLabel) => this(f.label).find(t)
      case FindAll => this.elems.flatMap(e => e.find(t))
    }
  }

  def find(keys: String): Iterator[D] = find(keys.split("\\.").map(s => {
    if (s == "*")
      FindAll
    else if(s.startsWith("#"))
      FindOrd(s.replace("#","").toInt)
    else
      FindLabel(s)
  }).toList)

  def value(keys: List[Key]): D = keys.foldLeft[D](this.asInstanceOf[D])((d,k) => d(k))

  def value(keys: String): D = value(keys.split("\\.").map(s =>
    if (s.startsWith("#"))
      Ord(s.replace("#","").toInt)
    else
      Label(s)
  ).toList)


  def headOption = elems.toList.headOption
}

sealed abstract class FindCriteria
case class FindOrd(ord: Int) extends FindCriteria
case class FindLabel(label: String) extends FindCriteria
case object FindAll extends FindCriteria