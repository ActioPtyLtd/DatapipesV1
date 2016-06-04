package com.actio

/**
  * Created by mauri on 27/05/2016.
  */
sealed abstract class SchemaDefinition
{
  def label: String
}

case class SchemaArray(label: String, content: SchemaDefinition) extends SchemaDefinition

object SchemaArray {

  def apply(content: SchemaDefinition) = new SchemaArray("", content)
}

case class SchemaRecord(label: String, fields: List[SchemaDefinition]) extends SchemaDefinition

object SchemaRecord {

  def apply(fields: List[SchemaDefinition]) = new SchemaRecord("", fields)
}

case class SchemaNumber(label: String, precision: Int, scale: Int) extends SchemaDefinition
case class SchemaString(label: String, maxLength: Int) extends SchemaDefinition
case class SchemaDate(label: String, format: String) extends SchemaDefinition

case object SchemaUnknown extends SchemaDefinition {
  def label = ""
}

sealed abstract class SchemaMatchError
case object SchemaMatchRecordExpected extends SchemaMatchError
case object SchemaMatchArrayExpected extends SchemaMatchError
case object DataDoesntMatchSchema extends SchemaMatchError