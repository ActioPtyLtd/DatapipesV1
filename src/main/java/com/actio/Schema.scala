package com.actio

/**
  * Created by mauri on 27/05/2016.
  */
sealed abstract class SchemaDefinition(val label: String)

case class SchemaArray(name: String, content: SchemaDefinition) extends SchemaDefinition(name)

object SchemaArray {
  def apply(content: SchemaDefinition) = new SchemaArray("", content)
}

case class SchemaRecord(name: String, fields: List[SchemaDefinition]) extends SchemaDefinition(name)

object SchemaRecord {
  def apply(fields: List[SchemaDefinition]) = new SchemaRecord("", fields)
}

case class SchemaNumber(name: String, precision: Int, scale: Int) extends SchemaDefinition(name)
case class SchemaString(name: String, maxLength: Int) extends SchemaDefinition(name)
case class SchemaDate(name: String, format: String) extends SchemaDefinition(name)

case object SchemaUnknown extends SchemaDefinition("")

sealed abstract class SchemaMatchError
case object SchemaMatchRecordExpected extends SchemaMatchError
case object SchemaMatchArrayExpected extends SchemaMatchError
case object DataDoesntMatchSchema extends SchemaMatchError