package com.actio

/**
  * Created by mauri on 27/05/2016.
  */
sealed abstract class SchemaDefinition

case class SchemaArray(content: SchemaDefinition) extends SchemaDefinition
case class SchemaNumber(precision: Int, scale: Int) extends SchemaDefinition
case class SchemaString(maxLength: Int) extends SchemaDefinition
case class SchemaDate(format: String) extends SchemaDefinition
case class SchemaField(name: String, required: Boolean, content: SchemaDefinition)
case class SchemaRecord(fields: List[SchemaField]) extends SchemaDefinition
case object SchemaUnknown extends SchemaDefinition

sealed abstract class SchemaMatchError
case object SchemaMatchRecordExpected extends SchemaMatchError
case object SchemaMatchArrayExpected extends SchemaMatchError
case object DataDoesntMatchSchema extends SchemaMatchError