package com.actio

import java.sql.ResultSet
import java.util

/**
  * Created by mauri on 26/05/2016.
  */

class DataSetSingleData(private val myschema: SchemaDefinition, val data: Data) extends DataSet{
  private var boolNext = true

  override def schema: SchemaDefinition = myschema

  override def next(): Data = {
    boolNext = false
    data
  }

  override def hasNext: Boolean = boolNext
}