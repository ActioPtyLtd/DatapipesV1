package com.actio

import java.sql.ResultSet
import java.util

/**
  * Created by mauri on 26/05/2016.
  */

class DataSetFixedData(myschema: SchemaDefinition, dataElems: Data) extends DataSet{
  private var elemCount = 0

  override def schema: SchemaDefinition = myschema

  override def next(): Data = {
    elemCount = elemCount + 1
    dataElems
  }

  override def hasNext: Boolean = 1 > elemCount //wanted to do varargs on data, but seems java doesn't like it
}