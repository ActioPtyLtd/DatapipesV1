package com.actio

import java.sql.ResultSet
import java.util

/**
  * Created by mauri on 26/05/2016.
  */

class DataSetFixedData(myschema: SchemaDefinition, dataElems: DataSet) extends DataSet{

  override def schema: SchemaDefinition = myschema

  override def elems = List(dataElems).toIterator

  override def label: String = ""
}