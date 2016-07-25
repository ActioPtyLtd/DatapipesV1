package com.actio

import java.sql.ResultSet
import java.util

/**
  * Created by mauri on 26/05/2016.
  */

class DataSetFixedData(mySchema: SchemaDefinition, dataElems: DataSet) extends DataSet{

  override def apply(int: Int): DataSet = dataElems(int)

  override def apply(label: String): DataSet = dataElems(label)

  override def schema: SchemaDefinition = mySchema

  override def elems: Iterator[DataSet] = List(dataElems).toIterator

  override def label: String = ""
}