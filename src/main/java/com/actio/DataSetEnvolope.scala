package com.actio

/**
 * Created by mauri on 6/07/2016.
 */

object DataSetEnvolope {
  def apply(label: String, data: DataSet): DataSetEnvolope = DataSetEnvolope(label, data, Nothin(), Nothin())
}

case class DataSetEnvolope(label: String, data: DataSet, message: DataSet, error: DataSet) extends DataSet {

  override def apply(field: String): DataSet =
    if (field == "message") {
      message
    }
    else if (field == "error") {
      error
    }
    else {
      data(field)
    }

  override def elems: Iterator[DataSet] = List(data).toIterator
}