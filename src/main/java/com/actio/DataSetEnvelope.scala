package com.actio

/**
 * Created by mauri on 6/07/2016.
 */

object DataSetEnvelope {
  def apply(label: String, data: DataSet): DataSetEnvelope = DataSetEnvelope(label, data, Nothin(), Nothin())
}

case class DataSetEnvelope(label: String, data: DataSet, message: DataSet, error: DataSet) extends DataSet {

  override def apply(field: String): DataSet =
    if (field == "message") {
      message
    }
    else if (field == "error") {
      error
    }
    else if (field == "data") {
      data
    }
    else {
      data(field)
    }

  override def elems: Iterator[DataSet] = data.elems
}
