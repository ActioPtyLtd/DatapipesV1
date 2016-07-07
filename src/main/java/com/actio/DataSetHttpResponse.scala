package com.actio

/**
  * Created by mauri on 6/07/2016.
  */
case class DataSetHttpResponse(label: String, uri: String, statusCode: Int, headers: Map[String,String], body: DataSet) extends DataSet {

  override def apply(field: String): DataSet = if(field=="body")
    body
  else
    headers.get(field).map(DataString(field,_)).getOrElse(Nothin())

  override def elems: Iterator[DataSet] = List(body).toIterator

}
