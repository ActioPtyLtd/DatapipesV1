package com.actio

import com.actio.dpsystem.Logging
import com.typesafe.config.Config

// scalastyle:off

/**
  * Created by jimpo on 10/10/2016.
  */

object DataSourceFTP {

  // do we need anything for the static class?

}


class DataSourceFTP extends DataSource with Logging {

  @throws(classOf[Exception])
  override def setConfig(_conf: Config, _master: Config) {
    super.setConfig(_conf, _master)


  }

  @throws(classOf[Exception])
  def execute(ds: DataSet, query: String): Unit = {

    ds.elems.foreach(e => executeQueryLabel(e,query))

  }

  @throws(classOf[Exception])
  def executeQuery(ds: DataSet, query: String): DataSet = {



      return new DataSetTabular
  }

  @throws(classOf[Exception])
  def extract(): Unit = {
    dataSet = read(Nothin()) // doesn't really need a dataset

    // open connection

    // read remote files locally

    // create dataset_uri


  }


  @throws(classOf[Exception])
  def load() {

  }

  @throws(classOf[Exception])
  def execute() {

  }

  @throws(classOf[Exception])
  def write(dataSet: DataSet): Unit = {

  }

  override def create(ds: DataSet): Unit = {
    executeQueryLabel(ds.elems.toList.head, "create")
  }

  override def delete(ds: DataSet): Unit = {
    executeQueryLabel(ds.elems.toList.head, "delete")
  }


  @throws(classOf[Exception])
  def write(data: DataSet, suffix: String) {
    if (suffix.contentEquals("_all")) write(data)
  }

  override def update(ds: DataSet): Unit = {
    executeQueryLabel(ds, "update")
  }

  @throws(classOf[Exception])
  def read(queryParser: QueryParser): DataSet = ???



  /** As seen from class DataSourceFTP, the missing signatures are as follows.
    *  For convenience, these are usable as stub implementations.
    */
  // Members declared in com.actio.DataSource
  def LogNextDataSet(theSet: com.actio.DataSet): Unit = ???
  def getLastLoggedDataSet(): com.actio.DataSet = ???

  override def clazz: Class[_] = classOf[DataSourceFTP]

}
