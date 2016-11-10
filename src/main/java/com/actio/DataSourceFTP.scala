package com.actio

import org.apache.commons.net.ftp._
import com.actio.dpsystem.Logging
import com.typesafe.config.Config
import scala.util.Try

// scalastyle:off

/**
  * Created by jimpo on 10/10/2016.
  */


class DataSourceFTP extends DataSource with Logging {

  @throws(classOf[Exception])
  override def setConfig(_conf: Config, _master: Config) {
    super.setConfig(_conf, _master)

  }

  @throws(classOf[Exception])
  override def setConfig(_conf: Config, _master: Config) {
    super.setConfig(_conf, _master)


  }

  @throws(classOf[Exception])
  def execute(ds: DataSet, query: String): Unit = {

    // ds.elems.foreach(e => executeQueryLabel(e,query))

  }

  @throws(classOf[Exception])
  def executeQuery(ds: DataSet, query: String): DataSet = {



      return new DataSetTabular
  }

  @throws(classOf[Exception])
  def extract(): Unit = ???

  @throws(classOf[Exception])
  def openConnection() : Try[FTPClient] = {

    println("openConnection::ftp")

    def ftp: FTPClient = new FTPClient()
    def conf: FTPClientConfig = new FTPClientConfig()
    conf.setLenientFutureDates(true)

    ftp.configure(conf)

    try {
      // attempt connection


      ftp.connect(hostname, port)
    }
    catch
      {
        case _:Exception => return Try(null)
      }

    return Try(ftp)
  }


  @throws(classOf[Exception])
  def load() {
    //dataSet = read(Nothin()) // doesn't really need a dataset

    // open connection
    def ftpconn : Try[FTPClient] = openConnection()

    // read remote files locally

    // create dataset_uri

  }

  @throws(classOf[Exception])
  def execute() {

  }

  @throws(classOf[Exception])
  def write(dataSet: DataSet): Unit = {

    println("ftp::write::")
    //dataSet = read(Nothin()) // doesn't really need a dataset

    // open connection
    def ftpconn : Try[FTPClient] = openConnection()

    var ftp = ftpconn
    // read remote files locally




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
