package com.actio

import org.apache.commons.net.ftp._
import com.actio.dpsystem.Logging
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import org.slf4j.Logger

import scala.util._

import java.io.InputStream
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets

// scalastyle:off

/**
  * Created by jimpo on 10/10/2016.
  */


class DataSourceFTP extends DataSource with Logging {

  override val logger: Logger = LoggerFactory.getLogger(classOf[DataSourceFTP])

  var connect : String = null
  var port : Int = -1
  var user: String = null
  var password: String = null
  var remotepath : String = "."
  var localpath : String = "."
  var remotefile : String = null
  var localfile : String = null
  var verb : String = null
  var ftpconn : Try[FTPClient] = null

  override def setConfig(_conf: Config, _master: Config) {
    super.setConfig(_conf, _master)

    connect = config.getString("connect")

    if (config.hasPath("port"))
      port =config.getInt("port")

    if (config.hasPath("credential")) {
      val credConfig: Config = config.getConfig("credential")
      user = credConfig.getString("user")
      password = credConfig.getString("password")
    }
    if (config.hasPath("query")) {
      val queryConf: Config = config.getConfig("query")
      localpath = queryConf.getString("localpath")
      // localfile = queryConf.getString("localfile")
      remotepath = queryConf.getString("remotepath")
      // remotefile = queryConf.getString("remotefile")
    }

  }

  def execute(ds: DataSet, query: String): Unit = {


  }

  @throws(classOf[Exception])
  def execute() {

  }

  @throws(classOf[Exception])
  def executeQuery(ds: DataSet, query: String): DataSet = {

      return new DataSetTabular
  }

  @throws(classOf[Exception])
  def extract(): Unit = ???

  @throws(classOf[Exception])
  def openConnection() : Try[FTPClient] = {

    logger.info(s"openConnection::ftp::$connect:$port")

    var ftp: FTPClient = new FTPClient()
    var conf: FTPClientConfig = new FTPClientConfig()
    conf.setLenientFutureDates(true)

    ftp.configure(conf)

    try {
      if (port < 0)
        ftp.connect(connect)
      else
        ftp.connect(connect,port)
      logger.info(ftp.getReplyString());

      var reply : Int = ftp.getReplyCode()

      logger.info(s"reply code = $reply")

      if (!FTPReply.isPositiveCompletion(reply)) {
        ftp.disconnect()
        return Failure(new Throwable("Failed to connect "+reply))
      }

      logger.info(s"Logging in as $user")
      ftp.login(user,password)
      logger.info(s"Connected to $connect ");
      logger.info(ftp.getReplyString());

      // After connection attempt, you should check the reply code to verify
      // success.
      reply = ftp.getReplyCode();

      if(!FTPReply.isPositiveCompletion(reply)) {
        ftp.disconnect();
        return Failure(new Throwable("FTP server refused connection."+reply))
      }

      ftp.enterLocalPassiveMode();
      reply = ftp.getReplyCode();

      if(!FTPReply.isPositiveCompletion(reply)) {
        ftp.disconnect();
        return Failure(new Throwable("FTP server failed to go into passive mode"+reply))
      }

    }
    catch {

        case ex :Exception => {
          logger.info(s"Exception: " + ex.getMessage)
          return Failure(ex)
        }
    }

    return Try(ftp)
  }


  @throws(classOf[Exception])
  def load() {
    //dataSet = read(Nothin()) // doesn't really need a dataset
    logger.info(s"load")
    // open connection
    ftpconn = openConnection()

    // read remote files locally

    // create dataset_uri

  }


  def write(dataSet: DataSet): Unit = {
    writeFromDataSet_Zeiss(dataSet)
  }



  // zeiss specific
  def writeFromDataSet(dataSet: DataSet): Unit = {

    logger.info(s"write")


    // read remote files locally
    try {
        ftpconn =  openConnection()
        if (!ftpconn.isSuccess) throw ftpconn.failed.get

        logger.info(s"cd to $remotepath")

        ftpconn.get.changeWorkingDirectory(remotepath)
        logger.info(ftpconn.get.getReplyString());
        logger.info(s"putting to "+ftpconn.get.printWorkingDirectory())

        // write DataSet
        var i = 0

        for (dsrecord: DataSet <- dataSet.elems) {

          val outRecord : String = dsrecord.toString()
          val data: InputStream = new ByteArrayInputStream(outRecord.getBytes(StandardCharsets.UTF_8))
          val fname = i + remotefile
          i += 1
          // write that stream to the remote system
          logger.info(s"writing $fname")
          var remoteFileStream = ftpconn.get.storeFile(fname, data)

          // check for success
          if (remoteFileStream == false) {
            // failed
            logger.error("Failed to write filestream")
          }
        }

    }
    catch
      {
        case ex: Exception => logger.info(s" Exception=="+ex.getMessage)
      }
    finally {
      // do cleanup here

    }
  }


  // zeiss specific
  def writeFromDataSet_Zeiss(dataSet: DataSet): Unit = {

    logger.info(s"write")


    // read remote files locally
    try {
      ftpconn =  openConnection()
      if (!ftpconn.isSuccess) throw ftpconn.failed.get

      logger.info(s"cd to $remotepath")

      ftpconn.get.changeWorkingDirectory(remotepath)
      logger.info(ftpconn.get.getReplyString());
      logger.info(s"putting to "+ftpconn.get.printWorkingDirectory())

      // write DataSet
      var i = 0

      for (dsrecord: DataSet <- dataSet.elems) {
        i += 1
        val extra_file : String = dsrecord("extra_file").stringOption.get.replaceAllLiterally("\\n","\n")
        val extra_filename : String = dsrecord("extra_filename").stringOption.getOrElse("gnm_default_H.xml")
        val body_file : String = dsrecord("body_file").stringOption.get.replaceAllLiterally("\\n","\n")
        val body_filename : String = dsrecord("body_filename").stringOption.getOrElse("gnm_default_L.xml")

        writeStringFTP(s"$extra_file",extra_filename)
        writeStringFTP(s"$body_file",body_filename)

      }

    }
    catch
      {
        case ex: Exception => logger.info(s" Exception=="+ex.getMessage)
      }
    finally {
      // do cleanup here

    }
  }

  def writeStringFTP(literal:String, fname:String ): Unit ={

    if (literal.trim == "" ) return;

    val data: InputStream = new ByteArrayInputStream(literal.getBytes(StandardCharsets.UTF_8))
    // write that stream to the remote system
    logger.info(s"writing "+fname)
    var remoteFileStream = ftpconn.get.storeFile(fname, data)

    // check for success
    if (! remoteFileStream ) {
    // failed
    logger.error("Failed to write filestream")

  }
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
