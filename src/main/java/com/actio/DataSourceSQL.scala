package com.actio

import com.actio.dpsystem.{ Logging, DPSystemFactory, DPSystemConfigurable }
import com.typesafe.config.Config
import java.sql._
import scala.util.{Try,Success,Failure}

/**
 * Created by mauri on 4/08/2016.
 */
class DataSourceSQL extends DataSource with Logging {
  override def execute(): Unit = extract()

  override def getLastLoggedDataSet: DataSet = ???

  override def execute(ds: DataSet, label: String): Unit = {
    val cn = DriverManager.getConnection(getConnectStr())

    logger.info("Connected")

    val statement = ds.elems.map(d => d(label).stringOption.getOrElse(
      Try(getConfig().hasPath("query."  + label)) match {
        case Success(s) => getConfig().getConfig("query").getString(label)
        case Failure(f) => label
      }
    )) mkString "; "

    val stmt: PreparedStatement = cn.prepareStatement(statement)

    logger.info("Executing SQL batch statement...")
    logger.info(statement)

    stmt.execute()

    logger.info("Successfully executed statement.")

    cn.close()
  }

  lazy val connection = DriverManager.getConnection(getConnectStr())

  lazy val statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY)

  //TODO: close connection on dispose

  override def executeQuery(ds: DataSet, label: String = "read"): DataSet = {
    logger.info("Connecting to database...")

    try {

      val sqlQuery = ds(label).stringOption.getOrElse(getConfig().getConfig("query").getString(label))

      logger.info("Executing Query: " + sqlQuery)

      dataSet = new DataSetDBStream(statement.executeQuery(sqlQuery), 50000)

      logger.info("Successfully executed statement.")
    } catch {
      case e: Exception => { logger.error("Exception " + e.getMessage()) }
    }

    dataSet
  }

  override def write(data: DataSet): Unit = {
    if(config.getString(DPSystemConfigurable.BEHAVIOR_LABEL).isEmpty()) {
      execute(data, config.getString("query.create"))
    }
    else {
      execute(data, config.getString("query." + DPSystemConfigurable.BEHAVIOR_LABEL))
    }
  }

  override def write(data: DataSet, suffix: String): Unit = ???

  override def load(): Unit = ???

  override def read(queryParser: QueryParser): DataSet = ???

  override def extract(): Unit = { dataSet = executeQuery(Nothin()) }

  override def LogNextDataSet(theSet: DataSet): Unit = ???

  override def setConfig(_conf: Config, _master: Config): Unit = {
    super.setConfig(_conf, _master)
    if (config.hasPath(DPSystemConfigurable.JDBC_DRIVER_LABEL)) {
      try {
        Class.forName(config.getString(DPSystemConfigurable.JDBC_DRIVER_LABEL))
      } catch {
        case var1: ClassNotFoundException => {
          var1.printStackTrace()
        }
      }
    }
  }

  override def clazz: Class[_] = classOf[DataSourceSQL]
}
