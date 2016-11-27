package com.actio

import java.io.File
import java.util.regex.Pattern

/**
  * Created by jimpo on 18/10/2016.
  */
class DataSourceFileStream  extends DataSource  {

  def execute = extract

  def getLastLoggedDataSet: DataSet = ???


  def write(data: DataSet): Unit ={

    // iterate over the dataset and stream out each file



  }

  def write(data: DataSet, suffix: String): Unit = ???

  def load(): Unit = ???

  def read(queryParser: QueryParser): DataSet = ???

  def extract() = ???

  def LogNextDataSet(theSet: DataSet): Unit = ???

  def dir = getConfig.getString("directory")

  def regex = getConfig.getString("regex")

  override def execute(ds: DataSet, query: String): Unit = ???

  override def executeQuery(ds: DataSet, query: String): DataSet = ???

}
