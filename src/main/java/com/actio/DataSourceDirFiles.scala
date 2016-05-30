package com.actio

import java.util.regex.Matcher
import java.util.regex.Pattern
import java.io._
import java.nio.charset.Charset
import java.nio.charset.UnmappableCharacterException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption

/**
  * Created by mauri on 14/04/2016.
  */
class DataSourceDirFiles extends DataSource {
  def execute = extract

  def getLastLoggedDataSet: DataSet = ???

  def write(data: DataSet): Unit = ???

  def write(data: DataSet, suffix: String): Unit = ???

  def load(): Unit = ???

  def read(queryParser: QueryParser): DataSet = ???

  def extract() = {
    dataSet = DataSetTableScala(List("directory","filename"), new File(dir).listFiles.filter(f => Pattern.compile(regex).matcher(f.getName).matches).toList.map(m => List(dir, m.getName))) }

  def LogNextDataSet(theSet: DataSet): Unit = ???


  def dir = getConfig.getString("directory")

  def regex = getConfig.getString("regex")

  override def execute(ds: DataSet, query: String): Unit = ???

  override def executeQuery(ds: DataSet, query: String): DataSet = ???
}
