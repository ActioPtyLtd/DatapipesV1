package com.actio

import java.io.{ FileInputStream, ObjectInputStream, File, DataInputStream }
import java.util.regex.Matcher
import java.util.regex.Pattern

import com.actio.dpsystem.Logging
//import upickle.default._

import java.nio.file._
import boopickle.Default._

/**
 * Created by mauri on 14/04/2016.
 */

object DataSetDumpFile {

  def apply(dir: String, regex: String): DataSet = {
    apply(new File(dir).listFiles.filter(f => Pattern.compile(regex).matcher(f.getName).matches).map(m => m.getPath()).toSeq)
  }

  def apply(fileNames: Seq[String]): DataSet = {
    new DataSet {

      override def elems = fileNames.view.map(f => new DataSetDumpFile(f).ds).toIterator
      val label = "something"
    }
  }
}

class DataSetDumpFile(private val fileName: String) extends DataSet with Logging {

  val ds = {
    logger.info(s"Reading $fileName...")

    val bytes = Files.readAllBytes(Paths.get(fileName))
    val ret = SerialisableDataSet.unpickle(bytes)

    logger.info(s"Finished reading $fileName...")
    ret
  }

  override def elems = List(ds).toIterator

  override def label: String = "file"

  override def clazz: Class[_] = classOf[DataSetDumpFile]
}
