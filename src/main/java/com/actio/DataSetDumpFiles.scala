package com.actio

import java.io.File
import java.util.regex.Matcher
import java.util.regex.Pattern

import com.actio.dpsystem.Logging

import java.nio.file._
import scala.collection.mutable.Queue

/**
 * Created by mauri on 14/04/2016.
 */

object DataSetDumpFile {

  // TODO: needs refactoring, only added this here to avoid writing java
  def apply(dir: String, regex: String): DataSet = {
    new DataSetDumpFile(new File(dir).listFiles.filter(f => Pattern.compile(regex).matcher(f.getName).matches).map(m => m.getPath()).toSeq)
  }
}

class DataSetDumpFile(fileNames: Seq[String]) extends DataSet with Logging {

  override def elems =
    new Iterator[DataSet] {
      private val files = Queue(fileNames:_*)

      override def hasNext: Boolean = !files.isEmpty

      override def next() = {
        val fileName = files.dequeue()

        try {
          logger.info(s"Reading file: $fileName...")

          val path = Paths.get(fileName)
          val bytes = Files.readAllBytes(path)
          val ds = SerialisableDataSet.unpickle(bytes)

          logger.info(s"Deleting file: $fileName...")
          Files.delete(path)
          logger.info(s"Successfully deleted file: $fileName...")

          ds
        }
        catch {
          case e: Throwable => {
            logger.error(e.getMessage())
            Nothin()
          }
        }
      }
    }

  override def label: String = "file"

  override def clazz: Class[_] = classOf[DataSetDumpFile]
}
