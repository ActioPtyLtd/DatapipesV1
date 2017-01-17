package com.actio

import org.apache.commons.net.ftp._
import java.util.regex.Matcher
import java.util.regex.Pattern
import com.actio.dpsystem.Logging

object DataSetFTP {

  // TODO: needs refactoring, only added this here to avoid writing java
  def apply(client: FTPClient, remotePath: String, regex: String): DataSet = {
    client.enterLocalPassiveMode()
    new DataSetFTP(client, remotePath, client.listFiles(remotePath).filter(f => Pattern.compile(regex).matcher(f.getName).matches).toList)
  }
}

class DataSetFTP(client: FTPClient, remotePath: String, files: List[FTPFile] ) extends DataSet with Logging {

    override lazy val elems = files.toIterator.flatMap(f => {
      val path = remotePath + "/" + f.getName
      try {
        logger.info(s"Fetching remote file: $path...")
        val fs = client.retrieveFileStream(path)
        val fds = DataRecord(DataRecord("meta", DataString("fileName", f.getName)))
        val ds = new DataSetFileStream(fs).elems.map(m => DataSetOperations.mergeLeft(DataRecord(m), fds)).toList.toIterator  // get everything right now so I can close
        client.completePendingCommand()
        logger.info(s"Deleting remote file: $path...")
        client.deleteFile(path)
        ds
      } catch {
        case e: Throwable => { logger.error(s"Failed to read or delete file $path. Ignoring file..."); Iterator.empty }
      }

    })

    override def label: String = "file"

    override def clazz: Class[_] = classOf[DataSetFTP]
}
