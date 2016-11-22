package com.actio

import com.actio.dpsystem.Logging
import java.io._
//import upickle.default._
import boopickle.Default._

/**
  * Created by mauri on 26/08/2016.
  */
class TaskFileDump extends Task with Logging {

  override def execute(): Unit = {
    super.setConfig(sysconf.getTaskConfig(this.node.getName).toConfig, sysconf.getMasterConfig)
    load()
  }

  override def load(): Unit = {

   val tmpFile = File.createTempFile(this.node.getName, ".ds", new File(config.getString("directory")))
   val fileOut = new FileOutputStream(tmpFile)

   fileOut.write(Pickle.intoBytes(SerialisableDataSet.convert(dataSet)).array())
   fileOut.close()

    //val fos = new FileOutputStream(this.node.getName + "-" + java.util.UUID.randomUUID.toString + ".ds")

    dataSet = DataRecord(dataSet)
  }

  override def extract(): Unit = ???

  override def clazz: Class[_] = classOf[TaskDump]
}
