package com.actio

import com.actio.dpsystem.Logging

/**
  * Created by mauri on 26/08/2016.
  */
class TaskDump extends Task with Logging {
  override def execute(): Unit = {
    logger.info(Data2Json.toJsonString(dataSet))
  }

  override def load(): Unit = ???

  override def extract(): Unit = ???

  override def clazz: Class[_] = classOf[TaskDump]
}
