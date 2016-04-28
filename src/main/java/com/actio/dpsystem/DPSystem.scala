package com.actio.dpsystem

import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
  * Created by mauri on 28/04/2016.
  */
trait Logging {
  val logger = LoggerFactory.getLogger(classOf[DPSystemConfigurable])
}
