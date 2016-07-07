import java.net.{URL, URLClassLoader}

import com.actio.dpsystem.{DPSystemFactory, DPSystemRuntime}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by jim on 28/03/2016.
  */
/*
object Main {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  @throws[Exception]
  def main(args: Array[String]) = {
    System.out.println("=====First line in main")
    logger.info("======First logging.info")
    var configFile: String = null
    var pipelineName: String = null
    if (args.length > 0) {
      if (args(0).contains(".json") || args(0).contains(".conf")) {
        configFile = args(0)
      }
      if (args.length > 1) {
        pipelineName = args(1)
      }
      logger.info("Params(" + args.length + ")=" + configFile + ":'" + args(0) + "'  Pipeline=" + pipelineName)
    }
    else
      logger.info("Params(" + args.length + ")=" + configFile + ": Pipeline=" + pipelineName)

    debug
    val tf: DPSystemFactory = new DPSystemFactory
    tf.loadConfig(configFile)
    val dprun: DPSystemRuntime = tf.newRuntime
    dprun.execute
    dprun.dump
  }

  private def debug {
    val cl: ClassLoader = ClassLoader.getSystemClassLoader
    val urls: Array[URL] = (cl.asInstanceOf[URLClassLoader]).getURLs
    for (url <- urls) {
      logger.info(url.getFile)
    }
  }

}
*/