package com.actio

import java.net.URI
import java.nio.charset.Charset

import com.actio.dpsystem.Logging
import com.typesafe.config.Config
import java.io.IOException
import java.io.UnsupportedEncodingException
import org.apache.commons.codec.binary.Base64
import org.apache.http.{HttpHeaders, HttpResponse}
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.CredentialsProvider
import org.apache.http.client.methods._
import org.apache.http.entity.StringEntity
import org.apache.http.impl.auth.BasicScheme
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import scala.collection.Iterator
import scala.util.{Failure, Success, Try}

/**
  * Created by jim on 7/8/2015.
  */
object DataSourceREST {
  private val CONTENT_TYPE: String = "application/json"
}

class DataSourceREST extends DataSource with Logging {
  var route: String = null
  var trans: TaskTransform = null
  var user: String = null
  var password: String = null
  var getfn: String = null
  var putfn: String = null
  var postfn: String = null
  var deletefn: String = null
  var patchfn: String = null
  var port: String = null

  @throws(classOf[Exception])
  override def setConfig(_conf: Config, _master: Config) {
    super.setConfig(_conf, _master)
    if (config.hasPath("url"))
      route = config.getString("url")
    if (config.hasPath("credential")) {
      val credConfig: Config = config.getConfig("credential")
      user = credConfig.getString("user")
      password = credConfig.getString("password")
    }
    if (config.hasPath("path")) {
      route = config.getString("path")
    }
    if (config.hasPath("get")) {
      getfn = config.getString("get")
    }
    if (config.hasPath("put")) {
      putfn = config.getString("put")
    }
    if (config.hasPath("post")) {
      postfn = config.getString("post")
    }
    if (config.hasPath("delete")) {
      deletefn = config.getString("delete")
    }
    if (config.hasPath("patch")) {
      patchfn = config.getString("patch")
    }
    if (config.hasPath("port")) {
      port = config.getString("port")
    }
  }

  @throws(classOf[Exception])
  def execute(ds: DataSet, query: String) {
  }

  def credentialsProvider = {
    val cre = new BasicCredentialsProvider
    cre.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password))
    cre
  }

  def send(request: HttpUriRequest): Try[(HttpResponse, String)] = {
    val httpClient = HttpClientBuilder.create().build()

    val response = Try({
      val re = httpClient.execute(request)
      (re, EntityUtils.toString(re.getEntity, "UTF-8"))
    })

    httpClient.close()

    response
  }

  def sendAndLog(request: HttpUriRequest) = {

    logger.info(s"Calling ${request.getMethod} ${request.getURI}...")

    val response = send(request)

    response match {
      case Success(s) => {
        val statusCode = s._1.getStatusLine.getStatusCode
        logger.info(s"Server status code: $statusCode")
        if (statusCode != 200)
          logger.error(s._2)
      }
      case Failure(f) => {
        logger.error(f.getMessage)
      }
    }
    response
  }

  def authHeader = "Basic " + new String(Base64.encodeBase64((user + ":" + password).getBytes(Charset.forName("ISO-8859-1"))))

  @throws(classOf[Exception])
  def executeQuery(ds: DataSet, query: String) = {
    val httpGet = new HttpGet(query)
    httpGet.setHeader(HttpHeaders.AUTHORIZATION, authHeader)
    httpGet.setHeader(HttpHeaders.CONTENT_TYPE, DataSourceREST.CONTENT_TYPE)

    val response = sendAndLog(httpGet).get // may log and throw an exception
    val responseData = Data2Json.fromJson2Data(response._2) // this can fail too, if json isn't returned

    new DataSetFixedData(responseData.schema, responseData)
  }

  @throws(classOf[Exception])
  def extract: Unit = {
    dataSet = read(new DataSetTableScala()) // doesn't really need a dataset
  }

  @throws(classOf[Exception])
  def load {
    if (trans != null) {
      trans.setDataSet(getDataSet)
      trans.execute
      setDataSet(trans.getDataSet)
    }
    write(getDataSet)
  }

  @throws(classOf[Exception])
  def execute {
    if (trans != null) {
      trans.setDataSet(getDataSet)
      trans.execute
      setDataSet(trans.getDataSet)
    }
    write(dataSet)
  }

  @throws(classOf[Exception])
  def write(data: DataSet, suffix: String) {
    if (suffix.contentEquals("_all")) write(data)
  }

  override def create(ds: DataSet): Unit = {
    getRequests(ds, new HttpPost(), createConfig, createBody).foreach({
      case Success(s) => sendAndLog(s)
      case Failure(f) => logger.error(f.getMessage)}
    )
  }

  private def configOption(config: Config, path: String) = if (config.hasPath(path)) Some(config.getString(path)) else None

  private lazy val createConfig = config.getString("query.create.header")
  private lazy val updateConfig = config.getString("query.update.header")

  private lazy val createBody = configOption(config,"query.create.body")
  private lazy val updateBody = configOption(config,"query.update.body")
  private lazy val createForEach = configOption(config,"query.create.foreach")

  override def update(ds: DataSet): Unit = {
    getRequests(ds, new HttpPut(), updateConfig, updateBody).foreach({
      case Success(s) => sendAndLog(s)
      case Failure(f) => logger.error(f.getMessage)}
    )
  }

  private def getRequests[T <: HttpEntityEnclosingRequestBase](ds: DataSet, f: => HttpEntityEnclosingRequestBase, templateHeader: String, templateBody: Option[String]) = {

    val headerParser = TemplateParser(templateHeader)

    if(templateBody.isDefined) {

      val bodyParser = TemplateParser(templateBody.get)

      split(ds).map(d => {
            val scope = Map("g" -> d._1, "d" -> d._2)
            for {
              a <- TemplateEngine(bodyParser, scope)
              b <- TemplateEngine(headerParser, scope)
            } yield createRequest(a, f, b.stringOption.getOrElse(""))
          })
    } else {
      split(ds).map(d => {
        val scope = Map("g" -> d._1, "d" -> d._2)
        for {
          b <- TemplateEngine(headerParser, scope)
        } yield createRequest(d._2, f, b.stringOption.getOrElse(""))
      })
    }
  }

  private def createRequest(body: DataSet, verb: => HttpEntityEnclosingRequestBase, uri: String): HttpEntityEnclosingRequestBase =
    createRequest(body match { case DataString(_,s) => Some(s)
    case _ => Some(Data2Json.toJsonString(body))}, verb, uri)

  private def createRequest(body: Option[String], verb: => HttpEntityEnclosingRequestBase, uri: String): HttpEntityEnclosingRequestBase = {

    val request = verb
    request.setURI(URI.create(uri))
    request.setHeader(HttpHeaders.AUTHORIZATION, authHeader)

    if(body.isDefined) {
      val input: StringEntity = new StringEntity(body.get)
      input.setContentType(DataSourceREST.CONTENT_TYPE)
      request.setEntity(input)
    }

    request
  }

  def split(dataSet: DataSet): Iterator[(DataSet,DataSet)] = {
    if(createForEach.isDefined)
      dataSet.elems.flatMap(d => d.find(createForEach.get).map((d,_))) //TODO: can remove elems part later when datasets have names
    else
      dataSet.elems.map(d => (d,d)) //TODO: dito above
  }

  @throws(classOf[Exception])
  def write(dataSet: DataSet): Unit = {
    create(dataSet)
  }

  @throws(classOf[Exception])
  def read(queryParser: QueryParser): DataSet = ???

  override def getDataSet: DataSet = {
    return dataSet
  }

  override def setDataSet(set: DataSet) {
    dataSet = set
  }

  @throws(classOf[Exception])
  def getLastLoggedDataSet: DataSet = ???

  @throws(classOf[Exception])
  def LogNextDataSet(set: DataSet) = ???

  def clazz: Class[_] = classOf[DataSourceREST]
}