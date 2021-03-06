package com.actio

import java.net.URI
import java.nio.charset.Charset

import com.actio.dpsystem.Logging
import com.typesafe.config.Config
import java.io.IOException
import java.io.UnsupportedEncodingException

import org.apache.commons.codec.binary.Base64
import org.apache.http._
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
import org.apache.http.message.BasicHeader
import org.apache.http.util.EntityUtils

import scala.collection.Iterator
import scala.util.{Failure, Success, Try}

/**
 * Created by jim on 7/8/2015.
 */
object DataSourceREST {
  private val CONTENT_TYPE: String = "application/json"

  def createHttpRequest(label: String): HttpRequestBase = {
    if (label == "create" || label == "post") {
      new HttpPost()
    } else if (label == "update" || label == "put") {
      new HttpPut()
    } else if (label == "patch") {
      new HttpPatch()
    } else if (label == "delete") {
      new HttpDelete()
    } else {
      new HttpGet()
    }
  }
}

class DataSourceREST extends DataSource with Logging {
  type HttpClient = HttpUriRequest => (StatusLine, Array[Header], String)
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
  var headers: List[(String, String)] = List()
  var onResponse: String = null
  var onEmpty: String = null
  var onError : String = null
  var outputBufferLogSize : Integer = 500

  @throws(classOf[Exception])
  override def setConfig(_conf: Config, _master: Config) {
    super.setConfig(_conf, _master)
    if (config.hasPath("url")) {
      route = config.getString("url")
    }
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
    if (config.hasPath("onResponse")) {
      onResponse = config.getString("onRsponse")
    }
    if (config.hasPath("onEmpty")) {
      onEmpty = config.getString("onEmpty")
    }
    if (config.hasPath("onError")) {
      onError = config.getString("onError")
    }
    if (config.hasPath("outputBufferLogSize"))
      {
        outputBufferLogSize = config.getInt("outputBufferLogSize")
      }

    if (config.hasPath("headers")) {
      import scala.collection.JavaConversions._
      var headmap = config.getObject("headers").entrySet()

      headmap.foreach(n => {
        headers = headers ::: List((n.getKey, n.getValue.unwrapped().toString))
      })

    }
  }

  @throws(classOf[Exception])
  def execute(ds: DataSet, query: String): Unit = {
    ds.elems.foreach(e => executeQueryLabel(e,query))
  }

  def credentialsProvider: CredentialsProvider = {
    val cre = new BasicCredentialsProvider
    cre.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password))
    cre
  }

  @throws(classOf[Exception])
  def executeQuery(ds: DataSet, query: String): DataSet = {
    Nothin()
  }

  @throws(classOf[Exception])
  def extract(): Unit = {
    dataSet = read(Nothin()) // doesn't really need a dataset
  }

  @throws(classOf[Exception])
  def load() {
    if (trans != null) {
      trans.setDataSet(getDataSet)
      trans.execute
      setDataSet(trans.getDataSet)
    }
    write(getDataSet)
  }

  @throws(classOf[Exception])
  def execute() {
    if (trans != null) {
      trans.setDataSet(getDataSet)
      trans.execute
      setDataSet(trans.getDataSet)
    }
    write(dataSet)
  }

  @throws(classOf[Exception])
  def write(dataSet: DataSet): Unit = {
    if (config.hasPath("iterate")) {
      dataSet.elems.foreach(d => create(DataArray(List(d))))
    }
    else {
      create(dataSet)
    }
  }

  override def read(ds: DataSet): DataSet = {
    if(config.hasPath("iterate")) {
      val condStr = config.getString("iterate.until")
      new DataSetIterator(new DataSourcePaging(this, x => MetaTerm.eval(x, condStr) == DataBoolean(true) ))
    }
    else {
      super.read(ds)
    }
  }

  override def create(ds: DataSet): Unit = {
    ds.elems.foreach(e => executeQueryLabel(e, "create"))
  }

  override def delete(ds: DataSet): Unit = {
    ds.elems.foreach(e => executeQueryLabel(e, "delete"))
  }

  @throws(classOf[Exception])
  override def executeQueryLabel(ds: DataSet, label: String): DataSet = {
    val requestQuery =
      createRequest(configOption(config,s"query.$label.body").map(d => ds(d)).getOrElse(ds),
        DataSourceREST.createHttpRequest(configOption(config,s"query.$label.verb").getOrElse(label)),
        ds(config.getString(s"query.$label.uri")).stringOption.getOrElse(config.getString(s"query.$label.uri"))
      )

    logger.info(s"Calling ${requestQuery.getMethod} ${requestQuery.getURI}")

    val element = getResponseDataSet(requestQuery)(sendRequest)

    if(element.statusCode >= 400 && element.statusCode < 600) {

      logger.error(s"Status code ${element.statusCode} returned.")
      logger.error(s"Body "+element.body.toString)
      logger.error("{" + Data2Json.toJsonString(element.body) + "}")
      if (onError != null)
        if (onError.toLowerCase  == "exit"){
          // exiting pipeline

          logger.error(s"**** Exiting OnError ${element.statusCode} returned.")

          System.exit(-1)
        }   else   if (onError.toLowerCase == "exception"){
          // exiting pipeline

          logger.error(s"**** On Error Exception: DataSourceREST ${element.statusCode} returned.")

          throw new Exception("On Error Exception: DataSourceREST ")
        }
    } else {
      logger.info(s"Status code ${element.statusCode} returned.")
    }

    element
  }

  def sendRequest(request: HttpUriRequest): (StatusLine, Array[Header], String) = {

    headers.foreach(t => request.setHeader(new BasicHeader(t._1, t._2.replace("\"", ""))))

    logger.info(">>>>>>>>" + request.toString + "<<<<<<<" + request.getRequestLine)

    request.getAllHeaders.foreach(f => logger.info(">>>" + f.getName + ">>" + f.getValue))

    val httpreq = HttpClientBuilder.create()

    val builthttp = httpreq.build()

    val response = builthttp.execute(request)
    val respEntity = response.getEntity

    val ret = (response.getStatusLine,
      response.getAllHeaders,
      if (Option(respEntity).isDefined) EntityUtils.toString(response.getEntity, "UTF-8") else "")

    response.close()
    ret
  }

  def getResponseDataSet(request: HttpUriRequest)(implicit httpClient: HttpClient): DataSetHttpResponse = {
    val response = httpClient(request)

    val displayString:String = Option(response._3).getOrElse("")
    if (displayString.length > 0) {
      logger.info(s"Body: '" + displayString.substring(0, Math.min(displayString.length, outputBufferLogSize)) + "'")
    }

    val dsBody = Try(Data2Json.fromJson2Data(response._3)).toOption.getOrElse(DataRecord({DataString("nonjsoncompliantmsg",Option(response._3).getOrElse(""))}))

    DataSetHttpResponse("response",
      request.getURI.toString,
      response._1.getStatusCode,
      response._2.map(h => h.getName -> h.getValue).toMap,
      DataRecord("root", dsBody.elems.toList))

  }

  private def configOption(config: Config, path: String) = if (config.hasPath(path)) Some(config.getString(path)) else None

  private def createRequest(body: DataSet, verb: => HttpRequestBase, uri: String): HttpRequestBase =
    verb match {
      case postput: HttpEntityEnclosingRequestBase => createRequest(body match {
        case DataString(_, s) => Some(s)
        case _ => Some(Data2Json.toJsonString(body))
      }, verb, uri)
      case _ => createRequest(None, verb, uri)
    }

  private def createRequest(body: Option[String], verb: => HttpRequestBase, uri: String): HttpRequestBase = {
    val request = verb
    request.setURI(URI.create(uri))
    request.setHeader(HttpHeaders.AUTHORIZATION, authHeader)

    if (body.isDefined) {

      logger.info(body.get)

      val input: StringEntity = new StringEntity(body.get,"UTF-8")
      input.setContentType(DataSourceREST.CONTENT_TYPE)
      request.asInstanceOf[HttpEntityEnclosingRequestBase].setEntity(input)
    }

    request
  }

  def authHeader: String = "Basic " + new String(Base64.encodeBase64((user + ":" + password).getBytes(Charset.forName("ISO-8859-1"))))

  override def getDataSet: DataSet = {
    dataSet
  }

  override def setDataSet(set: DataSet) {
    dataSet = set
  }

  @throws(classOf[Exception])
  def write(data: DataSet, suffix: String) {
    if (suffix.contentEquals("_all")) write(data)
  }

  override def update(ds: DataSet): Unit = {
    ds.elems.foreach(e => executeQueryLabel(e, "update"))
  }

  @throws(classOf[Exception])
  def read(queryParser: QueryParser): DataSet = ???

  @throws(classOf[Exception])
  def getLastLoggedDataSet: DataSet = ???

  @throws(classOf[Exception])
  def LogNextDataSet(set: DataSet) = ???

  def clazz: Class[_] = classOf[DataSourceREST]
}
