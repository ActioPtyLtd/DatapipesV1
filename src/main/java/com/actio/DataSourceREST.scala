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
    if (label == "create") {
      new HttpPost()
    } else if (label == "update") {
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

    if (config.hasPath("headers")) {
      import scala.collection.JavaConversions._
      var headmap = config.getObject("headers").entrySet()

      headmap.foreach(n => {
        headers = headers ::: List((n.getKey, n.getValue.unwrapped().toString))
      })

    }
  }

  @throws(classOf[Exception])
  def execute(ds: DataSet, query: String) {
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

  import scala.collection.JavaConverters._

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
    create(dataSet)
  }

  override def create(ds: DataSet): Unit = {
    executeQueryLabel(ds.elems.toList.head, "create")
  }

  override def delete(ds: DataSet): Unit = {
    executeQueryLabel(ds.elems.toList.head, "delete")
  }

  @throws(classOf[Exception])
  override def executeQueryLabel(ds: DataSet, label: String): DataSet = {
    val requestQuery =
      getRequest(ds,
        DataSourceREST.createHttpRequest(label),
        config.getString(s"query.$label.uri"),
        configOption(config, s"query.$label.body"),
        if(config.hasPath(s"query.$label.templates")) {
          config.getObject(s"query.$label.templates").asScala.map(kv => (kv._1, kv._2.unwrapped().toString)).toList
        }
        else {
          Nil
        }
      )

    logger.info(s"Calling ${requestQuery.getMethod} ${requestQuery.getURI}")

    val element = getResponseDataSet(requestQuery)(sendRequest)

    logger.info(s"Status code ${element.statusCode} returned.")

    new DataSetFixedData(element.schema, element)
  }

  def sendRequest(request: HttpUriRequest): (StatusLine, Array[Header], String) = {

    headers.foreach(t => request.setHeader(new BasicHeader(t._1, t._2.replace("\"", ""))))

    //request.setHeader(new BasicHeader("sfapikey","6ee2ac1c-6045-496f-892e-a0ad3dd5976c"))

    logger.info(">>>>>>>>" + request.toString + "<<<<<<<" + request.getRequestLine)

    import scala.collection.JavaConversions._
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

    this.logger.info(response._3)

    val dsBody = Try(Data2Json.fromJson2Data(response._3)).toOption.getOrElse(DataString(Option(response._3).getOrElse("")))

    DataSetHttpResponse("response",
      request.getURI.toString,
      response._1.getStatusCode,
      response._2.map(h => h.getName -> h.getValue).toMap,
      DataRecord("root", dsBody.elems.toList))
  }

  private def configOption(config: Config, path: String) = if (config.hasPath(path)) Some(config.getString(path)) else None

  private def getRequest[T <: HttpRequestBase](ds: DataSet, verb: => HttpRequestBase, templateHeader: String, templateBody: Option[String], templates: List[(String, String)]) = {
    val headerParser = TemplateParser(templateHeader)
    val scope = (("d" -> (() => ExprDataSet(ds))) :: templates.map(t => (t._1, () => TemplateParser(t._2))).toList).toMap
    val headerExpression = TemplateEngine.eval(headerParser, scope)

    if (templateBody.isDefined) {
      val bodyParser = TemplateParser(templateBody.get)
      val bodyExpression = TemplateEngine.eval(bodyParser, scope)

      createRequest(bodyExpression, verb, headerExpression.stringOption.getOrElse(""))
    } else {
      createRequest(ds, verb, headerExpression.stringOption.getOrElse(""))
    }
  }

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

      val input: StringEntity = new StringEntity(body.get)
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

  override def update(ds: DataSet): Unit = {}

  @throws(classOf[Exception])
  def read(queryParser: QueryParser): DataSet = ???

  @throws(classOf[Exception])
  def getLastLoggedDataSet: DataSet = ???

  @throws(classOf[Exception])
  def LogNextDataSet(set: DataSet) = ???

  def clazz: Class[_] = classOf[DataSourceREST]
}