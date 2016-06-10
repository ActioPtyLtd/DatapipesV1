package com.actio

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
import org.apache.http.client.methods.{HttpGet, HttpUriRequest, HttpPut}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.auth.BasicScheme
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils
import scala.collection.Iterator
import scala.util.Try

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

  def send(request: HttpUriRequest): Try[(HttpResponse,String)] = {
    val httpClient = HttpClientBuilder.create().build()

    logger.info(s"Querying URI: ${request.getURI}...")

    val response = Try({val re = httpClient.execute(request)
      (re,EntityUtils.toString(re.getEntity,"UTF-8"))})

    if(response.isSuccess)
      logger.info(s"Server status code: ${response.get._1.getStatusLine.getStatusCode}")

    httpClient.close()

    response
  }

  @throws(classOf[Exception])
  def executeQuery(ds: DataSet, query: String) = {
    val httpGet = new HttpGet(query)
    val auth = user + ":" + password
    val encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("ISO-8859-1")))
    val authHeader = "Basic " + new String(encodedAuth)
    httpGet.setHeader(HttpHeaders.AUTHORIZATION, authHeader)
    httpGet.setHeader(HttpHeaders.CONTENT_TYPE, DataSourceREST.CONTENT_TYPE)

    val response = send(httpGet).get // ideally return try instead

    new DataSetFixedData(SchemaUnknown, Data2Json.fromJson2Data(response._2))
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

  @throws(classOf[Exception])
  def write(data: DataSet) {
    while (data.hasNext) {
      val i: Iterator[Data] = data.next.elems.toIterator
      while (i.hasNext) {
        write(Data2Json.toJsonString(i.next))
      }
    }
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

  private def write(data: String) {
    var httpClient: CloseableHttpClient = null
    try {
      val credentialsProvider: CredentialsProvider = new BasicCredentialsProvider
      credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password))
      httpClient = HttpClientBuilder.create.setDefaultCredentialsProvider(credentialsProvider).build
      val postRequest: HttpPut = new HttpPut(route)
      var input: StringEntity = null
      input = new StringEntity(data)
      input.setContentType(DataSourceREST.CONTENT_TYPE)
      postRequest.setEntity(input)
      val response: HttpResponse = httpClient.execute(postRequest)
      logger.info("REST Service Returned:=" + response.getStatusLine.getStatusCode)
    }
    catch {
      case e: UnsupportedEncodingException => {
        logger.info("Unable to send data to xMatters" + e)
      }
      case e: ClientProtocolException => {
        logger.info("Unable to send data to xMatters" + e)
      }
      case e: IOException => {
        logger.info("Unable to send data to xMatters" + e)
      }
    } finally {
      if (httpClient != null) {
        try {
          httpClient.close
        }
        catch {
          case e: IOException => {
            logger.info("Unable close the Connection" + e)
          }
        }
      }
    }
  }

  def clazz: Class[_] = classOf[DataSourceREST]
}