package com.lucidworks.zeppelin.solr

import java.net.{ConnectException, SocketException}

import com.google.common.cache._
import org.apache.commons.httpclient.NoHttpResponseException
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.impl._
import org.apache.solr.client.solrj.request.UpdateRequest
import org.apache.solr.common.{SolrException, SolrInputDocument}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

object CacheSolrClient {
  private val loader = new CacheLoader[String, CloudSolrClient]() {
    def load(zkHost: String): CloudSolrClient = {
      SolrSupport.getNewSolrCloudClient(zkHost)
    }
  }

  private val listener = new RemovalListener[String, CloudSolrClient]() {
    def onRemoval(rn: RemovalNotification[String, CloudSolrClient]): Unit = {
      if (rn != null && rn.getValue != null) {
        rn.getValue.close()
      }
    }
  }

  val cache: LoadingCache[String, CloudSolrClient] = CacheBuilder
    .newBuilder()
    .removalListener(listener)
    .build(loader)
}

/**
 * TODO: Use Solr schema API to index field names
 */
object SolrSupport {

  val logger = LoggerFactory.getLogger(SolrSupport.getClass)

  def setupKerberosIfNeeded(): Unit = synchronized {
    val loginProp = System.getProperty(Krb5HttpClientConfigurer.LOGIN_CONFIG_PROP)
    if (loginProp != null && !loginProp.isEmpty) {
      HttpClientUtil.addConfigurer(new Krb5HttpClientConfigurer)
      logger.debug(s"Installed the Krb5HttpClientConfigurer for Solr security using config: $loginProp")
    }
  }

  def getHttpSolrClient(shardUrl: String): HttpSolrClient = {
    setupKerberosIfNeeded()
    new HttpSolrClient(shardUrl)
  }

  // This method should not be used directly. The method [[SolrSupport.getCachedCloudClient]] should be used instead
  private def getSolrCloudClient(zkHost: String): CloudSolrClient =  {
    setupKerberosIfNeeded()
    val solrClient = new CloudSolrClient(zkHost)
    solrClient.connect()
    solrClient
  }

  // Use this only if you want a new SolrCloudClient instance. This new instance should be closed by the methods downstream
  def getNewSolrCloudClient(zkHost: String): CloudSolrClient = {
    getSolrCloudClient(zkHost)
  }

  def getCachedCloudClient(zkHost: String): CloudSolrClient = {
    CacheSolrClient.cache.get(zkHost)
  }

  def getSolrBaseUrl(zkHost: String) = {
    val solrClient = getCachedCloudClient(zkHost)
    val liveNodes = solrClient.getZkStateReader.getClusterState.getLiveNodes
    if (liveNodes.isEmpty) {
      throw new RuntimeException("No live nodes found for cluster: " + zkHost)
    }
    var solrBaseUrl = solrClient.getZkStateReader.getBaseUrlForNodeName(liveNodes.iterator().next())
    if (!solrBaseUrl.endsWith("?")) solrBaseUrl += "/"
    solrBaseUrl
  }

  def sendBatchToSolr(solrClient: SolrClient, collection: String, batch: Iterable[SolrInputDocument]): Unit =
    sendBatchToSolr(solrClient, collection, batch, None)

  def sendBatchToSolr(
    solrClient: SolrClient,
    collection: String,
    batch: Iterable[SolrInputDocument],
    commitWithin: Option[Int]): Unit = {
    val req = new UpdateRequest()
    req.setParam("collection", collection)

    val initialTime = System.currentTimeMillis()

    if (commitWithin.isDefined)
      req.setCommitWithin(commitWithin.get)

    logger.info("Sending batch of " + batch.size + " to collection " + collection)

    req.add(asJavaCollection(batch))

    try {
      solrClient.request(req)
      val timeTaken = (System.currentTimeMillis() - initialTime)/1000.0
      logger.info("Took '" + timeTaken + "' secs to index '" + batch.size + "' documents")
    } catch {
      case e: Exception =>
        if (shouldRetry(e)) {
          logger.error("Send batch to collection " + collection + " failed due to " + e + " ; will retry ...")
          try {
            Thread.sleep(2000)
          } catch {
            case ie: InterruptedException => Thread.interrupted()
          }

          try {
            solrClient.request(req)
          } catch {
            case ex: Exception =>
              logger.error("Send batch to collection " + collection + " failed due to: " + e, e)
              ex match {
                case re: RuntimeException => throw re
                case e: Exception => throw new RuntimeException(e)
              }
          }
        } else {
          logger.error("Send batch to collection " + collection + " failed due to: " + e, e)
          e match {
            case re: RuntimeException => throw re
            case ex: Exception => throw new RuntimeException(ex)
          }
        }

    }

  }

  def shouldRetry(exc: Exception): Boolean = {
    val rootCause = SolrException.getRootCause(exc)
    rootCause match {
      case e: ConnectException => true
      case e: NoHttpResponseException => true
      case e: SocketException => true
      case _ => false
    }
  }

}
