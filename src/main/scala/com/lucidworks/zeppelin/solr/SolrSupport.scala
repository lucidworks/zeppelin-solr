package com.lucidworks.zeppelin.solr

import java.net.{ConnectException, InetAddress, SocketException, URL}

import com.google.common.cache._
import org.apache.commons.httpclient.NoHttpResponseException
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.impl._
import org.apache.solr.client.solrj.request.UpdateRequest
import org.apache.solr.common.cloud._
import org.apache.solr.common.params.ModifiableSolrParams
import org.apache.solr.common.{SolrException, SolrInputDocument}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Random

case class SolrShard(shardName: String, replicas: List[SolrReplica])
case class ShardInfo(shardUrl: String, zkHost: String)

case class SolrReplica(
    replicaNumber: Int,
    replicaName: String,
    replicaUrl: String,
    replicaHostName: String,
    locations: Array[InetAddress]) {
  def getHostAndPort(): String = {
    replicaHostName.substring(0, replicaHostName.indexOf('_'))
  }

  override def toString(): String = {
    return s"SolrReplica(${replicaNumber}) ${replicaName}: url=${replicaUrl}, hostName=${replicaHostName}, locations=" + locations.mkString(",")
  }
}

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

object CacheHttpSolrClient {
  private val loader = new CacheLoader[ShardInfo, HttpSolrClient]() {
    def load(shardUrl: ShardInfo): HttpSolrClient = {
      SolrSupport.getNewHttpSolrClient(shardUrl.shardUrl, shardUrl.zkHost)
    }
  }

  private val listener = new RemovalListener[ShardInfo, HttpSolrClient]() {
    def onRemoval(rn: RemovalNotification[ShardInfo, HttpSolrClient]): Unit = {
      if (rn != null && rn.getValue != null) {
        rn.getValue.close()
      }
    }
  }

  val cache: LoadingCache[ShardInfo, HttpSolrClient] = CacheBuilder
    .newBuilder()
    .removalListener(listener)
    .build(loader)
}

/**
 * TODO: Use Solr schema API to index field names
 */
object SolrSupport {

  val logger = LoggerFactory.getLogger(SolrSupport.getClass)

  def isKerberosNeeded(zkHost: String): Boolean = synchronized {
    val loginProp = System.getProperty(Krb5HttpClientBuilder.LOGIN_CONFIG_PROP)
    if (loginProp != null && loginProp.nonEmpty) {
      return true
    }
    return false
  }

  def isBasicAuthNeeded(zkHost: String): Boolean = synchronized {
    val credentials = System.getProperty(PreemptiveBasicAuthClientBuilderFactory.SYS_PROP_BASIC_AUTH_CREDENTIALS)
    val configFile = System.getProperty(PreemptiveBasicAuthClientBuilderFactory.SYS_PROP_HTTP_CLIENT_CONFIG)
    if (credentials != null || configFile != null) {
      if (configFile != null) {
        logger.debug("Basic auth configured with config file {}", configFile)
      } else {
        logger.debug("Basic auth configured with creds {}", credentials)
      }
      return true
    }
    return false
  }

  private def getHttpSolrClient(shardUrl: String, zkHost: String): HttpSolrClient = {
    new HttpSolrClient.Builder()
      .withBaseSolrUrl(shardUrl)
      .withHttpClient(getCachedCloudClient(zkHost).getHttpClient)
      .build()
  }

  def getNewHttpSolrClient(shardUrl: String, zkHost: String): HttpSolrClient = {
    getHttpSolrClient(shardUrl, zkHost)
  }

  def getCachedHttpSolrClient(shardUrl: String, zkHost: String): HttpSolrClient = {
    CacheHttpSolrClient.cache.get(ShardInfo(shardUrl, zkHost))
  }

  // This method should not be used directly. The method [[SolrSupport.getCachedCloudClient]] should be used instead
  private def getSolrCloudClient(zkHost: String): CloudSolrClient =  {
    logger.debug(s"Creating a new SolrCloudClient for zkhost $zkHost")
    val solrClientBuilder = new CloudSolrClient.Builder().withZkHost(zkHost)
    val params = new ModifiableSolrParams()
    params.set(HttpClientUtil.PROP_FOLLOW_REDIRECTS, false)
    if (isKerberosNeeded(zkHost)) {
      val krb5HttpClientBuilder = new Krb5HttpClientBuilder().getHttpClientBuilder(java.util.Optional.empty())
      HttpClientUtil.setHttpClientBuilder(krb5HttpClientBuilder)
    }
    if (isBasicAuthNeeded(zkHost)) {
      val basicAuthBuilder = new PreemptiveBasicAuthClientBuilderFactory().getHttpClientBuilder(java.util.Optional.empty())
      HttpClientUtil.setHttpClientBuilder(basicAuthBuilder)
    }
    val httpClient = HttpClientUtil.createClient(params)
    val solrClient = solrClientBuilder.withHttpClient(httpClient).build()
    solrClient.setZkClientTimeout(30000)
    solrClient.setZkConnectTimeout(60000)
    solrClient.connect()
    logger.debug(s"Created new SolrCloudClient for zkhost $zkHost")
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

  def randomReplica(solrShard: SolrShard): SolrReplica = {
    solrShard.replicas(Random.nextInt(solrShard.replicas.size))
  }

  def randomReplica(solrShard: SolrShard, replicaToExclude: SolrReplica): SolrReplica = {
    val filteredReplicas = solrShard.replicas.filter(p => p.equals(replicaToExclude))
    solrShard.replicas(Random.nextInt(filteredReplicas.size))
  }

  def buildShardList(zkHost: String, collection: String, shardsTolerant: Boolean): List[SolrShard] = {
    val solrClient = getCachedCloudClient(zkHost)
    val zkStateReader: ZkStateReader = solrClient.getZkStateReader
    val clusterState: ClusterState = zkStateReader.getClusterState
    var collections = Array.empty[String]
    for(col <- collection.split(",")) {
      if (clusterState.hasCollection(col)) {
        collections = collections :+ col
      }
      else {
        val aliases: Aliases = zkStateReader.getAliases
        val aliasedCollections: String = aliases.getCollectionAliasMap.get(col)
        if (aliasedCollections == null) {
          throw new IllegalArgumentException("Collection " + col + " not found!")
        }
        collections = aliasedCollections.split(",")
      }
    }
    val liveNodes  = clusterState.getLiveNodes

    val shards = new ListBuffer[SolrShard]()
    for (coll <- collections) {
      for (slice: Slice <- clusterState.getCollection(coll).getSlices()) {
        var replicas  =  new ListBuffer[SolrReplica]()
        for (r: Replica <- slice.getReplicas) {
          if (r.getState == Replica.State.ACTIVE) {
            val replicaCoreProps: ZkCoreNodeProps = new ZkCoreNodeProps(r)
            if (liveNodes.contains(replicaCoreProps.getNodeName)) {
              try {
                val addresses = InetAddress.getAllByName(new URL(replicaCoreProps.getBaseUrl).getHost)
                replicas += SolrReplica(0, replicaCoreProps.getCoreName, replicaCoreProps.getCoreUrl, replicaCoreProps.getNodeName, addresses)
              } catch {
                case e : Exception => logger.warn("Error resolving ip address " + replicaCoreProps.getNodeName + " . Exception " + e)
                  replicas += SolrReplica(0, replicaCoreProps.getCoreName, replicaCoreProps.getCoreUrl, replicaCoreProps.getNodeName, Array.empty[InetAddress])
              }
            }

          }
        }
        val numReplicas: Int = replicas.size
        if (!shardsTolerant && numReplicas == 0) {
          throw new IllegalStateException("Shard " + slice.getName + " in collection " + coll + " does not have any active replicas!")
        }
        shards += SolrShard(slice.getName, replicas.toList)
      }
    }
    if (shards.isEmpty) {
      throw new IllegalStateException(s"No active shards in collections: ${collections}")
    }
    shards.toList
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
