package com.lucidworks.zeppelin.solr

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.solr.client.solrj.impl.CloudSolrClient
import org.apache.solr.cloud.MiniSolrCloudCluster
import org.eclipse.jetty.servlet.ServletHolder
import org.junit.Assert._
import org.restlet.ext.servlet.ServerServlet
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SolrCloudTestBuilder extends BeforeAndAfterAll {
  this: Suite =>

  @transient var cluster: MiniSolrCloudCluster = _
  @transient var cloudClient: CloudSolrClient = _
  var zkHost: String = _
  var baseUrl : String = _
  var testWorkingDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val solrXml = new File("src/test/resources/solr.xml")
    val solrXmlContents: String = TestSolrCloudClusterSupport.readSolrXml(solrXml)

    val targetDir = new File("target")
    if (!targetDir.isDirectory)
      fail("Project 'target' directory not found at :" + targetDir.getAbsolutePath)

    testWorkingDir = new File(targetDir, "scala-solrcloud-" + System.currentTimeMillis)
    if (!testWorkingDir.isDirectory)
      testWorkingDir.mkdirs

    // need the schema stuff
    val extraServlets: java.util.SortedMap[ServletHolder, String] = new java.util.TreeMap[ServletHolder, String]()

    val solrSchemaRestApi: ServletHolder = new ServletHolder("SolrSchemaRestApi", classOf[ServerServlet])
    solrSchemaRestApi.setInitParameter("org.restlet.application", "org.apache.solr.rest.SolrSchemaRestApi")
    extraServlets.put(solrSchemaRestApi, "/schema/*")

    cluster = new MiniSolrCloudCluster(1, null /* hostContext */ ,
      testWorkingDir.toPath, solrXmlContents, extraServlets, null /* extra filters */)
    cloudClient = cluster.getSolrClient
    cloudClient.connect()
    //val runner = cluster.
    //var url : URL = runner.getBaseUrl()
    baseUrl = "http://localhost:"+cluster.getJettySolrRunner(0).getLocalPort+"/solr"

    assertTrue(!cloudClient.getZkStateReader.getClusterState.getLiveNodes.isEmpty)
    zkHost = cluster.getZkServer.getZkAddress
  }

  override def afterAll(): Unit = {
    cloudClient.close()
    cluster.shutdown()

    if (testWorkingDir != null && testWorkingDir.isDirectory) {
      FileUtils.deleteDirectory(testWorkingDir)
    }

    super.afterAll()
  }

}

// General builder to be used by all the tests that need Solr
trait TestSuiteBuilder extends ZeppelinSolrFunSuite with SolrCloudTestBuilder {}

trait CollectionSuiteBuilder extends TestSuiteBuilder {
  val collections = Array("col1", "col2")
  val emptyCollection = "emptyCol"

  override def beforeAll(): Unit = {
    super.beforeAll()
    collections.foreach(f => SolrCloudUtil.buildCollection(zkHost, f, 20, 1, cloudClient))
    SolrCloudUtil.buildCollection(zkHost, emptyCollection, 0, 1, cloudClient)
  }

  override def afterAll(): Unit = {
    SolrCloudUtil.deleteCollection(emptyCollection, cluster)
    collections.foreach(f => SolrCloudUtil.deleteCollection(f, cluster))
    super.afterAll()
  }

}