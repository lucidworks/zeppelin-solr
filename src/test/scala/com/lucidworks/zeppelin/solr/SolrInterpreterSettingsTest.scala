package com.lucidworks.zeppelin.solr

import java.util.Properties

import org.apache.solr.common.SolrException
import org.apache.zeppelin.interpreter.InterpreterResult


class SolrInterpreterSettingsTest extends TestSuiteBuilder {

  test("Test interpreter settings") {
    val properties = new Properties()
    properties.put(SolrInterpreter.ZK_HOST, zkHost)
    val solrInterpreter = new SolrInterpreter(properties)
    solrInterpreter.open()
    assert(solrInterpreter.getCloudClient != null)
  }

  test("Test invalid interpreter settings") {
    val properties = new Properties()
    properties.put(SolrInterpreter.ZK_HOST, "localhost12:12121")
    val solrInterpreter = new SolrInterpreter(properties)

    val caughtException = intercept[Throwable] { solrInterpreter.open() }
    assert(caughtException.getCause.isInstanceOf[SolrException])
  }

  test("Unknown command") {
    val properties = new Properties()
    properties.put(SolrInterpreter.ZK_HOST, zkHost)
    val solrInterpreter = new SolrInterpreter(properties)
    solrInterpreter.open()

    val result = solrInterpreter.interpret("index asdjkasd", null)
    assert(result.code().eq(InterpreterResult.Code.INCOMPLETE))
  }

  test("Invalid use command") {
    val properties = new Properties()
    properties.put(SolrInterpreter.ZK_HOST, zkHost)
    val solrInterpreter = new SolrInterpreter(properties)
    solrInterpreter.open()

    val result = intercept[IllegalArgumentException](solrInterpreter.interpret("use abc", null))
  }

}
